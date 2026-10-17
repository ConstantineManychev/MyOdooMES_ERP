import json
import logging
import math
from bisect import bisect_right
from datetime import datetime, timedelta

import pytz

from odoo import models, fields, api, _

_logger = logging.getLogger(__name__)

BACKFILL_DAYS = 30
RESCAN_MARGIN = timedelta(minutes=15)
MAX_UNMAPPED_PER_ROLE = 50
FAR_FUTURE = datetime(9999, 1, 1)
RECENT_RESTAMP = timedelta(days=2)
LOCK_TIMEOUT = '3s'
OEE_BATCH = 20
OEE_SETTLE = timedelta(minutes=30)
INTERVALS = ('mes.performance.alarm', 'mes.performance.running', 'mes.performance.slowing')
FACTS = (
    ('mes.performance.alarm', 'start_time', 'performance_id.machine_id'),
    ('mes.performance.running', 'start_time', 'performance_id.machine_id'),
    ('mes.performance.slowing', 'start_time', 'performance_id.machine_id'),
    ('mes.task', 'create_date', 'machine_id'),
    ('mes.pending.comment', 'event_time', 'machine_id'),
)


def match_key(text):
    return ' '.join((text or '').split()).lower()


def signature_text(text, number):
    if text is not None:
        return text.strip()
    if number is None:
        return ''
    number = float(number)
    if not math.isfinite(number):
        return ''
    if number.is_integer():
        return str(int(number))
    return f"{number:.6f}".rstrip('0').rstrip('.')


class MesProductEngine(models.AbstractModel):
    _name = 'mes.product.engine'
    _description = 'Product History Engine'

    @api.model
    def _cron_process(self):
        Workcenter = self.env['mrp.workcenter'].sudo()
        for workcenter in Workcenter.search([('signature_line_ids', '!=', False)]):
            self._run_unit("Product signature collection failed for %s", workcenter.name,
                           self._collect_unit, workcenter)
        for workcenter in Workcenter.search([('product_dirty_from', '!=', False)]):
            self._run_unit("Product run rebuild failed for %s", workcenter.name,
                           self._rebuild_dirty, workcenter)
        recent = fields.Datetime.now() - RECENT_RESTAMP
        self._run_unit("Interval machine and shift stamping failed%s", '', self._stamp_structure, recent)
        groups = self.env['mes.production.run'].sudo().read_group([], ['workcenter_id'], ['workcenter_id'])
        for workcenter in Workcenter.browse([group['workcenter_id'][0] for group in groups]):
            self._run_unit("Product propagation failed for %s", workcenter.name,
                           self._propagate_unit, workcenter, recent)
        for report in self._oee_pending_reports():
            error = self._run_unit("Product OEE failed for %s", report.display_name,
                                   self._compute_report_oee, report)
            if error:
                self._run_unit("Product OEE failure could not be recorded for %s", report.display_name,
                               self._mark_oee_failed, report, error)

    def _run_unit(self, message, subject, function, *args):
        cr = self.env.cr
        try:
            cr.execute(f"SET LOCAL lock_timeout = '{LOCK_TIMEOUT}'")
            function(*args)
            cr.commit()
            return None
        except Exception as error:
            cr.rollback()
            self.env.invalidate_all()
            _logger.exception(message, subject)
            return error

    def _collect_unit(self, workcenter):
        full = workcenter.signature_full_rescan
        self._collect(workcenter, full=full)
        if full:
            workcenter.signature_full_rescan = False

    def _propagate_unit(self, workcenter, recent):
        pending = workcenter.product_propagate_from
        self._propagate(workcenter, min(recent, pending) if pending else recent)
        if pending:
            self._invalidate_oee(workcenter, pending)
            workcenter.product_propagate_from = False

    def _report_window(self, report):
        window_start, window_end = report._performance_window()
        return (self._from_wall(report.machine_id, window_start),
                self._from_wall(report.machine_id, window_end))

    def _oee_pending_reports(self):
        Report = self.env['mes.machine.performance'].sudo()
        first = self.env['mes.production.run'].sudo().search([], order='date_start', limit=1)
        if not first:
            return Report
        cutoff = fields.Datetime.now() - OEE_SETTLE
        candidates = Report.search([
            ('state', '=', 'done'),
            ('product_oee_computed_at', '=', False),
            ('product_oee_error', '=', False),
            ('date', '>=', (first.date_start - timedelta(days=1)).date()),
        ], order='date desc, id desc', limit=OEE_BATCH * 3)
        return candidates.filtered(lambda report: self._report_window(report)[1] <= cutoff)[:OEE_BATCH]

    def _invalidate_oee(self, workcenter, since):
        reports = self.env['mes.machine.performance'].sudo().search([
            ('machine_id', '=', workcenter.id),
            ('date', '>=', (since - timedelta(days=1)).date()),
            '|', ('product_oee_computed_at', '!=', False), ('product_oee_error', '!=', False),
        ])
        if not reports:
            return
        self.env['mes.product.oee'].sudo().search([('performance_id', 'in', reports.ids)]).unlink()
        reports.write({'product_oee_computed_at': False, 'product_oee_error': False})

    def _mark_oee_failed(self, report, error):
        report.write({'product_oee_error': str(error)[:200] or 'failed'})

    @api.model
    def _oee_windows(self, start, end, runs, entries):
        points = {start, end}
        for run in runs:
            points.update([run.date_start, run.date_end or end])
        for entry in entries:
            points.update([entry.login_at, entry.logout_at or end])
        points = sorted(point for point in points if start <= point <= end)
        windows = []
        for window_start, window_end in zip(points, points[1:]):
            if window_end <= window_start:
                continue
            run = next((item for item in reversed(runs)
                        if item.date_start <= window_start < (item.date_end or FAR_FUTURE)), None)
            present = [item for item in entries
                       if item.login_at <= window_start < (item.logout_at or FAR_FUTURE)]
            windows.append((window_start, window_end,
                            run.product_id if run else None,
                            present[-1].employee_id if present else None))
        return windows

    def _compute_report_oee(self, report):
        workcenter = report.machine_id
        start, end = self._report_window(report)
        runs = self.env['mes.production.run'].sudo().search([
            ('workcenter_id', '=', workcenter.id),
            ('date_start', '<', end),
            '|', ('date_end', '=', False), ('date_end', '>', start),
        ], order='date_start')
        entries = self.env['mes.performance.login.entry'].sudo().search([
            ('workcenter_id', '=', workcenter.id),
            ('role', '=', 'operator'),
            ('login_at', '<', end),
            '|', ('logout_at', '=', False), ('logout_at', '>', start),
        ], order='login_at, id')
        rows = []
        for window_start, window_end, product, operator in self._oee_windows(start, end, list(runs), list(entries)):
            rows += self._window_rows(workcenter, report, window_start, window_end, product, operator)
        Oee = self.env['mes.product.oee'].sudo()
        Oee.search([('performance_id', '=', report.id)]).unlink()
        if rows:
            Oee.create(rows)
        report.write({'product_oee_computed_at': fields.Datetime.now(), 'product_oee_error': False})

    def _window_rows(self, workcenter, report, start, end, product, operator):
        breakdown = self.env['mes.oee.breakdown'].sudo()
        settings = self.env['mes.machine.settings'].sudo()
        active, planned_sec = settings._get_planned_working_intervals(start, end, workcenter)
        if not planned_sec or planned_sec <= 0:
            return []
        run_sec = settings._fetch_interval_stats(active, workcenter.id, mode='runtime') or 0.0
        good_qty = breakdown._fetch_good_quantity(workcenter, start, end) or 0.0
        capacity = workcenter.ideal_capacity_per_min or 0.0
        ideal_qty = run_sec * (capacity / 60.0 if capacity > 0 else 1.0)
        machine_waste = max(0.0, ideal_qty - good_qty)
        counts = breakdown._collect_counts(workcenter, start, end)
        stops = breakdown._collect_stops(workcenter, start, end)
        total_stop = sum(stop['duration'] for stop in stops)
        streams = breakdown._build_streams(workcenter, counts, stops, total_stop, machine_waste,
                                           planned_sec, run_sec / 3600.0)
        common = {
            'performance_id': report.id,
            'workcenter_id': workcenter.id,
            'shift_id': report.shift_id.id or False,
            'shift_date': report.date,
            'product_id': product.id if product else False,
            'operator_id': operator.id if operator else False,
            'date_start': start,
            'date_end': end,
        }
        rows = [dict(common, row_kind='machine', planned_sec=planned_sec, run_sec=run_sec,
                     good_qty=good_qty, ideal_qty=ideal_qty, waste_qty=machine_waste,
                     stop_sec=total_stop, stop_count=len(stops))]
        for stream in streams:
            stream_id = stream.get('id') or False
            rows.append(dict(common, row_kind='stream', stream_id=stream_id,
                             waste_qty=stream['waste']['qty'],
                             stop_sec=stream['downtime']['duration_sec'],
                             stop_count=stream['downtime']['stops']))
            for part in stream.get('parts', []):
                rows.append(dict(common, row_kind='part', stream_id=stream_id,
                                 part_id=part.get('id') or False,
                                 waste_qty=part['waste']['qty'],
                                 stop_sec=part['downtime']['duration_sec'],
                                 stop_count=part['downtime']['stops']))
        return rows

    @api.model
    def _stamp_structure(self, since):
        for model_name in INTERVALS:
            Model = self.env[model_name].sudo()
            Model.flush_model()
            self.env.cr.execute(f"""
                UPDATE "{Model._table}" AS fact
                SET workcenter_id = doc.machine_id,
                    shift_date = doc.date,
                    shift_id = doc.shift_id
                FROM mes_machine_performance AS doc
                WHERE fact.performance_id = doc.id
                  AND fact.start_time >= %s
                  AND fact.end_time IS NOT NULL
                  AND (fact.workcenter_id IS DISTINCT FROM doc.machine_id
                       OR fact.shift_date IS DISTINCT FROM doc.date
                       OR fact.shift_id IS DISTINCT FROM doc.shift_id)
            """, (since,))
            Model.invalidate_model(['workcenter_id', 'shift_date', 'shift_id'])

    def _rebuild_dirty(self, workcenter):
        workcenter = workcenter.sudo()
        moment = workcenter.product_dirty_from
        if not moment:
            return
        workcenter.product_dirty_from = False
        self._rebuild(workcenter, moment)

    def _machine_tz(self, workcenter):
        return pytz.timezone(workcenter.company_id.tz or self.env.company.tz or 'UTC')

    def _to_wall(self, workcenter, moment_utc):
        return pytz.utc.localize(moment_utc).astimezone(self._machine_tz(workcenter)).replace(tzinfo=None)

    def _from_wall(self, workcenter, wall):
        local = self._machine_tz(workcenter).localize(wall, is_dst=False)
        return local.astimezone(pytz.utc).replace(tzinfo=None)

    def _fetch_events(self, workcenter, tag_names, since_utc):
        machine = workcenter.machine_settings_id
        if not machine or not tag_names:
            return []
        since_wall = self._to_wall(workcenter, since_utc)
        with self.env['mes.timescale.base']._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT time, tag_name, value_str, value
                    FROM telemetry_process
                    WHERE machine_name = %s AND tag_name = ANY(%s)
                      AND time > %s::timestamptz
                      AND (value_str IS NOT NULL OR value IS NOT NULL)
                    ORDER BY time, id
                """, (machine.name, list(tag_names), since_wall.strftime('%Y-%m-%d %H:%M:%S.%f')))
                rows = cursor.fetchall()
        events = []
        for stamp, tag_name, text, number in rows:
            wall = stamp.astimezone(pytz.utc).replace(tzinfo=None) if stamp.tzinfo else stamp
            cleaned = signature_text(text, number)
            if cleaned:
                moment = self._from_wall(workcenter, wall).replace(microsecond=0)
                events.append((moment, tag_name, cleaned))
        return events

    def _collect(self, workcenter, full=False):
        workcenter = workcenter.sudo()
        lines = workcenter.signature_line_ids.filtered(lambda line: line.active and line.signal_id.tag_name)
        tag_roles = {}
        for line in lines:
            tag_roles.setdefault(line.signal_id.tag_name, []).append(line.role_id)
        if not tag_roles or not workcenter.machine_settings_id:
            return
        Interval = self.env['mes.signature.interval'].sudo()
        now = fields.Datetime.now()
        if full:
            earliest = Interval.search([('workcenter_id', '=', workcenter.id)], order='date_start', limit=1)
            since = earliest.date_start - timedelta(seconds=1) if earliest \
                else now - timedelta(days=BACKFILL_DAYS)
            Interval.search([('workcenter_id', '=', workcenter.id)]).unlink()
            current = Interval.browse()
        else:
            current = Interval.search([
                ('workcenter_id', '=', workcenter.id),
                ('date_end', '=', False),
            ], order='date_start desc', limit=1)
            since = current.date_start if current else now - timedelta(days=BACKFILL_DAYS)
            if current and workcenter.signature_scanned_to:
                since = max(since, workcenter.signature_scanned_to - RESCAN_MARGIN)
        events = self._fetch_events(workcenter, list(tag_roles), since)
        workcenter.signature_scanned_to = now
        if not events:
            return
        self._register_raw_values(workcenter, tag_roles, events)

        committed = json.loads(current.raw_json or '{}') if current else {}
        debounce = timedelta(minutes=max(workcenter.signature_debounce_min, 0))
        transitions = self._stable_transitions(committed, events, tag_roles, debounce, now)
        if not transitions:
            return

        roles = {role.code: role for role_list in tag_roles.values() for role in role_list}
        for start, state in transitions:
            payload = {
                'raw_json': json.dumps(state, sort_keys=True, ensure_ascii=False),
                'raw_label': self._raw_label(state, roles),
            }
            if current and start <= current.date_start:
                current.write(payload)
                continue
            if current:
                current.date_end = start
            current = Interval.create(dict(payload, workcenter_id=workcenter.id, date_start=start))
        workcenter._mark_product_dirty(transitions[0][0])

    @api.model
    def _stable_transitions(self, committed, events, tag_roles, debounce, now):
        transitions = []
        state = dict(committed)
        candidate = dict(committed)
        flurry_start = last_change = None
        for moment, tag_name, text in events:
            roles = tag_roles.get(tag_name)
            if not roles:
                continue
            if flurry_start is not None and moment - last_change >= debounce:
                if candidate != state:
                    transitions.append((flurry_start, dict(candidate)))
                    state = dict(candidate)
                flurry_start = None
            changed = False
            for role in roles:
                if candidate.get(role.code) != text:
                    candidate[role.code] = text
                    changed = True
            if not changed:
                continue
            if flurry_start is None:
                flurry_start = moment
            last_change = moment
        if flurry_start is not None and now - last_change >= debounce and candidate != state:
            transitions.append((flurry_start, dict(candidate)))
        return transitions

    @api.model
    def _raw_label(self, state, roles):
        ordered = sorted(state.items(), key=lambda item: (roles[item[0]].sequence if item[0] in roles else 999, item[0]))
        return ' / '.join(text for _code, text in ordered)

    def _register_raw_values(self, workcenter, tag_roles, events):
        seen = {}
        for moment, tag_name, text in events:
            for role in tag_roles.get(tag_name, []):
                first, last = seen.get((role.id, text), (moment, moment))
                seen[(role.id, text)] = (min(first, moment), max(last, moment))
        Alias = self.env['mes.signature.alias'].sudo()
        unmapped = {}
        for (role_id, text), (first, last) in seen.items():
            candidates = Alias.search([
                ('role_id', '=', role_id),
                ('raw_text', '=', text),
                '|', ('workcenter_id', '=', workcenter.id), ('workcenter_id', '=', False),
            ])
            own = candidates.filtered(lambda alias: alias.workcenter_id == workcenter)[:1]
            shared = candidates.filtered(lambda alias: not alias.workcenter_id and alias.value_id)[:1]
            target = own or shared
            if target:
                if not target.last_seen or last > target.last_seen:
                    target.last_seen = last
                continue
            if role_id not in unmapped:
                unmapped[role_id] = Alias.search_count([
                    ('role_id', '=', role_id),
                    ('workcenter_id', '=', workcenter.id),
                    ('value_id', '=', False),
                ])
            if unmapped[role_id] >= MAX_UNMAPPED_PER_ROLE:
                _logger.warning(
                    "Machine %s reports too many different values for signature role %s; the process "
                    "is probably a measurement rather than a code. New raw values are not recorded.",
                    workcenter.name, role_id)
                continue
            unmapped[role_id] += 1
            Alias.create({
                'raw_text': text,
                'role_id': role_id,
                'workcenter_id': workcenter.id,
                'first_seen': first,
                'last_seen': last,
            })

    def _rebuild(self, workcenter, moment):
        Run = self.env['mes.production.run'].sudo()
        anchor = Run.search([
            ('workcenter_id', '=', workcenter.id),
            ('date_start', '<=', moment),
            '|', ('date_end', '=', False), ('date_end', '>', moment),
        ], order='date_start desc', limit=1)
        start = anchor.date_start if anchor else moment
        Run.search([('workcenter_id', '=', workcenter.id), ('date_start', '>=', start)]).unlink()
        previous = Run.search([
            ('workcenter_id', '=', workcenter.id),
            ('date_end', '=', start),
        ], order='date_start desc', limit=1)

        now = fields.Datetime.now()
        pieces = self._merged_pieces(workcenter, start, now)
        if pieces and previous and pieces[0]['start'] == start and self._run_key(previous) == pieces[0]['key']:
            previous.date_end = pieces[0]['end']
            pieces = pieces[1:]
        for piece in pieces:
            Run.create({
                'workcenter_id': workcenter.id,
                'product_id': piece['product'].id or False,
                'date_start': piece['start'],
                'date_end': piece['end'],
                'source': piece['source'],
                'override_id': piece['override'].id or False,
                'production_id': piece['production'].id or False,
                'signature_label': piece['signature_label'],
                'unmapped_label': piece['unmapped'],
                'warning_label': piece['warning'],
            })
        workcenter._mark_product_propagate(start)

    def _propagate(self, workcenter, since):
        runs = self.env['mes.production.run'].sudo().search([
            ('workcenter_id', '=', workcenter.id),
            '|', ('date_end', '=', False), ('date_end', '>', since - RECENT_RESTAMP),
        ], order='date_start')
        self._stamp_facts(workcenter, since, runs)
        self._stamp_operators(workcenter, since)
        self._refresh_shift_products(workcenter, since, runs)

    def _stamp_operators(self, workcenter, since):
        entries = self.env['mes.performance.login.entry'].sudo().search([
            ('workcenter_id', '=', workcenter.id),
            ('role', '=', 'operator'),
            '|', ('logout_at', '=', False), ('logout_at', '>', since),
        ])
        spans = sorted((entry.login_at, entry.logout_at or FAR_FUTURE, entry.id, entry.employee_id.id)
                       for entry in entries)
        for model_name in INTERVALS:
            Model = self.env[model_name].sudo()
            rows = Model.search_read([
                ('performance_id.machine_id', '=', workcenter.id),
                ('start_time', '>=', since),
                ('end_time', '!=', False),
            ], ['start_time', 'operator_id'])
            def operator_at(moment):
                present = [employee for start, end, _entry, employee in spans if start <= moment < end]
                return present[-1] if present else None
            self._write_changes(Model, 'operator_id', rows, 'start_time', operator_at)

    def _write_changes(self, Model, field_name, rows, time_field, target_at):
        changes = {}
        for row in rows:
            if not row[time_field]:
                continue
            target = target_at(row[time_field])
            current = row[field_name][0] if row[field_name] else None
            if current != target:
                changes.setdefault(target, []).append(row['id'])
        for target, ids in changes.items():
            self.env.cr.execute(
                f'UPDATE "{Model._table}" SET {field_name} = %s WHERE id = ANY(%s)', (target, ids))
        if changes:
            Model.invalidate_model([field_name])

    @api.model
    def _run_spans(self, runs):
        return [(run.date_start, run.date_end or FAR_FUTURE, run) for run in runs]

    @api.model
    def _run_at(self, spans, starts, moment):
        index = bisect_right(starts, moment) - 1
        if index >= 0 and spans[index][0] <= moment < spans[index][1]:
            return spans[index][2]
        return None

    def _stamp_facts(self, workcenter, since, runs):
        spans = self._run_spans(runs)
        starts = [span[0] for span in spans]
        for model_name, time_field, machine_path in FACTS:
            Model = self.env[model_name].sudo().with_context(active_test=False)
            domain = [(machine_path, '=', workcenter.id), (time_field, '>=', since)]
            if model_name in INTERVALS:
                domain.append(('end_time', '!=', False))
            rows = Model.search_read(domain, [time_field, 'product_id'])
            def product_at(moment):
                run = self._run_at(spans, starts, moment)
                return run.product_id.id if run and run.product_id else None
            self._write_changes(Model, 'product_id', rows, time_field, product_at)

    def _refresh_shift_products(self, workcenter, since, runs):
        Report = self.env['mes.machine.performance'].sudo()
        Line = self.env['mes.performance.product.line'].sudo()
        now = fields.Datetime.now()
        reports = Report.search([
            ('machine_id', '=', workcenter.id),
            ('date', '>=', (since - timedelta(days=1)).date()),
        ])
        for report in reports:
            window_start, window_end = report._performance_window()
            start = self._from_wall(workcenter, window_start)
            end = self._from_wall(workcenter, window_end)
            if end <= since:
                continue
            wanted = []
            for run in runs:
                run_end = run.date_end or FAR_FUTURE
                piece_start, piece_end = max(run.date_start, start), min(run_end, end)
                if piece_end <= piece_start or piece_start > now:
                    continue
                ongoing = not run.date_end and end > now
                wanted.append((
                    run.product_id.id or False, piece_start, False if ongoing else piece_end,
                    run.source, run.id, run.date_start <= start,
                ))
            existing = [(line.product_id.id or False, line.date_start, line.date_end or False,
                         line.source, line.run_id.id or False, line.started_shift)
                        for line in report.product_line_ids.sorted('date_start')]
            if existing == wanted:
                continue
            report.product_line_ids.unlink()
            Line.create([{
                'performance_id': report.id,
                'product_id': product_id,
                'date_start': piece_start,
                'date_end': piece_end,
                'source': source,
                'run_id': run_id,
                'started_shift': started,
            } for product_id, piece_start, piece_end, source, run_id, started in wanted])

    @api.model
    def _piece_key(self, product_id, source, override_id, unmapped, warning):
        return (product_id or False, source, override_id or False,
                (unmapped or '') if source == 'unknown' else '', warning or '')

    @api.model
    def _run_key(self, run):
        return self._piece_key(run.product_id.id, run.source, run.override_id.id,
                               run.unmapped_label, run.warning_label)

    def _merged_pieces(self, workcenter, start, now):
        context = self._resolution_context(workcenter, start, now)
        points = {start, now}
        for interval in context['intervals']:
            points.update([interval.date_start, interval.date_end or now])
        for span_start, span_end, _entry in context['spans']:
            points.update([span_start, span_end])
        for config in context['configs']:
            points.add(config.date_from)
        points = sorted(point for point in points if start <= point <= now)

        pieces = []
        for begin, finish in zip(points, points[1:]):
            if finish <= begin:
                continue
            resolved = self._resolve_at(begin, context)
            if not resolved:
                continue
            key = self._piece_key(resolved['product'].id, resolved['source'], resolved['override'].id,
                                  resolved['unmapped'], resolved['warning'])
            end_value = False if finish >= now else finish
            if pieces and pieces[-1]['key'] == key and pieces[-1]['end'] == begin:
                pieces[-1]['end'] = end_value
                continue
            pieces.append(dict(resolved, key=key, start=begin, end=end_value))
        return pieces

    def _resolution_context(self, workcenter, start, now):
        Interval = self.env['mes.signature.interval'].sudo()
        Override = self.env['mes.product.override'].sudo()
        intervals = Interval.search([
            ('workcenter_id', '=', workcenter.id),
            '|', ('date_end', '=', False), ('date_end', '>', start),
        ], order='date_start')
        interval_starts = sorted(Interval.search([('workcenter_id', '=', workcenter.id)]).mapped('date_start'))
        overrides = Override.search([('workcenter_id', '=', workcenter.id)], order='date_start, id')
        spans = []
        for entry in overrides:
            end = entry.date_end
            if not end:
                limits = [now]
                limits += [other.date_start for other in overrides if other.date_start > entry.date_start][:1]
                limits += [moment for moment in interval_starts if moment > entry.date_start][:1]
                end = min(limits)
            if end > entry.date_start:
                spans.append((entry.date_start, end, entry))

        measured_roles = workcenter.signature_line_ids.filtered('active').mapped('role_id')
        aliases = {}
        for alias in self.env['mes.signature.alias'].sudo().search([('value_id', '!=', False)]):
            if alias.workcenter_id and alias.workcenter_id != workcenter:
                continue
            key = (alias.role_id.id, match_key(alias.raw_text))
            if alias.workcenter_id or key not in aliases:
                aliases[key] = alias.value_id

        rules = self.env['mes.product.rule'].sudo().search([]).filtered(
            lambda rule: not rule.workcenter_ids or workcenter in rule.workcenter_ids)
        conflicts = [(conflict.value_a_id, conflict.value_b_id, conflict.note or '')
                     for conflict in self.env['mes.signature.conflict'].sudo().search([])]
        return {
            'intervals': intervals,
            'spans': spans,
            'configs': self.env['mes.workcenter.product.config'].sudo().search(
                [('workcenter_id', '=', workcenter.id)], order='date_from'),
            'roles': {role.code: role for role in self.env['mes.signature.role'].sudo().search([])},
            'measured_roles': measured_roles,
            'aliases': aliases,
            'rules': rules,
            'conflicts': conflicts,
        }

    def _resolve_at(self, moment, context):
        interval = next((item for item in context['intervals']
                         if item.date_start <= moment < (item.date_end or FAR_FUTURE)), None)
        values, unmapped = self._normalize(interval, moment, context)
        label = ' / '.join(value.name for value in values.sorted(lambda v: (v.role_id.sequence, v.name)))

        covering = [span for span in context['spans'] if span[0] <= moment < span[1]]
        if covering:
            entry = max(covering, key=lambda span: (span[0], span[2].id))[2]
            return {'product': entry.product_id, 'source': 'manual', 'override': entry,
                    'production': entry.production_id, 'signature_label': label,
                    'unmapped': ', '.join(unmapped), 'warning': ''}
        if not interval:
            return None

        product = self._match_rule(values, context)
        problems = list(unmapped)
        config = self._config_at(moment, context)
        if not product:
            product, missing = self._match_variant(values, config)
            problems += missing
        warnings = self._consistency_warnings(values, config, context)
        Override = self.env['mes.product.override']
        return {
            'product': product,
            'source': 'signature' if product else 'unknown',
            'override': Override,
            'production': self.env['mrp.production'],
            'signature_label': label or interval.raw_label,
            'unmapped': ', '.join(problems if not product else unmapped),
            'warning': '; '.join(warnings),
        }

    def _normalize(self, interval, moment, context):
        values = self.env['mes.signature.value']
        unmapped = []
        if interval:
            for code, text in sorted(json.loads(interval.raw_json or '{}').items()):
                role = context['roles'].get(code)
                if not role:
                    continue
                value = context['aliases'].get((role.id, match_key(text)))
                if value:
                    values |= value
                else:
                    unmapped.append(f"{role.name}: {text}")
        config = self._config_at(moment, context)
        if config:
            values |= config.value_ids.filtered(lambda v: v.role_id not in context['measured_roles'])
        return values, unmapped

    def _consistency_warnings(self, values, config, context):
        warnings = []
        present = set(values.ids)
        for value_a, value_b, note in context.get('conflicts', []):
            if value_a.id in present and value_b.id in present:
                text = _("%(first)s together with %(second)s",
                         first=value_a.display_name, second=value_b.display_name)
                warnings.append(f"{text}: {note}" if note else text)
        if config:
            fixed = {value.attribute_id.id: value for value in config.attribute_value_ids}
            for value in values:
                reported = value.attribute_value_id
                if not reported:
                    continue
                configured = fixed.get(reported.attribute_id.id)
                if configured and configured != reported:
                    warnings.append(_(
                        "machine reports %(attribute)s %(reported)s, configuration says %(configured)s",
                        attribute=reported.attribute_id.name, reported=reported.name,
                        configured=configured.name))
        return warnings

    @api.model
    def _config_at(self, moment, context):
        effective = [item for item in context['configs'] if item.date_from <= moment]
        return effective[-1] if effective else None

    def _match_variant(self, values, config):
        Product = self.env['product.product']
        if not config or not config.product_tmpl_id:
            return Product, [_("no output product in the machine configuration")]
        template = config.product_tmpl_id
        chosen = {value.attribute_id.id: value for value in config.attribute_value_ids}
        for value in values:
            if value.attribute_value_id:
                chosen[value.attribute_value_id.attribute_id.id] = value.attribute_value_id
        lines = template.attribute_line_ids.filtered(
            lambda line: line.attribute_id.create_variant != 'no_variant')
        attributes = []
        repeated = []
        for line in lines:
            if line.attribute_id in attributes:
                if line.attribute_id not in repeated:
                    repeated.append(line.attribute_id)
                continue
            attributes.append(line.attribute_id)
        if repeated:
            return Product, [_(
                "%(product)s lists %(attributes)s on several lines; keep each attribute on one line "
                "with all its values",
                product=template.name, attributes=', '.join(a.name for a in repeated))]
        missing = [attribute.name for attribute in attributes if attribute.id not in chosen]
        if missing:
            return Product, [_("not determined: %s") % ', '.join(missing)]
        wanted = {chosen[attribute.id].id for attribute in attributes}
        for variant in template.product_variant_ids:
            have = set(variant.product_template_attribute_value_ids.mapped('product_attribute_value_id').ids)
            if have == wanted:
                return variant, []
        combination = ', '.join(chosen[attribute.id].name for attribute in attributes)
        return Product, [_("no variant %(product)s (%(values)s)",
                           product=template.name, values=combination)]

    def _match_rule(self, values, context):
        present = set(values.ids)
        best = None
        for rule in context['rules']:
            required = set(rule.value_ids.ids)
            if not required or not required <= present:
                continue
            rank = (-len(required), rule.sequence, rule.id)
            if best is None or rank < best[0]:
                best = (rank, rule)
        return best[1].product_id if best else self.env['product.product']
