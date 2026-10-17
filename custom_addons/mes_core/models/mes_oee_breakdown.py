import logging
import pytz
from datetime import timedelta
from odoo import models, fields, api, _

_logger = logging.getLogger(__name__)

NO_STREAM_KEY = 'no_stream'
OTHER_PART_KEY = 'other'


class MesOeeBreakdown(models.AbstractModel):
    _name = 'mes.oee.breakdown'
    _description = 'OEE Breakdown By Stream And Part'
    _inherit = ['mes.timescale.base']

    @api.model
    def build(self, workcenter):
        window = self._resolve_window(workcenter)
        if not window:
            return {'ok': False, 'error': _('No active shift')}
        start_utc, end_utc = window

        active_intervals, planned_sec = self.env['mes.machine.settings'] \
            ._get_planned_working_intervals(start_utc, end_utc, workcenter)
        runtime_sec = self.env['mes.machine.settings'] \
            ._fetch_interval_stats(active_intervals, workcenter.id, mode='runtime')
        runtime_hours = (runtime_sec / 3600.0) if runtime_sec else 0.0

        counts = self._collect_counts(workcenter, start_utc, end_utc)
        stops = self._collect_stops(workcenter, start_utc, end_utc)

        good_qty = self._fetch_good_quantity(workcenter, start_utc, end_utc)
        machine_waste = self._machine_waste(workcenter, runtime_sec, good_qty)
        total_stop_sec = sum(s['duration'] for s in stops)

        return {
            'ok': True,
            'machine': {
                'name': workcenter.name,
                'runtime_hours': round(runtime_hours, 2),
                'planned_hours': round(planned_sec / 3600.0, 2) if planned_sec else 0.0,
                'waste': machine_waste,
                'downtime': dict(
                    self._downtime_block(stops, total_stop_sec, planned_sec, runtime_hours),
                    loss_pct=round(workcenter.current_downtime_losses or 0.0, 1)),
                'items': self._node_items(counts, stops, machine_waste['qty'],
                                          total_stop_sec, runtime_hours),
            },
            'streams': self._build_streams(
                workcenter, counts, stops, total_stop_sec, machine_waste['qty'],
                planned_sec, runtime_hours),
        }

    @api.model
    def _resolve_window(self, workcenter):
        start_loc, end_loc = self.env['mes.shift'].sudo().get_current_shift_window(workcenter)
        if not start_loc:
            return None
        machine_tz = pytz.timezone(workcenter.company_id.tz or 'UTC')
        start_utc = machine_tz.localize(start_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
        end_utc = machine_tz.localize(end_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
        end_utc = min(end_utc, fields.Datetime.now())
        return (start_utc, end_utc) if end_utc > start_utc else None

    @api.model
    def _collect_counts(self, workcenter, start_utc, end_utc):
        machine = workcenter.machine_settings_id
        if not machine:
            return []
        signals = machine.count_tag_ids.filtered(
            lambda sig: sig.count_id and sig.tag_name)
        if not signals:
            return []

        machine_tz = pytz.timezone(workcenter.company_id.tz or 'UTC')
        to_local = lambda dt: pytz.utc.localize(dt).astimezone(machine_tz).replace(tzinfo=None)
        rows = self._fetch_count_totals(
            machine.name, sorted({sig.tag_name for sig in signals}),
            to_local(start_utc), to_local(end_utc))

        by_tag = {}
        for tag_name, sum_val, cum_val in rows:
            by_tag[tag_name] = (float(sum_val or 0), float(cum_val or 0))

        collected = []
        for sig in signals:
            sums = by_tag.get(sig.tag_name)
            if not sums:
                continue
            qty = sums[1] if sig.is_cumulative else sums[0]
            if qty <= 0:
                continue
            collected.append({
                'qty': qty,
                'count': sig.count_id,
                'name': sig.count_id.name,
                'calculable': sig.count_id.calculable,
                'stream': sig.count_id.stream_id,
                'part': sig.count_id.part_id,
                'piece': sig.count_id.piece_id,
            })
        return collected

    def _fetch_count_totals(self, machine_name, tag_names, start_local, end_local):
        try:
            with self._connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT tag_name,
                               COALESCE(SUM(value), 0),
                               COALESCE(MAX(value) - MIN(value), 0)
                        FROM telemetry_count
                        WHERE machine_name = %s AND tag_name = ANY(%s)
                          AND time >= %s::timestamptz AND time < %s::timestamptz
                        GROUP BY tag_name
                    """, (machine_name, tag_names,
                          start_local.strftime('%Y-%m-%d %H:%M:%S'),
                          end_local.strftime('%Y-%m-%d %H:%M:%S')))
                    return cursor.fetchall()
        except Exception:
            _logger.exception("OEE breakdown count fetch failed for %s", machine_name)
            return []

    @api.model
    def _collect_stops(self, workcenter, start_utc, end_utc):
        alarms = self.env['mes.performance.alarm'].sudo().search([
            ('performance_id.machine_id', '=', workcenter.id),
            ('start_time', '>=', start_utc),
            ('start_time', '<', end_utc),
        ])
        if not alarms:
            return []

        excluded = self._excluded_windows(workcenter, start_utc, end_utc)
        now_utc = fields.Datetime.now()
        stops = []
        for alarm in alarms:
            stop_end = min(alarm.end_time or now_utc, end_utc)
            if stop_end <= alarm.start_time:
                continue
            if any(alarm.start_time < win_end and stop_end > win_start
                   for win_start, win_end in excluded):
                continue
            stops.append({
                'duration': (stop_end - alarm.start_time).total_seconds(),
                'name': alarm.loss_id.name or '—',
                'calculable': alarm.loss_id.calculable,
                'stream': alarm.loss_id.stream_id,
                'part': alarm.loss_id.part_id,
                'piece': alarm.loss_id.piece_id,
            })
        return stops

    @api.model
    def _excluded_windows(self, workcenter, start_utc, end_utc):
        now_utc = fields.Datetime.now()
        windows = []
        planned = self.env['mes.flat.downtime'].sudo().search([
            ('machine_id', '=', workcenter.id),
            ('start_time', '<', end_utc),
            ('end_time', '>', start_utc),
        ])
        windows += [(slot.start_time, slot.end_time) for slot in planned]

        intervals = self.env['mes.task.work.interval'].sudo().search([
            ('machine_id', '=', workcenter.id),
            ('start_time', '<', end_utc),
            '|', ('end_time', '=', False), ('end_time', '>', start_utc),
        ])
        windows += [(iv.start_time, iv.end_time or now_utc) for iv in intervals]
        return windows

    @api.model
    def _fetch_good_quantity(self, workcenter, start_utc, end_utc):
        machine = workcenter.machine_settings_id
        production_count = workcenter.production_count_id
        if not machine or not production_count:
            return 0.0
        signals = machine.count_tag_ids.filtered(
            lambda sig: sig.count_id == production_count and sig.tag_name)
        if not signals:
            return 0.0

        machine_tz = pytz.timezone(workcenter.company_id.tz or 'UTC')
        to_local = lambda dt: pytz.utc.localize(dt).astimezone(machine_tz).replace(tzinfo=None)
        rows = self._fetch_count_totals(
            machine.name, sorted({sig.tag_name for sig in signals}),
            to_local(start_utc), to_local(end_utc))
        by_tag = {row[0]: (float(row[1] or 0), float(row[2] or 0)) for row in rows}

        total = 0.0
        for sig in signals:
            sums = by_tag.get(sig.tag_name)
            if sums:
                total += sums[1] if sig.is_cumulative else sums[0]
        return total

    @api.model
    def _machine_waste(self, workcenter, runtime_sec, good_qty):
        capacity = workcenter.ideal_capacity_per_min or 0.0
        expected = capacity * (runtime_sec / 60.0) if runtime_sec else 0.0
        waste = max(0.0, expected - (good_qty or 0.0))
        runtime_hours = (runtime_sec / 3600.0) if runtime_sec else 0.0
        return {
            'qty': round(waste),
            'expected': round(expected),
            'good': round(good_qty or 0.0),
            'loss_pct': round(workcenter.current_waste_losses or 0.0, 1),
            'per_hour': round(waste / runtime_hours) if runtime_hours > 0 else 0,
        }

    @api.model
    def _node_items(self, counts, stops, machine_waste_qty, total_stop_sec, runtime_hours):
        items = []

        by_count = {}
        for row in counts:
            key = row['count'].id
            entry = by_count.setdefault(key, {
                'kind': 'count', 'name': row['name'],
                'calculable': row['calculable'], 'qty': 0.0})
            entry['qty'] += row['qty']
        for entry in by_count.values():
            if entry['qty'] <= 0:
                continue
            items.append({
                'kind': 'count',
                'name': entry['name'],
                'calculable': entry['calculable'],
                'qty': round(entry['qty']),
                'pct': round(entry['qty'] / machine_waste_qty * 100, 1)
                       if machine_waste_qty > 0 else 0.0,
                'per_hour': round(entry['qty'] / runtime_hours) if runtime_hours > 0 else 0,
                'stops': 0, 'duration_text': '', 'avg_text': '',
            })

        by_event = {}
        for row in stops:
            entry = by_event.setdefault(row['name'], {
                'name': row['name'], 'calculable': row['calculable'],
                'duration': 0.0, 'stops': 0})
            entry['duration'] += row['duration']
            entry['stops'] += 1
        for entry in by_event.values():
            if entry['stops'] <= 0:
                continue
            items.append({
                'kind': 'event',
                'name': entry['name'],
                'calculable': entry['calculable'],
                'qty': entry['stops'],
                'stops': entry['stops'],
                'pct': round(entry['duration'] / total_stop_sec * 100, 1)
                       if total_stop_sec > 0 else 0.0,
                'per_hour': round(entry['stops'] / runtime_hours, 1) if runtime_hours > 0 else 0.0,
                'duration_text': self._format_hms(entry['duration']),
                'avg_text': self._format_hms(entry['duration'] / entry['stops']),
            })

        items.sort(key=lambda i: (i['kind'], -i['pct'], i['name']))
        return items

    @api.model
    def _waste_block(self, qty, machine_waste_qty, runtime_hours):
        return {
            'qty': round(qty),
            'loss_pct': round(qty / machine_waste_qty * 100, 1) if machine_waste_qty > 0 else 0.0,
            'per_hour': round(qty / runtime_hours) if runtime_hours > 0 else 0,
        }

    @api.model
    def _downtime_block(self, stops, total_stop_sec, planned_sec, runtime_hours):
        duration = sum(s['duration'] for s in stops)
        number = len(stops)
        return {
            'stops': number,
            'stops_per_hour': round(number / runtime_hours, 1) if runtime_hours > 0 else 0.0,
            'duration_sec': int(duration),
            'duration_text': self._format_hms(duration),
            'avg_sec': int(duration / number) if number else 0,
            'avg_text': self._format_hms(duration / number) if number else '00:00:00',
            'loss_pct': round(duration / total_stop_sec * 100, 1) if total_stop_sec > 0
                        else (round(duration / planned_sec * 100, 1) if planned_sec > 0 else 0.0),
        }

    @staticmethod
    def _format_hms(seconds):
        total = int(round(seconds or 0))
        return f"{total // 3600:02d}:{(total % 3600) // 60:02d}:{total % 60:02d}"

    @api.model
    def _build_streams(self, workcenter, counts, stops, total_stop_sec,
                       machine_waste_qty, planned_sec, runtime_hours):
        fitted_parts = workcenter.part_line_ids.mapped('part_id')
        buckets = {}

        def bucket(stream):
            key = stream.id if stream else NO_STREAM_KEY
            if key not in buckets:
                buckets[key] = {
                    'id': stream.id if stream else False,
                    'name': stream.name if stream else _('No stream'),
                    'counts': [], 'stops': [],
                }
            return buckets[key]

        for row in counts:
            bucket(row['stream'])['counts'].append(row)
        for row in stops:
            bucket(row['stream'])['stops'].append(row)

        result = []
        for key, data in buckets.items():
            parts = self._build_parts(
                data, fitted_parts, machine_waste_qty, total_stop_sec,
                planned_sec, runtime_hours)
            stream_waste_qty = self._resolve_stream_waste(data, parts)
            result.append({
                'id': data['id'],
                'key': f"stream-{key}",
                'name': data['name'],
                'waste': self._waste_block(stream_waste_qty, machine_waste_qty, runtime_hours),
                'downtime': self._downtime_block(
                    data['stops'], total_stop_sec, planned_sec, runtime_hours),
                'items': self._node_items(data['counts'], data['stops'],
                                          machine_waste_qty, total_stop_sec, runtime_hours),
                'parts': parts,
            })
        result.sort(key=lambda r: (r['key'] == f"stream-{NO_STREAM_KEY}",
                                   -r['waste']['qty'], r['name']))
        return result

    @api.model
    def _build_pieces(self, part_data, machine_waste_qty, total_stop_sec,
                      planned_sec, runtime_hours):
        buckets = {}

        def bucket(piece):
            key = piece.id if piece else 'other'
            if key not in buckets:
                buckets[key] = {
                    'id': piece.id if piece else False,
                    'name': piece.name if piece else _('Other'),
                    'counts': [], 'stops': [],
                }
            return buckets[key]

        for row in part_data['counts']:
            if row['piece']:
                bucket(row['piece'])['counts'].append(row)
        for row in part_data['stops']:
            if row['piece']:
                bucket(row['piece'])['stops'].append(row)

        pieces = []
        for key, data in buckets.items():
            qty = sum(row['qty'] for row in data['counts'] if row['calculable'])
            pieces.append({
                'id': data['id'],
                'key': f"piece-{key}",
                'name': data['name'],
                'waste': self._waste_block(qty, machine_waste_qty, runtime_hours),
                'downtime': self._downtime_block(
                    data['stops'], total_stop_sec, planned_sec, runtime_hours),
                'items': self._node_items(data['counts'], data['stops'],
                                          machine_waste_qty, total_stop_sec, runtime_hours),
            })
        pieces.sort(key=lambda r: (-r['waste']['qty'], r['name']))
        return pieces

    @api.model
    def _resolve_stream_waste(self, data, parts):
        direct = sum(row['qty'] for row in data['counts']
                     if row['calculable'] and not row['part'])
        if direct > 0:
            return direct
        return sum(part['waste']['qty'] for part in parts)

    @api.model
    def _build_parts(self, data, fitted_parts, machine_waste_qty, total_stop_sec,
                     planned_sec, runtime_hours):
        buckets = {}

        def bucket(part):
            usable = part and part in fitted_parts
            key = part.id if usable else OTHER_PART_KEY
            if key not in buckets:
                buckets[key] = {
                    'id': part.id if usable else False,
                    'name': part.name if usable else _('Other'),
                    'counts': [], 'stops': [],
                }
            return buckets[key]

        for row in data['counts']:
            bucket(row['part'])['counts'].append(row)
        for row in data['stops']:
            bucket(row['part'])['stops'].append(row)

        parts = []
        for key, part_data in buckets.items():
            calc = [row for row in part_data['counts'] if row['calculable']]
            without_pieces = sum(row['qty'] for row in calc if not row['piece'])
            qty = without_pieces or sum(row['qty'] for row in calc)
            pieces = self._build_pieces(
                part_data, machine_waste_qty, total_stop_sec, planned_sec, runtime_hours)
            parts.append({
                'id': part_data['id'],
                'key': f"part-{key}",
                'name': part_data['name'],
                'waste': self._waste_block(qty, machine_waste_qty, runtime_hours),
                'downtime': self._downtime_block(
                    part_data['stops'], total_stop_sec, planned_sec, runtime_hours),
                'items': self._node_items(part_data['counts'], part_data['stops'],
                                          machine_waste_qty, total_stop_sec, runtime_hours),
                'pieces': pieces,
            })
        parts.sort(key=lambda r: (r['key'] == f"part-{OTHER_PART_KEY}",
                                  -r['waste']['qty'], r['name']))
        return parts
