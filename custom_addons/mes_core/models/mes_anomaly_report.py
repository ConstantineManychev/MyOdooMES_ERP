import logging
import math
from datetime import timedelta

import pytz

from odoo import models, fields, api, _

_logger = logging.getLogger(__name__)

HISTORY_DAYS = 45
MAX_SAMPLES = 20
MIN_SAMPLES = 5
MIN_STOPS_FOR_AVERAGE = 3
MIN_RUNTIME_MIN = 30.0
DEVIATION_RATIO = 1.2
Z_WARNING = 2.0
Z_CRITICAL = 3.0
LEVELS = {'gap': -2, 'insufficient': -1, 'normal': 0, 'warning': 1, 'new': 2, 'critical': 3}


def overlap_seconds(start, end, window_start, window_end):
    return max(0.0, (min(end, window_end) - max(start, window_start)).total_seconds())


def mean_std(values):
    count = len(values)
    if not count:
        return 0.0, 0.0
    mean = sum(values) / count
    if count < 2:
        return mean, 0.0
    return mean, math.sqrt(sum((value - mean) ** 2 for value in values) / (count - 1))


def metric_floors(hours):
    hours = max(hours, 1.0 / 60.0)
    return {
        'stops_per_hour': 1.0 / hours,
        'downtime_pct': 100.0 / (60.0 * hours),
        'avg_stop_min': 1.0,
        'waste_per_hour': 1.0 / hours,
    }


def assess(current, values, floor=0.0):
    mean, std = mean_std(values)
    if len(values) < MIN_SAMPLES:
        return mean, std, 0.0, 'insufficient'
    if current > 0 and not any(values):
        return mean, std, 0.0, 'new'
    scale = max(std, floor)
    z = (current - mean) / scale if scale > 0 else 0.0
    if current <= mean * DEVIATION_RATIO:
        return mean, std, z, 'normal'
    if z >= Z_CRITICAL:
        return mean, std, z, 'critical'
    if z >= Z_WARNING:
        return mean, std, z, 'warning'
    return mean, std, z, 'normal'


def slice_totals(slices, window_start, window_end, runs, stops, excluded):
    runtime = sum(overlap_seconds(run_start, run_end, slice_start, slice_end)
                  for slice_start, slice_end in slices for run_start, run_end in runs)
    per_event = {}
    for event_id, start, end in stops:
        start, end = max(start, window_start), min(end, window_end)
        if end <= start or not any(slice_start <= start < slice_end for slice_start, slice_end in slices):
            continue
        if any(start < excluded_end and end > excluded_start for excluded_start, excluded_end in excluded):
            continue
        count, total = per_event.get(event_id, (0, 0.0))
        per_event[event_id] = (count + 1, total + (end - start).total_seconds())
    return runtime, per_event


def gapped_docs(docs, windows, outages, now):
    found = set()
    for doc_id, machine_id in docs:
        window_start, window_end = windows[doc_id]
        for outage_machine, outage_start, outage_end in outages:
            if outage_machine == machine_id and outage_start < window_end and (outage_end or now) > window_start:
                found.add(doc_id)
                break
    return found


def plan_comparisons(entries, windows, now):
    started = [entry for entry in entries if windows[entry[0]][0] <= now]
    latest = {}
    for doc_id, machine_id, shift_id in started:
        key = (machine_id, shift_id)
        if key not in latest or windows[doc_id][0] > windows[latest[key]][0]:
            latest[key] = doc_id
    plan = []
    for (machine_id, _shift_id), current_id in latest.items():
        current_start = windows[current_id][0]
        history_start = current_start - timedelta(days=HISTORY_DAYS)
        comparable = sorted(
            (doc_id for doc_id, machine, _shift in started
             if machine == machine_id and doc_id != current_id
             and history_start <= windows[doc_id][0] < current_start
             and windows[doc_id][1] <= now),
            key=lambda doc_id: windows[doc_id][0])
        plan.append((current_id, comparable))
    return plan


def build_totals(docs, windows, slices, runs, stops, excluded, now):
    totals = {}
    for doc_id, machine_id in docs:
        window_start, window_end = windows[doc_id][0], min(windows[doc_id][1], now)
        for product_id in {product for product, _start, _end in slices.get(doc_id, [])}:
            pieces = [(start, end) for product, start, end in slices[doc_id] if product == product_id]
            runtime, per_event = slice_totals(pieces, window_start, window_end, runs.get(doc_id, []),
                                              stops.get(doc_id, []), excluded.get(machine_id, []))
            if runtime > 0:
                totals[(doc_id, product_id)] = (runtime, per_event)
    return totals


def stop_excess(hours, current, assessed):
    minutes = (current['downtime_pct'] - assessed['downtime_pct'][0]) / 100.0 * hours * 60.0
    stops = (current['stops_per_hour'] - assessed['stops_per_hour'][0]) * hours
    return minutes, stops


def stop_metrics(runtime, count, seconds):
    hours = runtime / 3600.0
    return {
        'stops_per_hour': count / hours if hours > 0 else 0.0,
        'downtime_pct': seconds / runtime * 100.0 if runtime > 0 else 0.0,
        'avg_stop_min': seconds / count / 60.0 if count else 0.0,
    }


class MesAnomalyReport(models.Model):
    _name = 'mes.anomaly.report'
    _description = 'Anomalies Per Runtime Hour (Current Shift vs History)'
    _order = 'severity_rank desc, z_score desc, machine_id'

    kind = fields.Selection([('stop', 'Stop'), ('waste', 'Waste')], string='Kind', readonly=True)
    machine_id = fields.Many2one('mrp.workcenter', string='Machine', readonly=True, index=True)
    company_id = fields.Many2one('res.company', string='Company', readonly=True, index=True)
    shift_id = fields.Many2one('mes.shift', string='Shift', readonly=True)
    is_current_shift = fields.Boolean(string='Current Shift', readonly=True, index=True,
                                      help="The shift that is running on this machine right now.")
    performance_id = fields.Many2one('mes.machine.performance', string='Performance Doc', readonly=True)
    product_id = fields.Many2one('product.product', string='Product', readonly=True, index=True)
    loss_id = fields.Many2one('mes.event', string='Root Reason', readonly=True)
    count_id = fields.Many2one('mes.counts', string='Waste Count', readonly=True)
    subject = fields.Char(string='Reason', readonly=True)
    reason_group = fields.Char(string='Reason Group', readonly=True)
    runtime_hours = fields.Float(string='Runtime (h)', readonly=True, group_operator=False)

    stops_per_hour = fields.Float(string='Stops / Run Hour', readonly=True, group_operator=False)
    stops_per_hour_avg = fields.Float(string='Stops / Run Hour (History)', readonly=True, group_operator=False)
    stops_per_hour_std = fields.Float(string='Stops / Run Hour Std Dev', readonly=True, group_operator=False)
    downtime_pct = fields.Float(string='Downtime (% Of Runtime)', readonly=True, group_operator=False)
    downtime_pct_avg = fields.Float(string='Downtime % (History)', readonly=True, group_operator=False)
    downtime_pct_std = fields.Float(string='Downtime % Std Dev', readonly=True, group_operator=False)
    avg_stop_min = fields.Float(string='Avg Stop (min)', readonly=True, group_operator=False)
    avg_stop_min_avg = fields.Float(string='Avg Stop (History)', readonly=True, group_operator=False)
    avg_stop_min_std = fields.Float(string='Avg Stop Std Dev', readonly=True, group_operator=False)
    waste_per_hour = fields.Float(string='Waste / Run Hour', readonly=True, group_operator=False)
    waste_per_hour_avg = fields.Float(string='Waste / Run Hour (History)', readonly=True, group_operator=False)
    waste_per_hour_std = fields.Float(string='Waste / Run Hour Std Dev', readonly=True, group_operator=False)

    sample_count = fields.Integer(
        string='Comparable Shifts', readonly=True, group_operator=False,
        help="The most recent completed shifts (up to 20, within 45 days) of any type on the same "
             "machine in which this product ran.")
    excess_minutes = fields.Float(string='Extra Downtime (min)', readonly=True, group_operator=False,
                                  help="Downtime of this reason above its historical average, for the runtime "
                                       "of this shift so far.")
    excess_stops = fields.Float(string='Extra Stops', readonly=True, group_operator=False,
                                help="Stops of this reason above the historical average, for the runtime of "
                                     "this shift so far.")
    excess_pieces = fields.Float(string='Extra Waste (pcs)', readonly=True, group_operator=False,
                                 help="Waste of this counter above the historical average, for the runtime of "
                                      "this shift so far.")
    z_score = fields.Float(string='Z-Score', readonly=True, group_operator='max',
                           help="The highest deviation among the metrics of this row.")
    driver = fields.Char(string='Deviating Metric', readonly=True)
    status = fields.Selection([
        ('gap', 'Data Gap'),
        ('insufficient', 'Not Enough History'),
        ('normal', 'Normal'),
        ('warning', 'Warning'),
        ('new', 'New Problem'),
        ('critical', 'Critical'),
    ], string='Severity', readonly=True,
        help="Data Gap: the machine lost its connection during this shift, so its stops and waste "
             "are incomplete and are not judged. Such shifts are also left out of the history. "
             "Warning and Critical need both a statistical deviation and an excess above the limits in "
             "the MES settings; smaller deviations stay Normal.")
    severity_rank = fields.Integer(readonly=True, group_operator='max')
    computed_at = fields.Datetime(string='Computed', readonly=True)

    @api.model
    def cron_refresh(self):
        self.sudo()._rebuild(self.env['res.company'].sudo().search([]))

    def action_refresh(self):
        companies = self.env.companies
        self.sudo()._rebuild(companies)
        return {'type': 'ir.actions.client', 'tag': 'reload'}

    @api.model
    def _doc_window(self, doc):
        local_start, local_end = doc._performance_window()
        tz = pytz.timezone(doc.machine_id.company_id.tz or 'UTC')
        return tuple(tz.localize(moment, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
                     for moment in (local_start, local_end))

    @api.model
    def _live_waste(self, workcenter, start, end):
        production = workcenter.production_count_id
        totals = {}
        for item in self.env['mes.oee.breakdown'].sudo()._collect_counts(workcenter, start, end):
            if item['calculable'] and item['count'] != production:
                totals[item['count'].id] = totals.get(item['count'].id, 0.0) + item['qty']
        return totals

    @api.model
    def _anomaly_slices(self, docs, windows, now):
        return {doc.id: [(False, windows[doc.id][0], min(windows[doc.id][1], now))] for doc in docs}

    def _rebuild(self, companies):
        now = fields.Datetime.now()
        company_scope = ['|', ('company_id', '=', False), ('company_id', 'in', companies.ids)]
        Doc = self.env['mes.machine.performance']
        docs = Doc.search([
            ('date', '>=', fields.Date.context_today(self) - timedelta(days=HISTORY_DAYS + 2)),
            ('shift_id', '!=', False),
        ] + company_scope)
        windows = {doc.id: self._doc_window(doc) for doc in docs}

        by_id = {doc.id: doc for doc in docs}
        plan = [(by_id[current_id], [by_id[doc_id] for doc_id in comparable_ids])
                for current_id, comparable_ids in plan_comparisons(
                    [(doc.id, doc.machine_id.id, doc.shift_id.id) for doc in docs], windows, now)]

        involved = Doc.browse(sorted({doc.id for current, comparable in plan for doc in [current] + comparable}))
        slices = self._anomaly_slices(involved, windows, now)
        runs, stops, waste = self._load_facts(involved, windows, now)
        breakdown = self.env['mes.oee.breakdown'].sudo()
        earliest = min((windows[doc.id][0] for doc in involved), default=now)
        excluded = {machine.id: breakdown._excluded_windows(machine, earliest, now)
                    for machine in involved.mapped('machine_id')}

        totals = build_totals([(doc.id, doc.machine_id.id) for doc in involved], windows, slices,
                              runs, stops, excluded, now)
        gapped = self._gapped(involved, windows, earliest, now)

        rows = []
        for current, comparable in plan:
            rows += self._rows_for(current, comparable, windows, slices, totals, waste, now, gapped)
        self.search(company_scope).unlink()
        if rows:
            self.create(rows)

    def _gapped(self, involved, windows, earliest, now):
        if not involved:
            return frozenset()
        outages = self.env['mes.link.outage'].sudo().search([
            ('workcenter_id', 'in', involved.mapped('machine_id').ids),
            ('state', 'in', ('ongoing', 'checking', 'lost')),
            ('alerted_at', '!=', False),
            ('date_start', '<', now),
            '|', ('date_end', '=', False), ('date_end', '>', earliest),
        ])
        return frozenset(gapped_docs(
            [(doc.id, doc.machine_id.id) for doc in involved], windows,
            [(outage.workcenter_id.id, outage.date_start, outage.date_end) for outage in outages], now))

    def _load_facts(self, involved, windows, now):
        runs, stops, waste = {}, {}, {}
        for row in self.env['mes.performance.running'].search_read(
                [('performance_id', 'in', involved.ids)], ['performance_id', 'start_time', 'end_time']):
            doc_id = row['performance_id'][0]
            end = row['end_time'] or min(windows[doc_id][1], now)
            runs.setdefault(doc_id, []).append((row['start_time'], end))
        for row in self.env['mes.performance.alarm'].search_read(
                [('performance_id', 'in', involved.ids), ('loss_id', '!=', False)],
                ['performance_id', 'loss_id', 'start_time', 'end_time']):
            doc_id = row['performance_id'][0]
            end = row['end_time'] or min(windows[doc_id][1], now)
            stops.setdefault(doc_id, []).append((row['loss_id'][0], row['start_time'], end))
        for row in self.env['mes.performance.rejection'].search_read(
                [('performance_id', 'in', involved.ids), ('reason_id.calculable', '=', True)],
                ['performance_id', 'reason_id', 'qty']):
            doc_waste = waste.setdefault(row['performance_id'][0], {})
            doc_waste[row['reason_id'][0]] = doc_waste.get(row['reason_id'][0], 0.0) + (row['qty'] or 0.0)
        return runs, stops, waste

    @api.model
    def _single_product(self, doc_id, totals):
        products = [product for (key_doc, product) in totals if key_doc == doc_id]
        return products[0] if len(products) == 1 else None

    def _current_waste(self, current, product_id, slices, totals, waste, now):
        window_end = self._doc_window(current)[1]
        if window_end <= now and self._single_product(current.id, totals) == product_id:
            return waste.get(current.id, {})
        result = {}
        for product, start, end in slices.get(current.id, []):
            if product != product_id:
                continue
            for count_id, qty in self._live_waste(current.machine_id, start, end).items():
                result[count_id] = result.get(count_id, 0.0) + qty
        return result

    @api.model
    def _anomaly_limits(self, company):
        company = company or self.env.company
        return {
            'minutes': company.mes_anomaly_min_minutes,
            'stops': company.mes_anomaly_min_stops,
            'pieces': company.mes_anomaly_min_pieces,
        }

    def _rows_for(self, current, comparable, windows, slices, totals, waste, now, gapped=frozenset()):
        rows = []
        gap = current.id in gapped
        limits = self._anomaly_limits(current.company_id)
        ignored = set(self.env['mes.event'].sudo().search([('exclude_from_anomalies', '=', True)]).ids)
        for (doc_id, product_id), (runtime, per_event) in totals.items():
            if doc_id != current.id:
                continue
            history = [doc for doc in comparable
                       if (doc.id, product_id) in totals and doc.id not in gapped][-MAX_SAMPLES:]
            enough_runtime = runtime >= MIN_RUNTIME_MIN * 60
            hours = runtime / 3600.0
            floors = metric_floors(hours)
            base = {
                'is_current_shift': windows[current.id][0] <= now < windows[current.id][1],
                'machine_id': current.machine_id.id,
                'company_id': current.company_id.id or False,
                'shift_id': current.shift_id.id,
                'performance_id': current.id,
                'product_id': product_id or False,
                'runtime_hours': round(runtime / 3600.0, 2),
                'sample_count': len(history),
                'computed_at': now,
            }
            for event_id, (count, seconds) in per_event.items():
                if event_id in ignored:
                    continue
                event = self.env['mes.event'].browse(event_id)
                current_metrics = stop_metrics(runtime, count, seconds)
                past = [stop_metrics(totals[(doc.id, product_id)][0],
                                     *totals[(doc.id, product_id)][1].get(event_id, (0, 0.0)))
                        for doc in history]
                assessed = {metric: assess(current_metrics[metric], [m[metric] for m in past], floors[metric])
                            for metric in ('stops_per_hour', 'downtime_pct', 'avg_stop_min')}
                if count < MIN_STOPS_FOR_AVERAGE and assessed['avg_stop_min'][3] in ('warning', 'critical'):
                    assessed['avg_stop_min'] = assessed['avg_stop_min'][:3] + ('normal',)
                extra_minutes, extra_stops = stop_excess(hours, current_metrics, assessed)
                material = extra_minutes >= limits['minutes'] or extra_stops >= limits['stops']
                rows.append(self._row(base, 'stop', current_metrics, assessed, enough_runtime, gap, material,
                                      loss_id=event_id, subject=event.display_name,
                                      reason_group=event.parent_id.name or False,
                                      excess_minutes=round(extra_minutes, 1),
                                      excess_stops=round(extra_stops, 1)))
            waste_history = [doc for doc in history if self._single_product(doc.id, totals) == product_id]
            for count_id, qty in self._current_waste(current, product_id, slices, totals, waste, now).items():
                if qty <= 0:
                    continue
                count = self.env['mes.counts'].browse(count_id)
                value = qty / hours
                past = [waste.get(doc.id, {}).get(count_id, 0.0) / (totals[(doc.id, product_id)][0] / 3600.0)
                        for doc in waste_history]
                assessed = assess(value, past, floors['waste_per_hour'])
                extra_pieces = (value - assessed[0]) * hours
                rows.append(self._row(dict(base, sample_count=len(waste_history)), 'waste',
                                      {'waste_per_hour': value}, {'waste_per_hour': assessed},
                                      enough_runtime, gap, extra_pieces >= limits['pieces'],
                                      count_id=count_id, subject=count.display_name,
                                      reason_group=count.parent_id.name or False,
                                      excess_pieces=round(extra_pieces)))
        return rows

    @api.model
    def _row(self, base, kind, current, assessed, enough_runtime, gap=False, material=True, **extra):
        labels = {
            'stops_per_hour': _("Stops / Run Hour"),
            'downtime_pct': _("Downtime % Of Runtime"),
            'avg_stop_min': _("Average Stop"),
            'waste_per_hour': _("Waste / Run Hour"),
        }
        row = dict(base, kind=kind, **extra)
        worst_level, worst_z, driver = 'insufficient', 0.0, False
        for metric, (mean, std, z, level) in assessed.items():
            row[metric] = round(current[metric], 3)
            row[f'{metric}_avg'] = round(mean, 3)
            row[f'{metric}_std'] = round(std, 3)
            if (not enough_runtime or not material) and level in ('warning', 'critical'):
                level = 'normal'
            if (LEVELS[level], z) > (LEVELS[worst_level], worst_z):
                worst_level, worst_z = level, z
                driver = labels[metric] if level != 'insufficient' else False
        if gap:
            worst_level, worst_z, driver = 'gap', 0.0, False
        row.update({'status': worst_level, 'severity_rank': LEVELS[worst_level],
                    'z_score': round(worst_z, 2), 'driver': driver})
        return row
