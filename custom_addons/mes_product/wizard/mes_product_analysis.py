from bisect import bisect_right
from datetime import datetime

from odoo import models, fields, api, _

FAR_FUTURE = datetime(9999, 1, 1)
DIMENSIONS = ('machine', 'product', 'employee', 'event', 'period')


class MesProductAnalysisWizard(models.TransientModel):
    _name = 'mes.product.analysis.wizard'
    _inherit = 'mes.report.base.wizard'
    _description = 'Product Analysis Matrix Wizard'

    product_ids = fields.Many2many('product.product', string='Products')
    row_by_product = fields.Boolean(string='Product (Rows)', default=True)
    col_by_product = fields.Boolean(string='Product (Columns)', default=False)
    row_by_employee = fields.Boolean(string='Operator (Rows)', default=False)
    col_by_employee = fields.Boolean(string='Operator (Columns)', default=False)
    row_by_event = fields.Boolean(string='Stop Reason (Rows)', default=False)
    col_by_event = fields.Boolean(string='Stop Reason (Columns)', default=False)

    show_frequency = fields.Boolean(string='Number Of Stops', default=True)
    show_freq_per_hour = fields.Boolean(string='Stops / Run Hour', default=True)
    show_total_time = fields.Boolean(string='Stop Time (min)', default=True)
    show_avg_time_per_stop = fields.Boolean(string='Avg Stop (min)', default=False)
    show_time_per_hour = fields.Boolean(string='Stop Min / Run Hour', default=False)
    show_run_hours = fields.Boolean(string='Run Hours', default=True)
    show_slow_time = fields.Boolean(string='Slow Time (min)', default=False)

    @api.model
    def _get_limit_by_options(self):
        return [
            ('frequency', 'Number Of Stops'),
            ('freq_per_hour', 'Stops / Run Hour'),
            ('total_time', 'Stop Time'),
            ('run_hours', 'Run Hours'),
        ]

    def _enabled(self, dimension, axis):
        return bool(getattr(self, f'{axis}_by_{dimension}', False))

    def _grouped_dimensions(self):
        return [d for d in DIMENSIONS if self._enabled(d, 'row') or self._enabled(d, 'col')]

    @api.model
    def _build_slices(self, runs, entries, blocks, start, end):
        points = {start, end}
        for run in runs:
            points.update([run.date_start, run.date_end or end])
        for entry in entries:
            points.update([entry.login_at, entry.logout_at or end])
        for block_start, block_end, _name in blocks:
            points.update([block_start, block_end])
        points = sorted(point for point in points if start <= point <= end)
        slices = []
        for slice_start, slice_end in zip(points, points[1:]):
            if slice_end <= slice_start:
                continue
            run = next((item for item in reversed(runs)
                        if item.date_start <= slice_start < (item.date_end or FAR_FUTURE)), None)
            present = [item for item in entries
                       if item.login_at <= slice_start < (item.logout_at or FAR_FUTURE)]
            block = next((item for item in blocks if item[0] <= slice_start < item[1]), None)
            slices.append((slice_start, slice_end,
                           run.product_id if run else None,
                           present[-1].employee_id if present else None,
                           block[2] if block else None))
        return slices

    @api.model
    def _overlaps(self, slices, starts, begin, finish):
        index = max(bisect_right(starts, begin) - 1, 0)
        while index < len(slices) and slices[index][0] < finish:
            piece_start, piece_end = max(slices[index][0], begin), min(slices[index][1], finish)
            if piece_end > piece_start:
                yield slices[index], (piece_end - piece_start).total_seconds()
            index += 1

    @api.model
    def _slice_at(self, slices, starts, moment):
        index = bisect_right(starts, moment) - 1
        if index >= 0 and slices[index][0] <= moment < slices[index][1]:
            return slices[index]
        return None

    def action_generate_report(self):
        Line = self.env['mes.product.analysis.line']
        Line.search([('user_id', '=', self.env.user.id)]).unlink()
        now = fields.Datetime.now()
        report_end = min(self.end_datetime, now)
        grouped = self._grouped_dimensions()
        stops, runs_by_key, slows, labels = {}, {}, {}, {}
        wanted_products = set(self.product_ids.ids)

        for machine in self._get_filtered_machines():
            workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
            if not workcenter or report_end <= self.start_datetime:
                continue
            tz_name = workcenter.company_id.tz or 'UTC'
            shifts = self.env['mes.shift'].search(
                [('company_id', '=', workcenter.company_id.id)], order='start_hour asc')
            periods = self._get_logical_periods(self.start_datetime, self.end_datetime, shifts, tz_name)
            blocks = sorted((block_start, block_end, name)
                            for name, spans in periods.items() for block_start, block_end in spans)
            overlap = [
                ('start_time', '<', report_end),
                '|', ('end_time', '=', False), ('end_time', '>', self.start_datetime),
            ]
            product_runs = self.env['mes.production.run'].sudo().search([
                ('workcenter_id', '=', workcenter.id),
                ('date_start', '<', report_end),
                '|', ('date_end', '=', False), ('date_end', '>', self.start_datetime),
            ], order='date_start')
            entries = self.env['mes.performance.login.entry'].sudo().search([
                ('workcenter_id', '=', workcenter.id),
                ('role', '=', 'operator'),
                ('login_at', '<', report_end),
                '|', ('logout_at', '=', False), ('logout_at', '>', self.start_datetime),
            ], order='login_at, id')
            slices = self._build_slices(list(product_runs), list(entries), blocks,
                                        self.start_datetime, report_end)
            starts = [piece[0] for piece in slices]

            def key_for(piece, event):
                _start, _end, product, operator, period = piece
                if wanted_products and (not product or product.id not in wanted_products):
                    return None
                if 'period' in grouped and not period:
                    return None
                values = {
                    'machine': (workcenter.id, workcenter.name),
                    'product': (product.id if product else 0,
                                product.display_name if product else _("No product")),
                    'employee': (operator.id if operator else 0,
                                 operator.name if operator else _("No operator")),
                    'event': (event.id if event else 0, event.name if event else _("No reason")),
                    'period': (period, period),
                }
                dims = [d for d in grouped if event is not None or d != 'event']
                for dimension in dims:
                    labels[(dimension, values[dimension][0])] = values[dimension][1]
                return tuple(values[d] for d in dims)

            def span_of(record):
                return max(record.start_time, self.start_datetime), min(record.end_time or now, report_end)

            domain = [('performance_id.machine_id', '=', workcenter.id)] + overlap
            for running in self.env['mes.performance.running'].search(domain):
                begin, finish = span_of(running)
                for piece, seconds in self._overlaps(slices, starts, begin, finish):
                    key = key_for(piece, None)
                    if key is not None:
                        runs_by_key[key] = runs_by_key.get(key, 0.0) + seconds / 3600.0
            for slowing in self.env['mes.performance.slowing'].search(domain):
                begin, finish = span_of(slowing)
                for piece, seconds in self._overlaps(slices, starts, begin, finish):
                    key = key_for(piece, None)
                    if key is not None:
                        slows[key] = slows.get(key, 0.0) + seconds / 60.0
            for alarm in self.env['mes.performance.alarm'].search(domain):
                event = alarm.loss_id
                if alarm.start_time >= self.start_datetime:
                    piece = self._slice_at(slices, starts, alarm.start_time)
                    key = key_for(piece, event) if piece else None
                    if key is not None:
                        stops.setdefault(key, {'freq': 0, 'dur': 0.0})['freq'] += 1
                begin, finish = span_of(alarm)
                for piece, seconds in self._overlaps(slices, starts, begin, finish):
                    key = key_for(piece, event)
                    if key is not None:
                        stops.setdefault(key, {'freq': 0, 'dur': 0.0})['dur'] += seconds / 60.0

        event_index = grouped.index('event') if 'event' in grouped else None

        def base_of(key):
            return key if event_index is None else key[:event_index] + key[event_index + 1:]

        keys = set(stops)
        if event_index is None:
            keys |= set(runs_by_key) | set(slows)

        rows = []
        for key in keys:
            data = stops.get(key, {'freq': 0, 'dur': 0.0})
            base = base_of(key)
            hours = runs_by_key.get(base, 0.0)
            by_dimension = dict(zip(grouped, key))
            row_parts = [labels[(d, by_dimension[d][0])] for d in grouped if self._enabled(d, 'row')]
            col_parts = [labels[(d, by_dimension[d][0])] for d in grouped if self._enabled(d, 'col')]
            rows.append({
                'user_id': self.env.user.id,
                'period_name': by_dimension['period'][0] if 'period' in by_dimension else False,
                'row_group_label': " | ".join(row_parts) if row_parts else _("All Data"),
                'col_group_label': " | ".join(col_parts) if col_parts else _("All Data"),
                'frequency': data['freq'],
                'total_time': data['dur'],
                'avg_time_per_stop': data['dur'] / data['freq'] if data['freq'] else 0.0,
                'run_hours': hours,
                'freq_per_hour': data['freq'] / hours if hours > 0 else 0.0,
                'time_per_hour': data['dur'] / hours if hours > 0 else 0.0,
                'slow_time': slows.get(base, 0.0),
            })

        if rows:
            rows.sort(key=lambda row: row.get(self.limit_by, 0) or 0, reverse=True)
            if self.record_limit > 0:
                rows = rows[:self.record_limit]
            Line.create(rows)

        measures = [name for name, shown in [
            ('frequency', self.show_frequency), ('freq_per_hour', self.show_freq_per_hour),
            ('total_time', self.show_total_time), ('avg_time_per_stop', self.show_avg_time_per_stop),
            ('time_per_hour', self.show_time_per_hour), ('run_hours', self.show_run_hours),
            ('slow_time', self.show_slow_time),
        ] if shown] or ['frequency']

        return {
            'name': _("Product Analysis"),
            'type': 'ir.actions.act_window',
            'res_model': 'mes.product.analysis.line',
            'view_mode': 'pivot,graph,tree',
            'domain': [('user_id', '=', self.env.user.id)],
            'context': self._build_skd_context(measures),
        }


class MesProductAnalysisLine(models.Model):
    _name = 'mes.product.analysis.line'
    _description = 'Product Analysis Matrix Line'
    _order = 'row_group_label, col_group_label'

    user_id = fields.Many2one('res.users', string='User', index=True)
    period_name = fields.Char(string='Period')
    row_group_label = fields.Char(string='Rows')
    col_group_label = fields.Char(string='Columns')
    frequency = fields.Integer(string='Number Of Stops', group_operator='sum')
    total_time = fields.Float(string='Stop Time (min)', group_operator='sum')
    avg_time_per_stop = fields.Float(string='Avg Stop (min)', group_operator='avg')
    run_hours = fields.Float(string='Run Hours', group_operator='sum')
    freq_per_hour = fields.Float(string='Stops / Run Hour', group_operator='avg')
    time_per_hour = fields.Float(string='Stop Min / Run Hour', group_operator='avg')
    slow_time = fields.Float(string='Slow Time (min)', group_operator='sum')
