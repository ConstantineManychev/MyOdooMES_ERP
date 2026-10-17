from odoo import models, fields, api
from odoo.osv import expression

RAW_FIELDS = ('planned_sec', 'run_sec', 'good_qty', 'ideal_qty', 'waste_qty', 'stop_sec', 'stop_count')
SHOWN_FIELDS = ('oee', 'availability', 'performance', 'downtime_loss', 'waste_loss', 'downtime_share',
                'waste_share', 'planned_hours', 'run_hours', 'stop_minutes')
STRUCTURE_FIELDS = ('stream_id', 'part_id', 'row_kind')


class MesProductOee(models.Model):
    _name = 'mes.product.oee'
    _description = 'OEE By Product, Operator, Stream And Part'
    _order = 'date_start desc, id'

    performance_id = fields.Many2one('mes.machine.performance', string='Shift Report', required=True,
                                     ondelete='cascade', index=True)
    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', index=True)
    shift_id = fields.Many2one('mes.shift', string='Shift')
    shift_date = fields.Date(string='Shift Date', index=True)
    product_id = fields.Many2one('product.product', string='Product', index=True)
    operator_id = fields.Many2one('hr.employee', string='Operator', index=True)
    stream_id = fields.Many2one('mes.stream', string='Stream', index=True)
    part_id = fields.Many2one('mes.part', string='Part', index=True)
    row_kind = fields.Selection([
        ('machine', 'Machine'),
        ('stream', 'Stream'),
        ('part', 'Part'),
    ], string='Row Kind', required=True, index=True)
    date_start = fields.Datetime(string='From')
    date_end = fields.Datetime(string='Until')

    planned_sec = fields.Float(string='Planned Seconds')
    run_sec = fields.Float(string='Run Seconds')
    good_qty = fields.Float(string='Good Quantity')
    ideal_qty = fields.Float(string='Ideal Quantity')
    waste_qty = fields.Float(string='Waste Quantity')
    stop_sec = fields.Float(string='Stop Seconds')
    stop_count = fields.Integer(string='Number Of Stops')

    oee = fields.Float(string='OEE (%)')
    availability = fields.Float(string='Availability (%)')
    performance = fields.Float(string='Performance (%)')
    downtime_loss = fields.Float(string='Downtime Loss (%)')
    waste_loss = fields.Float(string='Waste Loss (%)')
    downtime_share = fields.Float(string='Share Of Downtime (%)')
    waste_share = fields.Float(string='Share Of Waste (%)')
    planned_hours = fields.Float(string='Planned Hours')
    run_hours = fields.Float(string='Run Hours')
    stop_minutes = fields.Float(string='Stop Time (min)')

    @api.model
    def fields_get(self, allfields=None, attributes=None):
        result = super().fields_get(allfields=allfields, attributes=attributes)
        for name in RAW_FIELDS + SHOWN_FIELDS + ('row_kind', 'date_start', 'date_end'):
            if name in result:
                result[name]['groupable'] = False
        return result

    @api.model
    def _leaf_field(self, term):
        if isinstance(term, (list, tuple)) and len(term) == 3:
            return str(term[0]).split('.')[0]
        return None

    @api.model
    def _cell_kind(self, domain):
        used = {self._leaf_field(term) for term in domain}
        if 'part_id' in used:
            return 'part'
        if 'stream_id' in used:
            return 'stream'
        return 'machine'

    @api.model
    def _without_structure(self, domain):
        return [expression.TRUE_LEAF if self._leaf_field(term) in STRUCTURE_FIELDS else term
                for term in domain]

    def _sums(self, domain):
        rows = super().read_group(domain, [f'{name}:sum' for name in RAW_FIELDS], [], lazy=False)
        row = rows[0] if rows else {}
        return {name: row.get(name) or 0.0 for name in RAW_FIELDS}

    @api.model
    def _metrics(self, base, own, kind):
        planned, run = base['planned_sec'], base['run_sec']
        good, ideal = base['good_qty'], base['ideal_qty']
        availability = max(0.0, min(run / planned, 1.0)) if planned > 0 else 0.0
        performance = max(0.0, min(good / ideal, 1.0)) if ideal > 0 else 0.0
        if kind == 'machine':
            waste_loss = max(0.0, 1.0 - good / ideal) if planned > 0 and ideal > 0 else 0.0
            downtime_loss = 1.0 - availability if planned > 0 else 0.0
            waste_share = 1.0 if base['waste_qty'] > 0 else 0.0
            downtime_share = 1.0 if base['stop_sec'] > 0 else 0.0
        else:
            waste_loss = own['waste_qty'] / ideal if ideal > 0 else 0.0
            downtime_loss = own['stop_sec'] / planned if planned > 0 else 0.0
            waste_share = own['waste_qty'] / base['waste_qty'] if base['waste_qty'] > 0 else 0.0
            downtime_share = own['stop_sec'] / base['stop_sec'] if base['stop_sec'] > 0 else 0.0
        return {
            'oee': round(availability * performance * 100, 2),
            'availability': round(availability * 100, 2),
            'performance': round(performance * 100, 2),
            'downtime_loss': round(downtime_loss * 100, 2),
            'waste_loss': round(waste_loss * 100, 2),
            'downtime_share': round(downtime_share * 100, 2),
            'waste_share': round(waste_share * 100, 2),
            'planned_hours': round(planned / 3600.0, 2),
            'run_hours': round(run / 3600.0, 2),
            'stop_minutes': round(own['stop_sec'] / 60.0, 1),
            'planned_sec': planned,
            'run_sec': run,
            'good_qty': good,
            'ideal_qty': ideal,
            'waste_qty': own['waste_qty'],
            'stop_sec': own['stop_sec'],
            'stop_count': int(own['stop_count']),
        }

    @api.model
    def read_group(self, domain, fields, groupby, offset=0, limit=None, orderby=False, lazy=True):
        result = super().read_group(domain, fields, groupby, offset=offset, limit=limit,
                                    orderby=orderby, lazy=lazy)
        requested = {spec.split(':')[0] for spec in fields}
        if not requested & set(RAW_FIELDS + SHOWN_FIELDS):
            return result
        for row in result:
            cell = list(row.get('__domain', domain))
            kind = self._cell_kind(cell)
            base = self._sums(self._without_structure(cell) + [('row_kind', '=', 'machine')])
            own = base if kind == 'machine' else self._sums(cell + [('row_kind', '=', kind)])
            metrics = self._metrics(base, own, kind)
            row.update({name: value for name, value in metrics.items() if name in requested})
        return result


class MesMachinePerformance(models.Model):
    _inherit = 'mes.machine.performance'

    product_oee_computed_at = fields.Datetime(string='Product OEE Computed', readonly=True, copy=False)
    product_oee_error = fields.Char(string='Product OEE Error', readonly=True, copy=False)
