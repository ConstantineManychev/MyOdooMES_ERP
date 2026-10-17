from odoo import models, fields, api
from odoo.osv import expression


class MesPerformanceProductLine(models.Model):
    _name = 'mes.performance.product.line'
    _description = 'Product Made During A Shift'
    _order = 'performance_id, date_start'

    performance_id = fields.Many2one('mes.machine.performance', string='Shift Report', required=True,
                                     ondelete='cascade', index=True)
    product_id = fields.Many2one('product.product', string='Product', index=True)
    date_start = fields.Datetime(string='From', required=True)
    date_end = fields.Datetime(string='Until', help="Empty while the product is still being made.")
    source = fields.Selection([
        ('signature', 'Machine signature'),
        ('manual', 'Manual'),
        ('unknown', 'Unknown signature'),
    ], string='Source')
    run_id = fields.Many2one('mes.production.run', string='Run', ondelete='set null')
    started_shift = fields.Boolean(string='Running At Shift Start')


class MesMachinePerformance(models.Model):
    _inherit = 'mes.machine.performance'

    product_line_ids = fields.One2many('mes.performance.product.line', 'performance_id',
                                       string='Products')
    product_ids = fields.Many2many('product.product', 'mes_performance_product_rel',
                                   'performance_id', 'product_id', string='Products Made',
                                   compute='_compute_product_ids', store=True)

    @api.depends('product_line_ids.product_id')
    def _compute_product_ids(self):
        for report in self:
            report.product_ids = report.product_line_ids.mapped('product_id')


STOP_RATE_FIELDS = ('avg_stop_minutes', 'stops_per_run_hour', 'stop_minutes_per_run_hour')
HIDDEN_FROM_GROUPING = ('stop_count',) + STOP_RATE_FIELDS
SHARED_WITH_RUNNING = ('workcenter_id', 'product_id', 'operator_id', 'shift_date', 'shift_id',
                       'performance_id', 'start_time', 'end_time')


class MesPerformanceAlarm(models.Model):
    _inherit = 'mes.performance.alarm'

    product_id = fields.Many2one('product.product', string='Product', readonly=True, index=True)
    avg_stop_minutes = fields.Float(string='Avg Stop (min)', readonly=True)
    stops_per_run_hour = fields.Float(string='Stops / Run Hour', readonly=True)
    stop_minutes_per_run_hour = fields.Float(string='Stop Min / Run Hour', readonly=True)

    @api.model
    def fields_get(self, allfields=None, attributes=None):
        result = super().fields_get(allfields=allfields, attributes=attributes)
        for name in HIDDEN_FROM_GROUPING:
            if name in result:
                result[name]['groupable'] = False
        return result

    @api.model
    def _running_domain(self, domain):
        translated = []
        for term in domain:
            if isinstance(term, (list, tuple)) and len(term) == 3:
                field = str(term[0]).split('.')[0]
                translated.append(tuple(term) if field in SHARED_WITH_RUNNING else expression.TRUE_LEAF)
            else:
                translated.append(term)
        return translated

    @api.model
    def read_group(self, domain, fields, groupby, offset=0, limit=None, orderby=False, lazy=True):
        result = super().read_group(domain, fields, groupby, offset=offset, limit=limit,
                                    orderby=orderby, lazy=lazy)
        requested = {spec.split(':')[0] for spec in fields}
        if not requested & set(STOP_RATE_FIELDS):
            return result
        Running = self.env['mes.performance.running']
        for row in result:
            group_domain = row.get('__domain', domain)
            totals = super().read_group(group_domain, ['stop_count:sum', 'duration:sum'], [], lazy=False)
            stops = (totals[0].get('stop_count') or 0) if totals else 0
            minutes = (totals[0].get('duration') or 0.0) if totals else 0.0
            running = Running.read_group(self._running_domain(group_domain), ['duration:sum'], [], lazy=False)
            hours = ((running[0].get('duration') or 0.0) if running else 0.0) / 60.0
            row['avg_stop_minutes'] = minutes / stops if stops else 0.0
            row['stops_per_run_hour'] = stops / hours if hours else 0.0
            row['stop_minutes_per_run_hour'] = minutes / hours if hours else 0.0
        return result
    stop_count = fields.Integer(string='Number Of Stops', default=1, readonly=True, group_operator='sum')
    operator_id = fields.Many2one('hr.employee', string='Operator', readonly=True, index=True)
    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', readonly=True, index=True)
    shift_date = fields.Date(string='Shift Date', readonly=True, index=True)
    shift_id = fields.Many2one('mes.shift', string='Shift', readonly=True)


class MesPerformanceRunning(models.Model):
    _inherit = 'mes.performance.running'

    product_id = fields.Many2one('product.product', string='Product', readonly=True, index=True)
    operator_id = fields.Many2one('hr.employee', string='Operator', readonly=True, index=True)
    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', readonly=True, index=True)
    shift_date = fields.Date(string='Shift Date', readonly=True, index=True)
    shift_id = fields.Many2one('mes.shift', string='Shift', readonly=True)


class MesPerformanceSlowing(models.Model):
    _inherit = 'mes.performance.slowing'

    product_id = fields.Many2one('product.product', string='Product', readonly=True, index=True)
    operator_id = fields.Many2one('hr.employee', string='Operator', readonly=True, index=True)
    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', readonly=True, index=True)
    shift_date = fields.Date(string='Shift Date', readonly=True, index=True)
    shift_id = fields.Many2one('mes.shift', string='Shift', readonly=True)


class MesTask(models.Model):
    _inherit = 'mes.task'

    product_id = fields.Many2one('product.product', string='Product', readonly=True, index=True)


class MesPendingComment(models.Model):
    _inherit = 'mes.pending.comment'

    product_id = fields.Many2one('product.product', string='Product', readonly=True, index=True)
