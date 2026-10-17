from odoo import models, fields, _


class MesSignatureInterval(models.Model):
    _name = 'mes.signature.interval'
    _description = 'Period With An Unchanged Machine Signature'
    _order = 'workcenter_id, date_start desc'

    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', required=True,
                                    ondelete='cascade', index=True)
    date_start = fields.Datetime(string='From', required=True, index=True)
    date_end = fields.Datetime(string='Until', index=True)
    raw_json = fields.Text(string='Raw Values', required=True, default='{}')
    raw_label = fields.Char(string='Machine Reported')


class MesProductionRun(models.Model):
    _name = 'mes.production.run'
    _description = 'Product Run On A Machine'
    _order = 'date_start desc, id desc'

    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', required=True,
                                    ondelete='cascade', index=True)
    product_id = fields.Many2one('product.product', string='Product', index=True)
    date_start = fields.Datetime(string='From', required=True, index=True)
    date_end = fields.Datetime(string='Until', index=True,
                               help="Empty while the run is still going on.")
    source = fields.Selection([
        ('signature', 'Machine signature'),
        ('manual', 'Manual'),
        ('unknown', 'Unknown signature'),
    ], string='Source', required=True)
    override_id = fields.Many2one('mes.product.override', string='Plan Or Correction',
                                  ondelete='set null')
    changed_via = fields.Selection(related='override_id.origin', string='Changed Via')
    changed_by = fields.Char(related='override_id.changed_by', string='Changed By')
    changed_at = fields.Datetime(related='override_id.changed_at', string='Changed At')
    production_id = fields.Many2one('mrp.production', string='Manufacturing Order',
                                    ondelete='set null')
    signature_label = fields.Char(string='Machine Signature')
    unmapped_label = fields.Char(string='Unrecognized Values')
    warning_label = fields.Char(string='Warnings')
    is_current = fields.Boolean(string='Current', compute='_compute_is_current')
    duration_hours = fields.Float(string='Duration (h)', compute='_compute_duration_hours',
                                  digits=(16, 2))

    def _compute_is_current(self):
        for run in self:
            run.is_current = not run.date_end

    def _compute_duration_hours(self):
        now = fields.Datetime.now()
        for run in self:
            end = run.date_end or now
            run.duration_hours = max((end - run.date_start).total_seconds(), 0.0) / 3600.0

    def action_change_product(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': _("Correct Product"),
            'res_model': 'mes.product.set.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_run_id': self.id},
        }
