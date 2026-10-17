from odoo import models, fields, api, _
from odoo.exceptions import UserError


class MesProductSetWizard(models.TransientModel):
    _name = 'mes.product.set.wizard'
    _description = 'Set Or Correct The Product Of A Machine'

    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', required=True)
    run_id = fields.Many2one('mes.production.run', string='Run Being Corrected', readonly=True)
    kind = fields.Selection([
        ('plan', 'Plan'),
        ('correction', 'Correction'),
    ], string='Kind', required=True, default='plan')
    product_id = fields.Many2one('product.product', string='Product', required=True)
    date_start = fields.Datetime(string='From', required=True, default=fields.Datetime.now)
    date_end = fields.Datetime(string='Until')
    production_id = fields.Many2one('mrp.production', string='Manufacturing Order')
    note = fields.Char(string='Note')
    current_product_id = fields.Many2one('product.product', string='Product Now Recorded',
                                         readonly=True)

    @api.model
    def default_get(self, field_names):
        values = super().default_get(field_names)
        run = self.env['mes.production.run'].browse(values.get('run_id') or 0).exists()
        if run:
            values.update({
                'workcenter_id': run.workcenter_id.id,
                'kind': 'correction',
                'date_start': run.date_start,
                'date_end': run.date_end or False,
                'current_product_id': run.product_id.id or False,
                'product_id': run.product_id.id or False,
            })
        return values

    @api.onchange('production_id')
    def _onchange_production_id(self):
        for wizard in self:
            if wizard.production_id.product_id:
                wizard.product_id = wizard.production_id.product_id

    def action_apply(self):
        self.ensure_one()
        if self.date_end and self.date_end <= self.date_start:
            raise UserError(_("'Until' must be later than 'From'."))
        values = {
            'workcenter_id': self.workcenter_id.id,
            'product_id': self.product_id.id,
            'date_start': self.date_start,
            'date_end': self.date_end or False,
            'kind': self.kind,
            'production_id': self.production_id.id or False,
            'note': self.note or False,
            'origin': 'backend',
            'employee_id': False,
            'user_id': self.env.uid,
            'changed_at': fields.Datetime.now(),
        }
        Override = self.env['mes.product.override']
        existing = self.run_id.override_id if self.run_id and self.run_id.source == 'manual' else Override
        if existing and existing.date_start == self.date_start:
            existing.write(values)
        else:
            Override.create(values)
        engine = self.env['mes.product.engine']
        engine._rebuild_dirty(self.workcenter_id)
        return {'type': 'ir.actions.client', 'tag': 'reload'}
