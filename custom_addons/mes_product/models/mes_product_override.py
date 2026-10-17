from odoo import models, fields, api, _
from odoo.exceptions import ValidationError

TRACKED_FIELDS = ('workcenter_id', 'product_id', 'date_start', 'date_end', 'kind', 'production_id')


class MesProductOverride(models.Model):
    _name = 'mes.product.override'
    _description = 'Product Plan Or Correction'
    _order = 'date_start desc, id desc'

    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', required=True,
                                    ondelete='cascade', index=True)
    product_id = fields.Many2one('product.product', string='Product', required=True,
                                 ondelete='restrict')
    date_start = fields.Datetime(string='From', required=True, index=True,
                                 default=fields.Datetime.now)
    date_end = fields.Datetime(string='Until',
                               help="Leave empty: the entry lasts until the next manual entry on this "
                                    "machine or until the machine changes its signature, whichever "
                                    "comes first.")
    kind = fields.Selection([
        ('plan', 'Plan'),
        ('correction', 'Correction'),
    ], string='Kind', required=True, default='plan')
    production_id = fields.Many2one('mrp.production', string='Manufacturing Order',
                                    ondelete='set null')
    note = fields.Char(string='Note')
    user_id = fields.Many2one('res.users', string='Entered By', default=lambda self: self.env.user,
                              readonly=True)
    origin = fields.Selection([
        ('backend', 'Back Office'),
        ('kiosk', 'Kiosk'),
    ], string='Changed Via', default='backend', required=True, readonly=True)
    employee_id = fields.Many2one('hr.employee', string='Changed By Employee', readonly=True,
                                  ondelete='set null')
    changed_at = fields.Datetime(string='Changed At', default=fields.Datetime.now, readonly=True)
    changed_by = fields.Char(string='Changed By', compute='_compute_changed_by')

    def _compute_changed_by(self):
        for entry in self:
            entry.changed_by = entry.employee_id.name or entry.user_id.name or ''

    @api.constrains('date_start', 'date_end')
    def _check_dates(self):
        for entry in self:
            if entry.date_end and entry.date_end <= entry.date_start:
                raise ValidationError(_("'Until' must be later than 'From'."))

    @api.onchange('production_id')
    def _onchange_production_id(self):
        for entry in self:
            if entry.production_id and entry.production_id.product_id:
                entry.product_id = entry.production_id.product_id

    def _mark_dirty(self, extra_moments=()):
        by_wc = {}
        for entry in self:
            moments = [entry.date_start] + [m for m in extra_moments if m]
            by_wc.setdefault(entry.workcenter_id, []).extend(moments)
        for workcenter, moments in by_wc.items():
            workcenter._mark_product_dirty(min(moments))

    @api.model_create_multi
    def create(self, vals_list):
        entries = super().create(vals_list)
        entries._mark_dirty()
        return entries

    def write(self, vals):
        if 'origin' not in vals and any(name in vals for name in TRACKED_FIELDS):
            vals = dict(vals, origin='backend', employee_id=False, user_id=self.env.uid,
                        changed_at=fields.Datetime.now())
        previous = {entry.id: (entry.workcenter_id, entry.date_start) for entry in self}
        result = super().write(vals)
        for entry in self:
            old_wc, old_start = previous[entry.id]
            old_wc._mark_product_dirty(old_start)
            entry.workcenter_id._mark_product_dirty(min(old_start, entry.date_start))
        return result

    def unlink(self):
        marks = [(entry.workcenter_id, entry.date_start) for entry in self]
        result = super().unlink()
        for workcenter, moment in marks:
            workcenter._mark_product_dirty(moment)
        return result
