from odoo import models, fields, api, _
from odoo.exceptions import ValidationError


class MesProductRule(models.Model):
    _name = 'mes.product.rule'
    _description = 'Signature To Product Rule'
    _order = 'sequence, id'

    product_id = fields.Many2one('product.product', string='Product', required=True,
                                 ondelete='restrict')
    value_ids = fields.Many2many('mes.signature.value', string='When The Machine Shows',
                                 required=True)
    workcenter_ids = fields.Many2many('mrp.workcenter', string='Only On Machines',
                                      help="Leave empty to use the rule on every machine.")
    sequence = fields.Integer(string='Sequence', default=10,
                              help="Among equally specific rules the lower sequence wins.")
    active = fields.Boolean(string='Active', default=True)
    specificity = fields.Integer(string='Specificity', compute='_compute_specificity',
                                 help="Number of values the rule requires. More specific rules win.")

    @api.depends('value_ids')
    def _compute_specificity(self):
        for rule in self:
            rule.specificity = len(rule.value_ids)

    @api.depends('product_id', 'value_ids')
    def _compute_display_name(self):
        for rule in self:
            values = ' + '.join(rule.value_ids.sorted(lambda v: (v.role_id.sequence, v.name)).mapped('name'))
            rule.display_name = f"{values} → {rule.product_id.display_name}" if values \
                else rule.product_id.display_name or ''

    @api.constrains('value_ids')
    def _check_one_value_per_role(self):
        for rule in self:
            roles = rule.value_ids.mapped('role_id')
            if len(roles) != len(rule.value_ids):
                raise ValidationError(_("A rule can use only one value per role."))

    @api.model_create_multi
    def create(self, vals_list):
        rules = super().create(vals_list)
        self.env['mrp.workcenter']._mark_all_product_dirty()
        return rules

    def write(self, vals):
        result = super().write(vals)
        self.env['mrp.workcenter']._mark_all_product_dirty()
        return result

    def unlink(self):
        result = super().unlink()
        self.env['mrp.workcenter']._mark_all_product_dirty()
        return result
