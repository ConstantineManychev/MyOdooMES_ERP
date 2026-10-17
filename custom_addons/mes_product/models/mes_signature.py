from odoo import models, fields, api, _
from odoo.exceptions import ValidationError


class MesSignatureRole(models.Model):
    _name = 'mes.signature.role'
    _description = 'Product Signature Role'
    _order = 'sequence, id'

    name = fields.Char(string='Role', required=True, translate=True)
    code = fields.Char(string='Code', required=True)
    sequence = fields.Integer(string='Sequence', default=10)
    active = fields.Boolean(string='Active', default=True)
    value_ids = fields.One2many('mes.signature.value', 'role_id', string='Values')

    _sql_constraints = [
        ('code_uniq', 'unique(code)', 'A signature role with this code already exists.'),
    ]


class MesSignatureValue(models.Model):
    _name = 'mes.signature.value'
    _description = 'Normalized Signature Value'
    _order = 'role_id, sequence, name'

    name = fields.Char(string='Value', required=True)
    role_id = fields.Many2one('mes.signature.role', string='Role', required=True,
                              ondelete='cascade', index=True)
    sequence = fields.Integer(string='Sequence', default=10)
    active = fields.Boolean(string='Active', default=True)
    attribute_value_id = fields.Many2one(
        'product.attribute.value', string='Product Attribute Value',
        help="The product variant attribute value this machine value stands for, for example "
             "paper program Raw Black Cones stands for Variant: Black.")

    _sql_constraints = [
        ('role_name_uniq', 'unique(role_id, name)', 'This value already exists for the role.'),
    ]

    @api.depends('name', 'role_id.name')
    def _compute_display_name(self):
        for value in self:
            value.display_name = f"{value.role_id.name}: {value.name}" if value.role_id else value.name

    def write(self, vals):
        result = super().write(vals)
        self.env['mrp.workcenter']._mark_all_product_dirty()
        return result

    def unlink(self):
        result = super().unlink()
        self.env['mrp.workcenter']._mark_all_product_dirty()
        return result


class MesSignatureAlias(models.Model):
    _name = 'mes.signature.alias'
    _description = 'Raw Value Reported By A Machine'
    _order = 'is_mapped, last_seen desc, id desc'

    raw_text = fields.Char(string='Raw Value', required=True)
    role_id = fields.Many2one('mes.signature.role', string='Role', required=True,
                              ondelete='cascade', index=True)
    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', index=True,
                                    help="Leave empty to apply the mapping to every machine.")
    value_id = fields.Many2one('mes.signature.value', string='Means',
                               domain="[('role_id', '=', role_id)]")
    is_mapped = fields.Boolean(string='Mapped', compute='_compute_is_mapped', store=True)
    first_seen = fields.Datetime(string='First Seen', readonly=True)
    last_seen = fields.Datetime(string='Last Seen', readonly=True)

    _sql_constraints = [
        ('alias_uniq', 'unique(workcenter_id, role_id, raw_text)',
         'This raw value is already listed for the role and machine.'),
    ]

    @api.depends('value_id')
    def _compute_is_mapped(self):
        for alias in self:
            alias.is_mapped = bool(alias.value_id)

    @api.constrains('value_id', 'role_id')
    def _check_value_role(self):
        for alias in self:
            if alias.value_id and alias.value_id.role_id != alias.role_id:
                raise ValidationError(_(
                    "'%(value)s' belongs to role %(value_role)s, not %(role)s.",
                    value=alias.value_id.name, value_role=alias.value_id.role_id.name,
                    role=alias.role_id.name))

    @api.model_create_multi
    def create(self, vals_list):
        aliases = super().create(vals_list)
        if any(vals.get('value_id') for vals in vals_list):
            self.env['mrp.workcenter']._mark_all_product_dirty()
        return aliases

    def write(self, vals):
        result = super().write(vals)
        if {'value_id', 'raw_text', 'role_id', 'workcenter_id'} & set(vals):
            self.env['mrp.workcenter']._mark_all_product_dirty()
        return result

    def unlink(self):
        mapped = any(alias.value_id for alias in self)
        result = super().unlink()
        if mapped:
            self.env['mrp.workcenter']._mark_all_product_dirty()
        return result


class MesSignatureConflict(models.Model):
    _name = 'mes.signature.conflict'
    _description = 'Signature Values That Must Not Appear Together'
    _order = 'id'

    value_a_id = fields.Many2one('mes.signature.value', string='Value', required=True, ondelete='cascade')
    value_b_id = fields.Many2one('mes.signature.value', string='Must Not Appear With', required=True,
                                 ondelete='cascade')
    note = fields.Char(string='Likely Cause',
                       help="Shown on the run, for example: pack program not switched.")
    active = fields.Boolean(string='Active', default=True)

    @api.constrains('value_a_id', 'value_b_id')
    def _check_values(self):
        for conflict in self:
            if conflict.value_a_id == conflict.value_b_id:
                raise ValidationError(_("A value cannot conflict with itself."))

    @api.model_create_multi
    def create(self, vals_list):
        conflicts = super().create(vals_list)
        self.env['mrp.workcenter']._mark_all_product_dirty()
        return conflicts

    def write(self, vals):
        result = super().write(vals)
        self.env['mrp.workcenter']._mark_all_product_dirty()
        return result

    def unlink(self):
        result = super().unlink()
        self.env['mrp.workcenter']._mark_all_product_dirty()
        return result
