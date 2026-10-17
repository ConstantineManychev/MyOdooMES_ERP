from datetime import datetime

from odoo import models, fields, api, _
from odoo.exceptions import ValidationError

HISTORY_START = datetime(2000, 1, 1)


class MesWorkcenterSignatureLine(models.Model):
    _name = 'mes.workcenter.signature.line'
    _description = 'Process Carrying A Signature Role'
    _order = 'workcenter_id, role_id'

    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', required=True,
                                    ondelete='cascade', index=True)
    role_id = fields.Many2one('mes.signature.role', string='Role', required=True)
    signal_id = fields.Many2one('mes.signal.process', string='Process Signal', required=True,
                                ondelete='cascade',
                                help="A text process, or a number used as a code such as a program or "
                                     "format number. A measured number like speed or temperature is not a "
                                     "signature: every reading would become a new raw value.")
    tag_name = fields.Char(related='signal_id.tag_name', string='Tag')
    process_id = fields.Many2one(related='signal_id.process_id', string='Process')
    active = fields.Boolean(string='Active', default=True)

    _sql_constraints = [
        ('workcenter_role_uniq', 'unique(workcenter_id, role_id)',
         'Each role can be read from only one process per machine.'),
    ]

    @api.constrains('signal_id', 'workcenter_id')
    def _check_signal_machine(self):
        for line in self:
            machine = line.workcenter_id.machine_settings_id
            if machine and line.signal_id.machine_id != machine:
                raise ValidationError(_(
                    "The process signal must belong to this machine's telemetry settings."))

    @api.model_create_multi
    def create(self, vals_list):
        lines = super().create(vals_list)
        lines.mapped('workcenter_id')._signature_setup_changed()
        return lines

    def write(self, vals):
        result = super().write(vals)
        self.mapped('workcenter_id')._signature_setup_changed()
        return result

    def unlink(self):
        workcenters = self.mapped('workcenter_id')
        result = super().unlink()
        workcenters._signature_setup_changed()
        return result


class MesWorkcenterProductConfig(models.Model):
    _name = 'mes.workcenter.product.config'
    _description = 'Machine Configuration Valid From A Date'
    _order = 'workcenter_id, date_from desc'

    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', required=True,
                                    ondelete='cascade', index=True)
    date_from = fields.Datetime(string='Valid From', required=True)
    date_to = fields.Datetime(string='Valid Until', compute='_compute_date_to')
    value_ids = fields.Many2many('mes.signature.value', string='Fixed Signature Values',
                                 help="Signature values that do not come from the machine; used only "
                                      "by explicit product rules.")
    product_tmpl_id = fields.Many2one('product.template', string='Machine Output Product',
                                      help="Product whose variants this machine produces.")
    attribute_value_ids = fields.Many2many(
        'product.attribute.value', string='Fixed Product Attributes',
        help="Attribute values set by the machine itself rather than by its programs: size, box or "
             "tray, and the edition used when nothing else is planned.")
    note = fields.Char(string='Note')

    _sql_constraints = [
        ('workcenter_date_uniq', 'unique(workcenter_id, date_from)',
         'A configuration already starts at this moment on this machine.'),
    ]

    def _compute_date_to(self):
        for config in self:
            following = self.search([
                ('workcenter_id', '=', config.workcenter_id.id),
                ('date_from', '>', config.date_from),
            ], order='date_from', limit=1)
            config.date_to = following.date_from or False

    @api.constrains('value_ids')
    def _check_one_value_per_role(self):
        for config in self:
            if len(config.value_ids.mapped('role_id')) != len(config.value_ids):
                raise ValidationError(_("A configuration can hold only one value per role."))

    @api.constrains('attribute_value_ids', 'product_tmpl_id')
    def _check_attribute_values(self):
        for config in self:
            attributes = config.attribute_value_ids.mapped('attribute_id')
            if len(attributes) != len(config.attribute_value_ids):
                raise ValidationError(_("A configuration can hold only one value per product attribute."))
            if config.product_tmpl_id:
                allowed = config.product_tmpl_id.attribute_line_ids.mapped('attribute_id')
                foreign = attributes - allowed
                if foreign:
                    raise ValidationError(_(
                        "%(product)s has no attribute %(attributes)s.",
                        product=config.product_tmpl_id.display_name,
                        attributes=', '.join(foreign.mapped('name'))))

    @api.model_create_multi
    def create(self, vals_list):
        configs = super().create(vals_list)
        for config in configs:
            config.workcenter_id._mark_product_dirty(config.date_from)
        return configs

    def write(self, vals):
        previous = {config.id: config.date_from for config in self}
        result = super().write(vals)
        for config in self:
            config.workcenter_id._mark_product_dirty(min(previous[config.id], config.date_from))
        return result

    def unlink(self):
        marks = [(config.workcenter_id, config.date_from) for config in self]
        result = super().unlink()
        for workcenter, moment in marks:
            workcenter._mark_product_dirty(moment)
        return result


class MrpWorkcenter(models.Model):
    _inherit = 'mrp.workcenter'

    signature_line_ids = fields.One2many('mes.workcenter.signature.line', 'workcenter_id',
                                         string='Signature Processes')
    product_config_ids = fields.One2many('mes.workcenter.product.config', 'workcenter_id',
                                         string='Configuration History')
    signature_debounce_min = fields.Integer(
        string='Signature Stable For (min)', default=3,
        help="A new combination of programs becomes a product change only after it stays unchanged "
             "this long, so clicking through programs during a changeover does not create "
             "short runs.")
    product_run_ids = fields.One2many('mes.production.run', 'workcenter_id', string='Product Runs')
    signature_interval_ids = fields.One2many('mes.signature.interval', 'workcenter_id',
                                             string='Signature History')
    product_override_ids = fields.One2many('mes.product.override', 'workcenter_id',
                                           string='Plans And Corrections')
    current_product_run_id = fields.Many2one('mes.production.run', string='Current Run',
                                             compute='_compute_current_product')
    current_product_id = fields.Many2one('product.product', string='Current Product',
                                         compute='_compute_current_product')
    product_dirty_from = fields.Datetime(string='Product History Stale From', readonly=True,
                                         copy=False)
    signature_scanned_to = fields.Datetime(string='Signatures Scanned Up To', readonly=True,
                                           copy=False)
    product_propagate_from = fields.Datetime(string='Product Marking Pending From', readonly=True,
                                             copy=False)
    signature_full_rescan = fields.Boolean(string='Full Signature Rescan Requested', readonly=True,
                                           copy=False)

    def _kiosk_header_badges(self):
        badges = super()._kiosk_header_badges()
        run = self.env['mes.production.run'].sudo().search([
            ('workcenter_id', '=', self.id),
            ('date_end', '=', False),
        ], order='date_start desc', limit=1)
        if not run:
            return badges
        product = run.product_id
        if product:
            badges.append({
                'key': 'product',
                'title': _("Product"),
                'name': product.product_tmpl_id.name,
                'details': product.product_template_attribute_value_ids.mapped('name'),
                'tone': 'manual' if run.source == 'manual' else 'normal',
                'note': self._kiosk_change_note(run.override_id) if run.source == 'manual' else '',
            })
        else:
            badges.append({
                'key': 'product',
                'title': _("Product"),
                'name': _("Not recognized"),
                'details': [],
                'tone': 'warning',
                'note': run.unmapped_label or '',
            })
        return badges

    def _kiosk_change_note(self, override):
        if not override:
            return ''
        origin = dict(override._fields['origin']._description_selection(self.env)).get(override.origin, '')
        moment = override.changed_at or override.create_date
        stamp = self.env['mes.product.engine']._to_wall(self, moment).strftime('%d/%m %H:%M') if moment else ''
        return ' · '.join(part for part in (origin, override.changed_by, stamp) if part)

    def _compute_current_product(self):
        Run = self.env['mes.production.run'].sudo()
        for workcenter in self:
            run = Run.search([
                ('workcenter_id', '=', workcenter.id),
                ('date_end', '=', False),
            ], order='date_start desc', limit=1) if workcenter.id else Run
            workcenter.current_product_run_id = run
            workcenter.current_product_id = run.product_id

    def _mark_product_dirty(self, moment):
        if not moment:
            return
        for workcenter in self.sudo():
            if not workcenter.product_dirty_from or moment < workcenter.product_dirty_from:
                workcenter.product_dirty_from = moment

    def _mark_product_propagate(self, moment):
        if not moment:
            return
        for workcenter in self.sudo():
            if not workcenter.product_propagate_from or moment < workcenter.product_propagate_from:
                workcenter.product_propagate_from = moment

    def _signature_setup_changed(self):
        self.sudo().write({'signature_scanned_to': False})
        self._mark_product_dirty(HISTORY_START)

    @api.model
    def _mark_all_product_dirty(self):
        workcenters = self.sudo().search([('signature_line_ids', '!=', False)])
        workcenters |= self.env['mes.product.override'].sudo().search([]).mapped('workcenter_id')
        workcenters._mark_product_dirty(HISTORY_START)

    def action_recompute_product_history(self):
        self.sudo().write({'signature_full_rescan': True})
        self._mark_product_dirty(HISTORY_START)
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'type': 'info',
                'title': _("Product history"),
                'message': _("The product history will be rebuilt in the background within a few minutes."),
                'next': {'type': 'ir.actions.client', 'tag': 'reload'},
            },
        }

    def action_set_product(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': _("Set Product"),
            'res_model': 'mes.product.set.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_workcenter_id': self.id, 'default_kind': 'plan'},
        }
