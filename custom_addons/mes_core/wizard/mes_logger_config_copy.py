from odoo import models, fields, api, _
from odoo.exceptions import UserError

ADMIN_GROUP = 'mes_core.group_mes_administrator'


class MesLoggerConfigCopyWizard(models.TransientModel):
    _name = 'mes.logger.config.copy.wizard'
    _description = 'Copy Logger Configuration To Other Machines'

    source_machine_id = fields.Many2one('mes.machine.settings', string='Copy From',
                                        required=True, readonly=True)
    target_machine_ids = fields.Many2many('mes.machine.settings', string='Copy To')
    mode = fields.Selection([
        ('merge', 'Merge: add and overwrite, keep other parameters'),
        ('replace', 'Replace: targets get exactly this list'),
    ], string='Mode', default='merge', required=True)
    parameter_count = fields.Integer(string='Parameters To Copy',
                                     compute='_compute_parameter_count')

    @api.depends('source_machine_id')
    def _compute_parameter_count(self):
        for wizard in self:
            wizard.parameter_count = len(wizard.source_machine_id.logger_config_line_ids)

    def action_apply(self):
        self.ensure_one()
        if not self.env.user.has_group(ADMIN_GROUP):
            raise UserError(_("Only MES administrators can change logger configuration."))
        targets = self.target_machine_ids - self.source_machine_id
        if not targets:
            raise UserError(_("Select at least one other machine."))

        source = {line.key: line.value for line in self.source_machine_id.logger_config_line_ids}
        Line = self.env['mes.logger.config.line']
        stats = {'added': 0, 'updated': 0, 'removed': 0}

        for machine in targets:
            existing = {line.key: line for line in machine.logger_config_line_ids}
            for key, value in source.items():
                line = existing.get(key)
                if not line:
                    Line.create({'machine_id': machine.id, 'key': key, 'value': value})
                    stats['added'] += 1
                elif line.value != value:
                    line.value = value
                    stats['updated'] += 1
            if self.mode == 'replace':
                extra = machine.logger_config_line_ids.filtered(lambda l: l.key not in source)
                stats['removed'] += len(extra)
                extra.unlink()

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'type': 'success',
                'title': _("Logger configuration copied"),
                'message': _(
                    "%(machines)s machine(s): added %(added)s, updated %(updated)s, "
                    "removed %(removed)s. Loggers apply it at their next check.",
                    machines=len(targets), **stats),
                'next': {'type': 'ir.actions.act_window_close'},
            },
        }


class MesMachineSettings(models.Model):
    _inherit = 'mes.machine.settings'

    def action_copy_logger_config(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': _("Copy Logger Config"),
            'res_model': 'mes.logger.config.copy.wizard',
            'view_mode': 'form',
            'target': 'new',
            'context': {'default_source_machine_id': self.id},
        }
