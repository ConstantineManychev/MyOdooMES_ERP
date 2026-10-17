import hashlib
import json

from odoo import models, fields, api, _
from odoo.exceptions import ValidationError

LOGGER_PARAMS = {
    'health_interval_sec': ('float', 60.0),
    'notif_cycle_time_ms': ('int', 10),
    'notif_max_delay_ms': ('int', 200),
    'notif_mismatch_rebind_threshold': ('int', 2),
    'pipeline_max_batch': ('int', 5000),
    'recovery_ads_restart_enabled': ('bool', True),
    'recovery_fail_window_sec': ('float', 120.0),
    'recovery_ipc_reboot_enabled': ('bool', False),
    'recovery_ipc_reboot_grace_sec': ('int', 60),
    'recovery_ipc_reboot_threshold': ('int', 3),
    'recovery_log_dedup_window_sec': ('float', 30.0),
    'recovery_max_reboots_per_day': ('int', 6),
    'recovery_odoo_alert_cooldown_sec': ('float', 3600.0),
    'recovery_odoo_alert_threshold': ('int', 3),
    'recovery_os_action_cooldown_sec': ('float', 600.0),
    'recovery_reboot_backoff_factor': ('float', 2.0),
    'recovery_reboot_max_interval_sec': ('float', 21600.0),
    'recovery_reboot_min_interval_sec': ('float', 1800.0),
    'recovery_router_restart_threshold': ('int', 2),
    'recovery_runtime_restart_cooldown_sec': ('float', 150.0),
    'recovery_runtime_restart_enabled': ('bool', False),
    'recovery_runtime_restart_threshold': ('int', 12),
    'retn_days': ('int', 30),
    'self_update_enabled': ('bool', True),
    'stall_restart_enabled': ('bool', True),
    'stall_threshold_sec': ('float', 300.0),
    'state_filter_max': ('float', 9.0),
    'tx_poison_threshold': ('int', 10),
    'update_check_interval_sec': ('float', 300.0),
    'watchdog_interval_sec': ('float', 15.0),
    'watchdog_silence_threshold_sec': ('float', 120.0),
    'watchdog_verify_reads': ('bool', True),
}


TRUE_WORDS = ('true', '1', 'yes', 'on')
FALSE_WORDS = ('false', '0', 'no', 'off')


def parse_logger_value(kind, raw):
    text = (raw or '').strip()
    if kind == 'bool':
        lowered = text.lower()
        if lowered in TRUE_WORDS:
            return True
        if lowered in FALSE_WORDS:
            return False
        raise ValueError(text)
    if kind == 'int':
        return int(text)
    if kind == 'float':
        return float(text)
    return text


def config_digest(payload):
    canonical = json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False)
    return hashlib.sha256(canonical.encode('utf-8')).hexdigest()


class MesLoggerConfigLine(models.Model):
    _name = 'mes.logger.config.line'
    _description = 'Logger Configuration Parameter'
    _order = 'key'

    machine_id = fields.Many2one('mes.machine.settings', string='Machine', required=True,
                                 ondelete='cascade', index=True)
    key = fields.Selection([(key, key) for key in sorted(LOGGER_PARAMS)],
                           string='Parameter', required=True)
    value = fields.Char(string='Value', required=True)
    value_type = fields.Char(string='Type', compute='_compute_meta')
    default_value = fields.Char(string='Logger Default', compute='_compute_meta')

    _sql_constraints = [
        ('machine_key_uniq', 'unique(machine_id, key)',
         'This parameter is already set for the machine.'),
    ]

    @api.depends('key')
    def _compute_meta(self):
        for line in self:
            kind, default = LOGGER_PARAMS.get(line.key, ('', None))
            line.value_type = kind
            line.default_value = '' if default is None else str(default).lower() \
                if isinstance(default, bool) else str(default)

    @api.onchange('key')
    def _onchange_key(self):
        for line in self:
            if line.key and not line.value:
                line.value = line.default_value

    @api.constrains('key', 'value')
    def _check_value(self):
        for line in self:
            kind = LOGGER_PARAMS[line.key][0]
            try:
                parse_logger_value(kind, line.value)
            except (TypeError, ValueError):
                raise ValidationError(_(
                    "Value '%(value)s' is not valid for %(key)s, which expects %(kind)s.",
                    value=line.value, key=line.key, kind=kind))

    def typed_value(self):
        self.ensure_one()
        return parse_logger_value(LOGGER_PARAMS[self.key][0], self.value)


class MesMachineSettings(models.Model):
    _inherit = 'mes.machine.settings'

    logger_config_line_ids = fields.One2many('mes.logger.config.line', 'machine_id',
                                             string='Logger Configuration')
    logger_config_hash = fields.Char(string='Applied Config Hash', readonly=True)
    logger_config_state = fields.Selection([
        ('applied', 'Applied'),
        ('pending', 'Waiting for the logger'),
        ('unknown', 'Not reported yet'),
    ], string='Config State', compute='_compute_logger_config_state')

    def _logger_config_payload(self):
        self.ensure_one()
        payload = {line.key: line.typed_value() for line in self.logger_config_line_ids}
        return payload, config_digest(payload)

    def _compute_logger_config_state(self):
        for machine in self:
            if not machine.logger_config_hash:
                machine.logger_config_state = 'unknown'
                continue
            _payload, digest = machine._logger_config_payload()
            machine.logger_config_state = 'applied' if digest == machine.logger_config_hash \
                else 'pending'
