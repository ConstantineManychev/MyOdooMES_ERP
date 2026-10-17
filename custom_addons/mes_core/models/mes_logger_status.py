from odoo import models, fields

from .mes_link_outage import pulse_values


class MesMachineSettings(models.Model):
    _inherit = 'mes.machine.settings'

    log_conn_dt = fields.Datetime()
    log_cfg_req_dt = fields.Datetime()
    log_cfg_ok_dt = fields.Datetime()
    log_bind_req_dt = fields.Datetime()
    log_bind_ok_dt = fields.Datetime()
    log_plc_recv_dt = fields.Datetime()
    log_odoo_send_dt = fields.Datetime()

    log_err_msg = fields.Char()
    log_err_dt = fields.Datetime()

    log_pulse_dt = fields.Datetime(string='Last Logger Pulse', readonly=True)
    log_pulse_since_dt = fields.Datetime(string='Logger Reachable Since', readonly=True)
    log_ads_ok = fields.Boolean(string='PLC Readable', readonly=True)
    log_ads_ok_dt = fields.Datetime(string='PLC Last Read OK', readonly=True)
    log_ads_up_since_dt = fields.Datetime(string='PLC Readable Since', readonly=True)
    log_ads_down_since_dt = fields.Datetime(string='PLC Unreadable Since', readonly=True)
    log_backlog = fields.Integer(string='Records Waiting On Logger', readonly=True)
    log_backlog_from_dt = fields.Datetime(string='Oldest Waiting Record', readonly=True)
    log_clock_skew_sec = fields.Integer(string='Logger Clock Offset (s)', readonly=True,
                                        help="Logger PC clock minus Odoo server clock. Telemetry times come "
                                             "from the logger PC, so data checks are shifted by this offset.")
    log_cut_dt = fields.Datetime(string='Last Counter Jump Removed', readonly=True)
    log_started_dt = fields.Datetime(string='Logger Started', readonly=True)

    def _register_pulse(self, payload):
        self.ensure_one()
        now = fields.Datetime.now()
        self.with_context(tracking_disable=True).write(
            pulse_values(self.log_pulse_dt, self.log_pulse_since_dt, payload, now))
        workcenter = self.env['mrp.workcenter'].sudo().search([('machine_settings_id', '=', self.id)], limit=1)
        return self.env['mes.link.outage']._loss_minutes(workcenter.company_id or self.env.company) * 60


class MrpWorkcenter(models.Model):
    _inherit = 'mrp.workcenter'

    log_conn_dt = fields.Datetime(related='machine_settings_id.log_conn_dt')
    log_cfg_req_dt = fields.Datetime(related='machine_settings_id.log_cfg_req_dt')
    log_cfg_ok_dt = fields.Datetime(related='machine_settings_id.log_cfg_ok_dt')
    log_bind_req_dt = fields.Datetime(related='machine_settings_id.log_bind_req_dt')
    log_bind_ok_dt = fields.Datetime(related='machine_settings_id.log_bind_ok_dt')
    log_plc_recv_dt = fields.Datetime(related='machine_settings_id.log_plc_recv_dt')
    log_odoo_send_dt = fields.Datetime(related='machine_settings_id.log_odoo_send_dt')

    log_err_msg = fields.Char(related='machine_settings_id.log_err_msg')
    log_err_dt = fields.Datetime(related='machine_settings_id.log_err_dt')

    log_pulse_dt = fields.Datetime(related='machine_settings_id.log_pulse_dt')
    log_ads_ok = fields.Boolean(related='machine_settings_id.log_ads_ok')
    log_ads_ok_dt = fields.Datetime(related='machine_settings_id.log_ads_ok_dt')
    log_ads_down_since_dt = fields.Datetime(related='machine_settings_id.log_ads_down_since_dt')
    log_backlog = fields.Integer(related='machine_settings_id.log_backlog')
    log_backlog_from_dt = fields.Datetime(related='machine_settings_id.log_backlog_from_dt')
    log_clock_skew_sec = fields.Integer(related='machine_settings_id.log_clock_skew_sec')
    log_cut_dt = fields.Datetime(related='machine_settings_id.log_cut_dt')
    log_started_dt = fields.Datetime(related='machine_settings_id.log_started_dt')
