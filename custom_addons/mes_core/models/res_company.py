from odoo import fields, models, api, _
from odoo.exceptions import UserError


class ResCompany(models.Model):
    _inherit = 'res.company'

    maintainx_enabled = fields.Boolean(string="MaintainX Sync Enabled")
    maintainx_api_token = fields.Char(string="MaintainX API Token")
    maintainx_webhook_secret = fields.Char(
        string="MaintainX Webhook Secret",
        help="Signing secret shown by MaintainX when the webhook subscription is created. "
             "Required to accept status-change callbacks.")
    maintainx_default_assignee_id = fields.Many2one(
        'hr.employee',
        string="Default MaintainX Assignee",
        help="Employee assigned to a work order created from the kiosk until a "
             "technician takes it over. Requires the employee to have a MaintainX ID "
             "or a work email matching their MaintainX account.")
    maintainx_comment_author_id = fields.Many2one(
        'hr.employee',
        string="Operator Confirmation Comment Author",
        help="Employee named as the author of the operator confirmation comment "
             "pushed to MaintainX.")
    mes_planned_downtime_event_id = fields.Many2one(
        'mes.event',
        string="Default Planned Downtime Event",
        help="Applied to every new planned downtime rule and to existing rules "
             "that have no event set.")
    mes_unplanned_downtime_color = fields.Char(
        string="Unplanned Downtime Colour",
        default='#f59e0b',
        help="Timeline colour used while a technician is working on a help request.")

    mes_link_loss_minutes = fields.Integer(
        string="Data Gap After (min)",
        default=10,
        help="A machine whose logger does not report, or whose logger cannot read the PLC, for this "
             "many minutes is recorded as a data gap. Minimum 3.")
    mes_link_loss_emails = fields.Char(
        string="Data Gap Emails",
        help="Addresses that receive an email naming what failed (logger or PLC) when a data gap "
             "starts, and another when data comes in again. Separate them with commas. Leave empty to "
             "send nothing.")

    mes_anomaly_min_minutes = fields.Float(
        string="Anomaly: Minimum Extra Downtime (min)",
        default=5.0,
        help="A stop reason is flagged only if its downtime this shift exceeds the historical average by "
             "at least this many minutes, or its stops by the number below.")
    mes_anomaly_min_stops = fields.Float(
        string="Anomaly: Minimum Extra Stops",
        default=5.0,
        help="A stop reason is flagged only if it stopped the machine at least this many times more than "
             "usual this shift, or its downtime exceeds the minutes above.")
    mes_anomaly_min_pieces = fields.Float(
        string="Anomaly: Minimum Extra Waste (pcs)",
        default=100.0,
        help="A waste counter is flagged only if it counted at least this many pieces more than usual "
             "this shift.")

    mes_downtime_role_ids = fields.Many2many(
        'mes.kiosk.role', 'res_company_mes_downtime_role_rel', 'company_id', 'role_id',
        string="Planned Downtime Button For",
        default=lambda self: self._default_kiosk_roles('technician'),
        help="Kiosk roles that see and can use the Start/Finish planned downtime button. "
             "Leave empty to hide the button for everyone.")
    mes_help_role_ids = fields.Many2many(
        'mes.kiosk.role', 'res_company_mes_help_role_rel', 'company_id', 'role_id',
        string="Call For Help Button For",
        default=lambda self: self._default_kiosk_roles('operator'),
        help="Kiosk roles that see and can use the Call for Help button. "
             "Leave empty to hide the button for everyone.")

    mes_supervisor_confirmation_required = fields.Boolean(
        string="Supervisor Confirmation Required",
        default=False,
        help="When enabled, a technician-closed help request waits for supervisor "
             "confirmation before the operator is asked to confirm completion. "
             "When disabled, the request goes straight to the operator.")

    def action_test_maintainx_connection(self):
        self.ensure_one()
        if not self.maintainx_enabled or not self.maintainx_api_token:
            raise UserError(_("Enable MaintainX and set API Token first."))

        import requests
        try:
            response = requests.get(
                "https://api.getmaintainx.com/v1/workorders",
                headers={
                    'Authorization': f'Bearer {self.maintainx_api_token}',
                    'Content-Type': 'application/json',
                },
                params={'limit': 1},
                timeout=10,
            )
            response.raise_for_status()
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("MaintainX Connection OK"),
                    'message': _("Token works for %s.") % self.name,
                    'type': 'success',
                    'sticky': False,
                },
            }
        except Exception as e:
            raise UserError(_("MaintainX Connection Failed for %s: %s") % (self.name, str(e)))

    @api.model
    def _default_kiosk_roles(self, code):
        return self.env['mes.kiosk.role'].search([('code', '=', code)])

    def _kiosk_role_codes(self, feature):
        self.ensure_one()
        roles = self.mes_downtime_role_ids if feature == 'planned_downtime' else self.mes_help_role_ids
        return roles.mapped('code')
