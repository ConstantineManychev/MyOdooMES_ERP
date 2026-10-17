from odoo import fields, models, api, _
from odoo.exceptions import UserError
import logging

_logger = logging.getLogger(__name__)

class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    use_gemba_sql = fields.Boolean(
        string="Enable Gemba SQL Sync",
        config_parameter='mes_core.use_gemba_sql'
    )
    
    gemba_sql_server = fields.Char(
        string="SQL Server Host", 
        config_parameter='gemba.sql_server'
    )
    gemba_sql_database = fields.Char(
        string="SQL Database", 
        config_parameter='gemba.sql_database'
    )
    gemba_sql_user = fields.Char(
        string="SQL User", 
        config_parameter='gemba.sql_user'
    )
    gemba_sql_password = fields.Char(
        string="SQL Password", 
        config_parameter='gemba.sql_password'
    )

    use_maintainx = fields.Boolean(
        related='company_id.maintainx_enabled',
        readonly=False,
        string="Enable MaintainX Sync (this company)",
    )

    mes_planned_downtime_event_id = fields.Many2one(
        related='company_id.mes_planned_downtime_event_id',
        readonly=False,
        string="Default Planned Downtime Event",
    )

    mes_unplanned_downtime_color = fields.Char(
        related='company_id.mes_unplanned_downtime_color',
        readonly=False,
        string="Unplanned Downtime Colour",
    )

    maintainx_default_assignee_id = fields.Many2one(
        related='company_id.maintainx_default_assignee_id',
        readonly=False,
        string="Default MaintainX Assignee",
    )

    maintainx_comment_author_id = fields.Many2one(
        related='company_id.maintainx_comment_author_id',
        readonly=False,
        string="Operator Confirmation Comment Author",
    )

    maintainx_webhook_secret = fields.Char(
        related='company_id.maintainx_webhook_secret',
        readonly=False,
        string="MaintainX Webhook Secret",
    )

    maintainx_api_token = fields.Char(
        related='company_id.maintainx_api_token',
        readonly=False,
        string="MaintainX API Token (this company)",
    )

    mes_anomaly_min_minutes = fields.Float(
        related='company_id.mes_anomaly_min_minutes',
        readonly=False,
        string="Anomaly: Minimum Extra Downtime (min, this company)",
    )
    mes_anomaly_min_stops = fields.Float(
        related='company_id.mes_anomaly_min_stops',
        readonly=False,
        string="Anomaly: Minimum Extra Stops (this company)",
    )
    mes_anomaly_min_pieces = fields.Float(
        related='company_id.mes_anomaly_min_pieces',
        readonly=False,
        string="Anomaly: Minimum Extra Waste (pcs, this company)",
    )

    mes_downtime_role_ids = fields.Many2many(
        related='company_id.mes_downtime_role_ids',
        readonly=False,
        string="Planned Downtime Button For (this company)",
    )
    mes_help_role_ids = fields.Many2many(
        related='company_id.mes_help_role_ids',
        readonly=False,
        string="Call For Help Button For (this company)",
    )

    mes_supervisor_confirmation_required = fields.Boolean(
        related='company_id.mes_supervisor_confirmation_required',
        readonly=False,
        string="Require Supervisor Confirmation (this company)",
    )

    mes_link_loss_minutes = fields.Integer(
        related='company_id.mes_link_loss_minutes',
        readonly=False,
        string="Data Gap After (min, this company)",
    )
    mes_link_loss_emails = fields.Char(
        related='company_id.mes_link_loss_emails',
        readonly=False,
        string="Data Gap Emails (this company)",
    )

    mes_dashboard_refresh_interval = fields.Integer(
        string="Dashboard Refresh Interval (sec)",
        config_parameter='mes_core.dashboard_refresh_interval',
        default=60,
        help="How often the dashboard list of machines will be refreshed (in seconds)"
    )

    stock_move_sms_validation = fields.Boolean(
        string="SMS Validation for Stock Moves",
        config_parameter='stock.sms_validation'
    )

    stock_sms_confirmation_template_id = fields.Many2one(
        'sms.template',
        string="SMS Confirmation Template",
        config_parameter='stock.sms_template_id'
    )

    def action_test_sql_connection(self):
        self.ensure_one()
        
        if not self.use_gemba_sql:
            raise UserError(_("Please enable Gemba SQL Sync first."))

        connection_string = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={self.gemba_sql_server};'
            f'DATABASE={self.gemba_sql_database};'
            f'UID={self.gemba_sql_user};'
            f'PWD={self.gemba_sql_password};'
            'TrustServerCertificate=yes;'
        )

        try:
            import pyodbc 
            conn = pyodbc.connect(connection_string, timeout=5)
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()
            conn.close()
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("Connection Successful"),
                    'message': f"Connected to MS SQL Server.\nVersion: {version[0][:50]}...",
                    'type': 'success',
                    'sticky': False,
                }
            }
        except Exception as e:
            raise UserError(f"Connection Failed: {str(e)}")

    def action_test_maintainx_connection(self):
        self.ensure_one()
        return self.company_id.action_test_maintainx_connection()