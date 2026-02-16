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
        string="Enable MaintainX Sync",
        config_parameter='mes_core.use_maintainx'
    )
    
    maintainx_api_token = fields.Char(
        string="MaintainX API Token",
        config_parameter='gemba.maintainx_token'
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
        if not self.use_maintainx or not self.maintainx_api_token:
            raise UserError(_("Please enable MaintainX Sync and provide an API Token first."))
        
        try:
            import requests
            headers = {
                'Authorization': f'Bearer {self.maintainx_api_token}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(
                "https://api.getmaintainx.com/v1/workorders", 
                headers=headers, 
                params={'limit': 1},
                timeout=10
            )
            response.raise_for_status()
            
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _("MaintainX Connection Successful"),
                    'message': "Successfully connected to MaintainX API.",
                    'type': 'success',
                    'sticky': False,
                }
            }
        except Exception as e:
            raise UserError(f"MaintainX Connection Failed: {str(e)}")