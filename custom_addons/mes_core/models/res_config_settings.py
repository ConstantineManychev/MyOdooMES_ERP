from odoo import fields, models

class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    gemba_sql_server = fields.Char(
        string="SQL Server Host", 
        config_parameter='gemba.sql_server',
        default='ServerName'
    )
    gemba_sql_database = fields.Char(
        string="SQL Database", 
        config_parameter='gemba.sql_database', 
        default='DBName'
    )
    gemba_sql_user = fields.Char(
        string="SQL User", 
        config_parameter='gemba.sql_user'
    )
    gemba_sql_password = fields.Char(
        string="SQL Password", 
        config_parameter='gemba.sql_password'
    )

    maintainx_api_token = fields.Char(
        string="MaintainX API Token",
        config_parameter='gemba.maintainx_token'
    )

    # --- Stock SMS. Error fix ---
    stock_move_sms_validation = fields.Boolean(
        string="SMS Validation for Stock Moves"
    )

    stock_sms_confirmation_template_id = fields.Many2one(
            'mail.template',
            string="SMS Confirmation Template"
        )