from odoo import fields, models

class ResConfigSettings(models.TransientModel):
    _inherit = 'res.config.settings'

    # --- Gemba (SQL) Settings ---
    gemba_sql_server = fields.Char(
        string="SQL Server Host", 
        config_parameter='gemba.sql_server', # Имя ключа в базе
        default='AB-AS03'
    )
    gemba_sql_database = fields.Char(
        string="SQL Database", 
        config_parameter='gemba.sql_database', 
        default='Connect'
    )
    gemba_sql_user = fields.Char(
        string="SQL User", 
        config_parameter='gemba.sql_user'
    )
    gemba_sql_password = fields.Char(
        string="SQL Password", 
        config_parameter='gemba.sql_password'
    )

    # --- MaintainX (API) Settings ---
    maintainx_api_token = fields.Char(
        string="MaintainX API Token",
        config_parameter='gemba.maintainx_token'
    )