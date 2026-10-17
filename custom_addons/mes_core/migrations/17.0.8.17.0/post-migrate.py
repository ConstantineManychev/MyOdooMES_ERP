from odoo import api, SUPERUSER_ID


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Company = env['res.company']
    technician = Company._default_kiosk_roles('technician')
    operator = Company._default_kiosk_roles('operator')
    for company in Company.with_context(active_test=False).search([]):
        values = {}
        if not company.mes_downtime_role_ids:
            values['mes_downtime_role_ids'] = [(6, 0, technician.ids)]
        if not company.mes_help_role_ids:
            values['mes_help_role_ids'] = [(6, 0, operator.ids)]
        if values:
            company.write(values)
