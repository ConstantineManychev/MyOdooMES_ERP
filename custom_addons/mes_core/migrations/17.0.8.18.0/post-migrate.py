from odoo import api, SUPERUSER_ID


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    env['mes.event'].with_context(active_test=False).search([
        ('name', '=ilike', 'tech work'),
    ]).write({'exclude_from_anomalies': True})
