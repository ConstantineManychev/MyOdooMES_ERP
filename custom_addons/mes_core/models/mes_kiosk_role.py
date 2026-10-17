from odoo import models, fields

KIOSK_ROLE_ORDER = ('operator', 'supervisor', 'technician')


class MesKioskRole(models.Model):
    _name = 'mes.kiosk.role'
    _description = 'Kiosk Role'
    _order = 'sequence, id'

    name = fields.Char(required=True, translate=True)
    code = fields.Char(required=True, index=True)
    sequence = fields.Integer(default=10)

    _sql_constraints = [
        ('code_uniq', 'unique(code)', 'Each kiosk role code must be unique.'),
    ]
