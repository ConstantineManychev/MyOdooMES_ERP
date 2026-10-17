from odoo import api, fields, models
from odoo.exceptions import ValidationError


class HrEmployee(models.Model):
    _inherit = 'hr.employee'

    mes_fob_uid = fields.Char(string='Kiosk FOB UID', copy=False, index=True)
    mes_fob_uid_normalized = fields.Char(
        string='Kiosk FOB UID (normalized)',
        compute='_compute_mes_fob_uid_normalized',
        store=True, index=True, copy=False,
        help="Separator-free uppercase form of the FOB UID, so that a keyboard-wedge "
             "reader and an NFC phone reporting the same tag both resolve to this employee.")
    mes_pin = fields.Char(string='Kiosk PIN', copy=False)
    mes_kiosk_role = fields.Selection([
        ('operator', 'Operator'),
        ('supervisor', 'Supervisor'),
        ('technician', 'Technician'),
    ], string='Kiosk Role')

    _sql_constraints = [
        ('mes_fob_uid_uniq', 'unique(mes_fob_uid)', 'Kiosk FOB UID must be unique.'),
    ]

    @api.model
    def _normalize_fob_uid(self, raw_uid):
        if not raw_uid:
            return False
        cleaned = ''.join(char for char in str(raw_uid) if char.isalnum())
        return cleaned.upper() or False

    @api.depends('mes_fob_uid')
    def _compute_mes_fob_uid_normalized(self):
        for rec in self:
            rec.mes_fob_uid_normalized = self._normalize_fob_uid(rec.mes_fob_uid)

    def init(self):
        self.env.cr.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS hr_employee_mes_pin_uniq
            ON hr_employee (mes_pin)
            WHERE mes_pin IS NOT NULL AND mes_pin <> ''
        """)

    @api.constrains('mes_fob_uid')
    def _check_mes_fob_uid_unique(self):
        for rec in self:
            normalized = self._normalize_fob_uid(rec.mes_fob_uid)
            if not normalized:
                continue
            duplicate = self.sudo().search([
                ('mes_fob_uid_normalized', '=', normalized),
                ('id', '!=', rec.id),
            ], limit=1)
            if duplicate:
                raise ValidationError(
                    f"Kiosk FOB UID must be unique. Already used by: {duplicate.name}"
                )

    @api.constrains('mes_pin')
    def _check_mes_pin_unique(self):
        for rec in self:
            if not rec.mes_pin:
                continue
            duplicate = self.sudo().search([
                ('mes_pin', '=', rec.mes_pin),
                ('id', '!=', rec.id),
            ], limit=1)
            if duplicate:
                raise ValidationError(
                    f"Kiosk PIN must be unique. Already used by: {duplicate.name}"
                )
