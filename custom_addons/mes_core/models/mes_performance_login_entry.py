from odoo import fields, models, api


class MesPerformanceLoginEntry(models.Model):
    _name = 'mes.performance.login.entry'
    _description = 'Kiosk Login History'
    _order = 'login_at desc'

    workcenter_id = fields.Many2one('mrp.workcenter', required=True, ondelete='cascade', index=True)
    employee_id = fields.Many2one('hr.employee', required=True, ondelete='restrict')
    job_title = fields.Char(related='employee_id.job_title', store=True, readonly=True)
    role = fields.Selection([
        ('operator', 'Operator'),
        ('supervisor', 'Supervisor'),
        ('technician', 'Technician'),
    ], required=True, index=True)
    login_at = fields.Datetime(required=True, default=fields.Datetime.now, index=True)
    logout_at = fields.Datetime(index=True)
    login_method = fields.Selection([
        ('manual', 'Login/PIN'),
        ('fob', 'FOB Scan'),
    ], default='manual')
    duration_minutes = fields.Float(compute='_compute_duration_minutes', store=True, group_operator='sum')
    session_id = fields.Many2one('mes.kiosk.session', ondelete='set null', index=True)

    @api.depends('login_at', 'logout_at')
    def _compute_duration_minutes(self):
        now = fields.Datetime.now()
        for rec in self:
            if not rec.login_at:
                rec.duration_minutes = 0.0
                continue
            end = rec.logout_at or now
            rec.duration_minutes = (end - rec.login_at).total_seconds() / 60.0
