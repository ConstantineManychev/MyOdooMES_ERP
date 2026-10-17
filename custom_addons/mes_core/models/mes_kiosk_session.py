import logging

from odoo import models, fields, api, _
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)


class MesKioskSession(models.Model):
    _name = 'mes.kiosk.session'
    _description = 'MES Kiosk Session'
    _order = 'login_at desc'
    _rec_name = 'display_name'

    workcenter_id = fields.Many2one('mrp.workcenter', required=True, ondelete='restrict', index=True)
    employee_id = fields.Many2one('hr.employee', required=True, ondelete='restrict', index=True)
    user_id = fields.Many2one('res.users', ondelete='set null', index=True)
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
    ], required=True, default='manual')
    duration_minutes = fields.Float(compute='_compute_duration_minutes', store=True)
    is_active = fields.Boolean(compute='_compute_is_active', store=True, index=True)
    display_name = fields.Char(compute='_compute_display_name', store=True)

    def init(self):
        self.env.cr.execute("DROP INDEX IF EXISTS mes_kiosk_session_active_employee_uniq")
        self.env.cr.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS mes_kiosk_session_active_workcenter_uniq
            ON mes_kiosk_session (workcenter_id)
            WHERE logout_at IS NULL
        """)

    @api.depends('login_at', 'logout_at')
    def _compute_duration_minutes(self):
        now = fields.Datetime.now()
        for rec in self:
            end = rec.logout_at or now
            rec.duration_minutes = (end - rec.login_at).total_seconds() / 60.0 if rec.login_at else 0.0

    @api.depends('logout_at')
    def _compute_is_active(self):
        for rec in self:
            rec.is_active = not rec.logout_at

    @api.depends('employee_id.name', 'workcenter_id.name', 'role', 'login_at')
    def _compute_display_name(self):
        for rec in self:
            ts = fields.Datetime.context_timestamp(rec, rec.login_at).strftime('%Y-%m-%d %H:%M') if rec.login_at else ''
            rec.display_name = f"{rec.employee_id.name} @ {rec.workcenter_id.name} [{rec.role}] {ts}"

    @api.model
    def _resolve_employee_by_fob(self, fob_uid, alternatives=None):
        Employee = self.env['hr.employee'].sudo()
        candidates = [fob_uid] + list(alternatives or [])
        normalized = []
        for candidate in candidates:
            value = Employee._normalize_fob_uid(candidate)
            if value and value not in normalized:
                normalized.append(value)
        if not normalized:
            return Employee
        employee = Employee.search([('mes_fob_uid_normalized', 'in', normalized)], limit=1)
        if not employee:
            _logger.info("No employee matched FOB candidates %s", normalized)
        return employee

    @api.model
    def _resolve_employee_by_pin(self, login, pin):
        pin_value = (pin or login or '').strip()
        if not pin_value:
            return self.env['hr.employee']
        return self.env['hr.employee'].sudo().search([('mes_pin', '=', pin_value)], limit=1)

    @api.model
    def _classify_role(self, employee):
        return employee.mes_kiosk_role or False

    @api.model
    def toggle_fob_session(self, workcenter_id, employee, login_method='fob'):
        if not employee:
            raise UserError(_("Authentication failed."))
        active = self.sudo().search([
            ('workcenter_id', '=', workcenter_id),
            ('employee_id', '=', employee.id),
            ('logout_at', '=', False),
        ])
        if active:
            role = active[0].role
            active._do_logout(fields.Datetime.now())
            return {'action': 'logout', 'role': role, 'session': self.browse()}
        session = self.login(workcenter_id, employee, login_method=login_method)
        return {'action': 'login', 'role': session.role, 'session': session}

    @api.model
    def login(self, workcenter_id, employee, login_method='manual', force_role=None):
        if not employee:
            raise UserError(_("Authentication failed."))
            
        role = force_role or self._classify_role(employee) or 'operator'
        now = fields.Datetime.now()
        
        self._end_existing_active(workcenter_id, now)
        
        session = self.sudo().create({
            'workcenter_id': workcenter_id,
            'employee_id': employee.id,
            'user_id': employee.user_id.id if employee.user_id else False,
            'role': role,
            'login_method': login_method,
            'login_at': now,
        })
        
        self.env['mes.performance.login.entry'].sudo().create({
            'workcenter_id': workcenter_id,
            'employee_id': employee.id,
            'role': role,
            'login_method': login_method,
            'login_at': now,
            'session_id': session.id,
        })
        
        self.env['bus.bus']._sendone(
            f'mes.kiosk.workcenter.{workcenter_id}',
            'session_changed',
            {'session_id': session.id, 'role': role, 'employee_name': employee.name},
        )

        session._apply_login_side_effects()

        return session

    def _apply_login_side_effects(self):
        self.ensure_one()
        workcenter = self.workcenter_id
        if self.role == 'operator':
            self.env['mes.planned.downtime'].sudo().finish_on_operator_login(
                workcenter, _("%s (operator login)", self.employee_id.name))
        elif self.role == 'technician':
            self._auto_engage_single_open_task()

    def _auto_engage_single_open_task(self):
        self.ensure_one()
        subtask = self._single_open_technician_subtask()
        if not subtask:
            return False
        try:
            if subtask.state == subtask.TECH_STATE_REQUESTED:
                subtask.sudo().action_technician_arrive(self.id)
                return True

            subtask.sudo()._handover_technician(self)
            if subtask.state == subtask.TECH_STATE_PAUSED:
                subtask.sudo().with_context(
                    mes_acting_employee_id=self.employee_id.id
                ).action_technician_resume()
        except UserError as exc:
            _logger.info("Auto engage skipped for session %s: %s", self.id, exc)
            return False
        return True

    def _single_open_technician_subtask(self):
        self.ensure_one()
        Task = self.env['mes.task'].sudo()
        subtasks = Task.search([
            ('machine_id', '=', self.workcenter_id.id),
            ('task_type', '=', 'kiosk_help'),
            ('parent_id', '!=', False),
            ('state', 'in', list(Task._TECH_TRANSITIONABLE_STATES)),
        ])
        return subtasks if len(subtasks) == 1 else Task.browse()

    @api.model
    def _end_existing_active(self, workcenter_id, at):
        existing = self.sudo().search([
            ('workcenter_id', '=', workcenter_id),
            ('logout_at', '=', False),
        ])
        
        if existing:
            existing._do_logout(at)
        self.env.cr.execute(
            "UPDATE mes_kiosk_session SET logout_at = %s WHERE id = ANY(%s)",
            (at, existing.ids),
        )
        existing.invalidate_recordset(['logout_at', 'is_active'])
        self.env['mes.performance.login.entry'].sudo().search([
            ('session_id', 'in', existing.ids),
            ('logout_at', '=', False),
        ]).write({'logout_at': at})
        for rec in existing:
            self.env['bus.bus']._sendone(
                f'mes.kiosk.workcenter.{rec.workcenter_id.id}',
                'session_changed',
                {'session_id': rec.id, 'role': rec.role, 'logout': True},
            )

    def action_logout(self):
        self._do_logout(fields.Datetime.now())

    def _do_logout(self, at):
        for rec in self:
            if rec.logout_at:
                continue
            rec._pause_task_on_technician_logout()
            rec.logout_at = at
            entries = self.env['mes.performance.login.entry'].sudo().search([
                ('session_id', '=', rec.id),
                ('logout_at', '=', False),
            ])
            entries.write({'logout_at': at})
            self.env['bus.bus']._sendone(
                f'mes.kiosk.workcenter.{rec.workcenter_id.id}',
                'session_changed',
                {'session_id': rec.id, 'role': rec.role, 'logout': True},
            )

    def _pause_task_on_technician_logout(self):
        self.ensure_one()
        if self.role != 'technician':
            return False
        subtask = self.env['mes.task'].sudo().search([
            ('technician_session_id', '=', self.id),
            ('state', '=', 'assigned'),
        ], limit=1)
        if not subtask:
            return False
        comment = f"{self.employee_id.name} log out" if self.employee_id else "Technician log out"
        try:
            subtask.action_technician_pause(comment=comment)
        except UserError as exc:
            _logger.info("Auto pause skipped for session %s: %s", self.id, exc)
            return False
        return True

    @api.model
    def cron_close_stale_operator_sessions(self):
        cutoff = fields.Datetime.subtract(fields.Datetime.now(), hours=12)
        stale = self.search([
            ('logout_at', '=', False),
            ('login_at', '<', cutoff),
        ])
        stale.action_logout()
