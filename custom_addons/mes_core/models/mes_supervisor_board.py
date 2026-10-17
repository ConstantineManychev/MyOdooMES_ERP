from odoo import api, fields, models, _
from odoo.exceptions import AccessError, UserError

MANAGER_GROUP = 'mes_core.group_mes_manager'
REFRESH_PARAM = 'mes_core.dashboard_refresh_interval'
REFRESH_BOUNDS = (10, 60)
CLOSED_HELP_STATES = ('help_completed', 'help_rejected', 'cancel')
UNACCEPTED_HELP_STATES = ('help_open', 'help_reopened')
TECH_ON_SITE = ('assigned',)
TECH_WAITING = ('new', 'on_hold')
TECH_DONE = ('done', 'help_supervisor_confirmed')
TECH_TONES = ((TECH_ON_SITE, 'yellow'), (TECH_WAITING, 'red'), (TECH_DONE, 'green'))
URGENCY = {('red', True): 0, ('red', False): 1, ('yellow', False): 2, ('green', False): 3}


def pick_subtask(subtasks):
    for states, tone in TECH_TONES:
        for sub_id, sub_state in subtasks:
            if sub_state in states:
                return sub_id, tone
    return None, 'red'


def help_badge(task_id, state, subtasks):
    if state in UNACCEPTED_HELP_STATES:
        return {'label': 'HR', 'tone': 'red', 'blink': True, 'open_id': task_id}
    if state == 'help_supervisor_accepted':
        return {'label': 'HR', 'tone': 'yellow', 'blink': False, 'open_id': task_id}
    if state not in ('help_tech_requested', 'help_awaiting_operator'):
        return None
    sub_id, tone = pick_subtask(subtasks)
    if state == 'help_awaiting_operator':
        tone = 'green'
    if not sub_id:
        return {'label': 'HR', 'tone': tone, 'blink': False, 'open_id': task_id}
    return {'label': 'HR-T', 'tone': tone, 'blink': False, 'open_id': sub_id}


def most_urgent(badges):
    ranked = [badge for badge in badges if badge]
    if not ranked:
        return None
    return min(ranked, key=lambda badge: URGENCY.get((badge['tone'], badge['blink']), 9))


def refresh_seconds(raw):
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = REFRESH_BOUNDS[1]
    low, high = REFRESH_BOUNDS
    return min(max(value, low), high)


class MesSupervisorBoard(models.AbstractModel):
    _name = 'mes.supervisor.board'
    _description = 'Supervisor Dashboard'

    def _check_manager(self):
        if not self.env.user.has_group(MANAGER_GROUP):
            raise AccessError(_("The supervisor dashboard is available to MES managers only."))

    def _machines(self):
        return self.env['mrp.workcenter'].search([
            ('machine_settings_id', '!=', False),
            ('company_id', 'in', self.env.companies.ids),
        ])

    @api.model
    def get_board(self):
        self._check_manager()
        machines = self._machines()
        helps = self._open_helps(machines)
        param = self.env['ir.config_parameter'].sudo().get_param(REFRESH_PARAM)
        return {
            'machines': [self._card(machine, helps.get(machine.id, [])) for machine in machines],
            'can_toggle_pm': True,
            'refresh_sec': refresh_seconds(param),
        }

    @api.model
    def set_pm(self, workcenter_id, active):
        self._check_manager()
        workcenter = self._machines().filtered(lambda machine: machine.id == int(workcenter_id or 0))
        if not workcenter:
            return {'ok': False, 'error': _("This machine is not available to you.")}
        Downtime = self.env['mes.planned.downtime'].sudo()
        try:
            if active:
                Downtime.start_workcenter_pm(workcenter, self.env.user, origin='dashboard')
            else:
                Downtime._finish_now(Downtime._active_pm_slots(workcenter), self.env.user.name)
        except UserError as exc:
            return {'ok': False, 'error': str(exc.args[0] if exc.args else exc)}
        workcenter.invalidate_recordset(['pm_active'])
        return {'ok': True, 'active': workcenter.pm_active}

    def _open_helps(self, machines):
        tasks = self.env['mes.task'].sudo().search([
            ('machine_id', 'in', machines.ids),
            ('task_type', '=', 'kiosk_help'),
            ('parent_id', '=', False),
            ('state', 'not in', list(CLOSED_HELP_STATES)),
        ], order='id desc')
        grouped = {}
        for task in tasks:
            grouped.setdefault(task.machine_id.id, []).append(task)
        return grouped

    def _cycle_subtasks(self, task):
        subtasks = task.child_ids.sorted('id', reverse=True)
        if task.reopened_at:
            subtasks = subtasks.filtered(lambda sub: sub.create_date and sub.create_date >= task.reopened_at)
        return subtasks

    def _help_info(self, machine, tasks):
        badges = []
        for task in tasks:
            subtasks = self._cycle_subtasks(task)
            badge = help_badge(task.id, task.state, [(sub.id, sub.state) for sub in subtasks])
            if badge:
                badge['title'] = self._help_title(machine, task, subtasks, badge)
                badges.append(badge)
        chosen = most_urgent(badges)
        if chosen:
            chosen['count'] = len(badges)
        return chosen

    def _help_title(self, machine, task, subtasks, badge):
        states = dict(self.env['mes.task']._fields['state']._description_selection(self.env))
        parts = [task.name, states.get(task.state, task.state)]
        opened = subtasks.filtered(lambda sub: sub.id == badge['open_id'])
        if opened:
            technician = opened.assigned_id.name or opened.technician_session_id.employee_id.name
            parts.append(_("Technician: %s", states.get(opened.state, opened.state)
                           + (f" ({technician})" if technician else '')))
        parts.append(_("Opened %s", self._local_stamp(machine, task.create_date)))
        return "\n".join(part for part in parts if part)

    def _local_stamp(self, machine, moment):
        return self.env['mes.planned.downtime']._short_local(machine, moment)

    def _pm_title(self, machine):
        slot = self.env['mes.planned.downtime']._active_pm_slots(machine)[:1]
        if not slot:
            return ''
        rule = slot.rule_id
        if rule.started_by:
            return _("%(name)s, started by %(who)s at %(when)s", name=rule.name, who=rule.started_by,
                     when=self._local_stamp(machine, rule.started_at or slot.start_time))
        return _("%(name)s, until %(when)s", name=rule.name, when=self._local_stamp(machine, slot.end_time))

    def _sessions(self, machine):
        roles = dict(self.env['mes.kiosk.session']._fields['role']._description_selection(self.env))
        return [{
            'id': session.id,
            'role': session.role,
            'role_label': roles.get(session.role, session.role or ''),
            'name': session.employee_id.name or '',
            'job_title': session.employee_id.job_title or '',
        } for session in machine._active_kiosk_sessions()]

    def _card(self, machine, tasks):
        return {
            'id': machine.id,
            'name': machine.name,
            'status_label': machine.mes_status_label or '',
            'status_color': machine.mes_status_color or '',
            'status_since': fields.Datetime.to_string(machine.mes_status_since) if machine.mes_status_since else '',
            'status_provisional': machine.mes_status_provisional,
            'start_time': self._local_stamp(machine, machine.current_first_running_time),
            'start_time_full': machine.current_first_running_time_disp or '',
            'produced': machine.current_produced,
            'runtime': machine.current_runtime_formatted or '',
            'waste_losses': machine.current_waste_losses,
            'downtime_losses': machine.current_downtime_losses,
            'oee': machine.current_oee,
            'top_rejection': machine.current_top_rejection or '',
            'top_alarm': machine.current_top_alarm or '',
            'pm_active': machine.pm_active,
            'pm_title': self._pm_title(machine) if machine.pm_active else '',
            'help': self._help_info(machine, tasks),
            'sessions': self._sessions(machine),
            'badges': [{
                'key': badge.get('key') or '',
                'title': badge.get('title') or '',
                'name': badge.get('name') or '',
                'details': list(badge.get('details') or []),
                'tone': badge.get('tone') or 'normal',
            } for badge in machine.sudo()._kiosk_header_badges()],
        }
