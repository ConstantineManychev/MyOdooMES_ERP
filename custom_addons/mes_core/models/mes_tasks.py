import logging
import pytz
import json
import hashlib
from dateutil import parser
from datetime import datetime
from odoo import models, fields, api, _
from odoo.exceptions import UserError
from odoo.addons.mes_core.tools.maintainx_api import MaintainXClient

_logger = logging.getLogger(__name__)

class MesTaskReasonContribution(models.Model):
    _name = 'mes.task.reason.contribution'
    _description = 'Help Request Reason Contribution'
    _order = 'create_date'

    task_id = fields.Many2one('mes.task', required=True, ondelete='cascade', index=True)
    role = fields.Selection([
        ('operator', 'Operator'),
        ('supervisor', 'Supervisor'),
        ('technician', 'Tech'),
    ], required=True)
    employee_id = fields.Many2one('hr.employee', string='Author')
    reason_label = fields.Char()
    comment = fields.Text()

    def format_line(self):
        self.ensure_one()
        role_label = dict(self._fields['role'].selection).get(self.role, self.role)
        author = self.employee_id.name or ''
        head = f"{role_label} {author}".strip()
        parts = [self.reason_label or '', self.comment or '']
        body = ". ".join(part.strip() for part in parts if part and part.strip())
        return f"{head}: {body}" if body else f"{head}:"


class MesTaskWorkInterval(models.Model):
    _name = 'mes.task.work.interval'
    _description = 'Technician Work Interval'
    _order = 'start_time desc'

    task_id = fields.Many2one('mes.task', required=True, ondelete='cascade', index=True)
    machine_id = fields.Many2one('mrp.workcenter', required=True, ondelete='cascade', index=True)
    employee_id = fields.Many2one('hr.employee', string='Technician')
    start_time = fields.Datetime(required=True)
    end_time = fields.Datetime()


class MesTaskStatusHistory(models.Model):
    _name = 'mes.task.status.history'
    _description = 'Task Status History'
    _order = 'change_date desc'

    task_id = fields.Many2one('mes.task', string='Task', ondelete='cascade')
    status = fields.Char(string='Status')
    change_date = fields.Datetime(string='Date', default=fields.Datetime.now)
    source = fields.Selection([('odoo', 'Odoo'), ('maintainx', 'MaintainX')], string='Source', default='maintainx')
    user_id = fields.Many2one('res.users', string='User')
    employee_id = fields.Many2one('hr.employee', string='Employee')
    comment = fields.Text(string='Comment')
    mx_comment_id = fields.Char(string='MaintainX Comment ID', index=True)

class MesTask(models.Model):
    _name = 'mes.task'
    _description = 'MES Task'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    _STATUS_MAPPING = {
        'OPEN': 'new',
        'IN_PROGRESS': 'assigned',
        'ON_HOLD': 'on_hold',
        'DONE': 'done',
        'COMPLETED': 'done',
        'CANCELED': 'cancel',
        'CANCELLED': 'cancel',
        'SKIPPED': 'cancel',
    }

    _STATE_TO_MX = {
        'new': 'OPEN',
        'assigned': 'IN_PROGRESS',
        'on_hold': 'ON_HOLD',
        'done': 'DONE',
        'cancel': 'CANCELED',
    }

    TECH_STATE_REQUESTED = 'new'
    TECH_STATE_IN_PROGRESS = 'assigned'
    TECH_STATE_PAUSED = 'on_hold'
    TECH_STATE_CLOSED = 'done'
    TECH_STATE_REJECTED = 'cancel'

    _PRIORITY_MAPPING = {
        'HIGH': '2',
        'MEDIUM': '1',
        'LOW': '0',
    }

    name = fields.Char(string='Task Title', required=True)
    description = fields.Html(string='Description')
    
    maintainx_id = fields.Char(string="MaintainX ID", readonly=True, copy=False, index=True)
    maintainx_created_at = fields.Datetime(string="MX Created At", readonly=True)
    maintainx_assignees_history = fields.Text(string="Assignees History (MX)", readonly=True)
    maintainx_updated_at = fields.Datetime(string="MX Updated At", readonly=True)
    maintainx_data_hash = fields.Char(string="Data Hash", index=True, copy=False)

    status_history_ids = fields.One2many('mes.task.status.history', 'task_id', string="Status History")
    author_id = fields.Many2one('res.users', string='Author', default=lambda self: self.env.user, readonly=True)
    assigned_id = fields.Many2one('hr.employee', string='Assigned To', tracking=True)
    machine_id = fields.Many2one('mrp.workcenter', string='Machine', tracking=True)
    part_id = fields.Many2one('mes.part', string='Part',
                              help="Optional. When set, the MaintainX work order is "
                                   "raised against this part instead of the machine.")
    piece_ids = fields.Many2many('mes.piece', string='Pieces',
                                 domain="[('part_id', '=', part_id)]")
    
    company_id = fields.Many2one('res.company', string='Company', required=True, default=lambda self: self.env.company)

    state = fields.Selection([
        ('new', 'Open'),
        ('assigned', 'In Progress'),
        ('on_hold', 'On Hold'),
        ('done', 'Done'),
        ('cancel', 'Cancelled'),
        ('help_open', 'Help: Open'),
        ('help_supervisor_accepted', 'Help: Supervisor Accepted'),
        ('help_tech_requested', 'Help: Technician Requested'),
        ('help_supervisor_confirmed', 'Help: Supervisor Confirmed'),
        ('help_awaiting_operator', 'Help: Awaiting Operator Confirmation'),
        ('help_completed', 'Help: Completed'),
        ('help_rejected', 'Help: Rejected by Technician'),
        ('help_reopened', 'Help: Reopened'),
    ], string='Status', default='new', tracking=True, group_expand='_expand_states')

    task_type = fields.Selection([
        ('maintainx', 'MaintainX'),
        ('kiosk_help', 'Kiosk Help Request'),
    ], default='maintainx', required=True, index=True)

    reason_event_id = fields.Many2one('mes.event', string='Event Reason', tracking=True)
    reason_count_id = fields.Many2one('mes.counts', string='Count Reason', tracking=True)
    reason_process_id = fields.Many2one('mes.process', string='Process Reason', tracking=True)
    reason_defect_id = fields.Many2one('mes.defect', string='Defect Reason', tracking=True)
    problem_area = fields.Selection([
        ('alarm', 'Alarm'),
        ('rejection', 'Rejection'),
        ('defect', 'Defect'),
        ('manual', 'Manual'),
    ], string='Problem Area', default='manual', tracking=True)
    reason_source = fields.Selection([
        ('auto_event', 'Auto: Current Stoppage'),
        ('auto_count', 'Auto: Top Recent Count'),
        ('manual', 'Manual'),
    ], default='manual', tracking=True)
    operator_description = fields.Text(string='Operator Description')
    reason_snapshot = fields.Char(
        string='Reason At Creation', readonly=True, copy=False,
        help="Machine reason resolved when the help request was raised, kept even "
             "when the operator described the problem manually.")

    def _log_status_change(self, new_state, comment=None):
        label = dict(self._fields['state'].selection).get(new_state, new_state)
        ctx = self.env.context
        employee_id = ctx.get('mes_acting_employee_id')
        if not employee_id:
            employee_id = self.env.user.employee_id.id if self.env.user.employee_id else False
        History = self.env['mes.task.status.history'].sudo()
        for task in self:
            History.create({
                'task_id': task.id,
                'status': label,
                'change_date': fields.Datetime.now(),
                'source': 'odoo',
                'user_id': self.env.user.id,
                'employee_id': employee_id,
                'comment': comment if comment is not None else ctx.get('mes_status_comment') or False,
            })

    @api.model_create_multi
    def create(self, vals_list):
        tasks = super().create(vals_list)
        if not self.env.context.get('skip_auto_status_history'):
            for task in tasks:
                task._log_status_change(task.state)
        return tasks

    def write(self, vals):
        if 'state' not in vals:
            return super().write(vals)

        state_changed = self.filtered(lambda t: t.state != vals['state'])
        state_changed._close_work_intervals()
        res = super().write(vals)
        state_changed._open_work_intervals(vals['state'])

        if not self.env.context.get('skip_auto_status_history'):
            if state_changed:
                state_changed._log_status_change(vals['state'])
            if vals['state'] == self.TECH_STATE_REJECTED:
                state_changed._finalize_parent_after_rejection()
        return res

    def _close_work_intervals(self):
        open_intervals = self.env['mes.task.work.interval'].sudo().search([
            ('task_id', 'in', self.ids),
            ('end_time', '=', False),
        ])
        if open_intervals:
            open_intervals.write({'end_time': fields.Datetime.now()})

    def _open_work_intervals(self, new_state):
        if new_state != self.TECH_STATE_IN_PROGRESS:
            return
        Interval = self.env['mes.task.work.interval'].sudo()
        for task in self.filtered(lambda t: t.task_type == 'kiosk_help' and t.parent_id):
            Interval.create({
                'task_id': task.id,
                'machine_id': task.machine_id.id,
                'employee_id': task.technician_session_id.employee_id.id or False,
                'start_time': fields.Datetime.now(),
            })

    def _finalize_parent_after_rejection(self):
        live_states = set(self._TECH_TRANSITIONABLE_STATES) | {self.TECH_STATE_CLOSED}
        for sub in self:
            parent = sub.parent_id
            if not parent:
                continue
            if parent.state in ('help_completed', 'help_rejected', 'cancel',
                                'help_awaiting_operator'):
                continue
            if any(t.state in live_states for t in parent.child_ids):
                continue
            parent.write({
                'state': 'help_rejected',
                'closed_at': fields.Datetime.now(),
            })
            parent._broadcast_state()

    operator_session_id = fields.Many2one('mes.kiosk.session', string='Operator Session', readonly=True, ondelete='restrict')
    supervisor_session_id = fields.Many2one('mes.kiosk.session', string='Supervisor Session', readonly=True, ondelete='restrict')
    supervisor_accepted_at = fields.Datetime(readonly=True, tracking=True)
    technician_session_id = fields.Many2one('mes.kiosk.session', string='Technician Session', readonly=True, ondelete='restrict')
    technician_arrived_at = fields.Datetime(readonly=True, tracking=True)

    closed_at = fields.Datetime(readonly=True, tracking=True)
    operator_confirmation_state = fields.Selection([
        ('pending', 'Pending'),
        ('confirmed', 'Confirmed'),
        ('not_done', 'Not Done'),
    ], readonly=True, tracking=True)
    operator_confirmation_comment = fields.Text(readonly=True)
    operator_confirmed_at = fields.Datetime(readonly=True, tracking=True)
    confirmation_prompt_due_at = fields.Datetime(readonly=True, index=True)
    reopened_at = fields.Datetime(readonly=True, tracking=True)
    reopened_by_id = fields.Many2one('res.users', readonly=True, tracking=True)

    priority = fields.Selection([
        ('0', 'Low'),
        ('1', 'Medium'),
        ('2', 'High'),
        ('3', 'Blocker'),
    ], string='Priority', default='0', tracking=True)

    parent_id = fields.Many2one('mes.task', string="Parent Task", ondelete='cascade', index=True)
    child_ids = fields.One2many('mes.task', 'parent_id', string="Subtasks")
    child_count = fields.Integer(compute='_compute_child_count')

    @api.depends('child_ids')
    def _compute_child_count(self):
        for rec in self:
            rec.child_count = len(rec.child_ids)

    def _subtask_context(self):
        self.ensure_one()
        return {
            'default_parent_id': self.id,
            'default_machine_id': self.machine_id.id,
            'default_company_id': self.company_id.id,
            'default_priority': self.priority,
            'default_task_type': self.task_type,
            'default_assigned_id': self.assigned_id.id,
        }

    def action_view_subtasks(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': _("Subtasks of %s", self.name),
            'res_model': 'mes.task',
            'view_mode': 'tree,form',
            'domain': [('parent_id', '=', self.id)],
            'context': self._subtask_context(),
        }

    def action_create_subtask(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'name': _("New Subtask"),
            'res_model': 'mes.task',
            'view_mode': 'form',
            'target': 'current',
            'context': self._subtask_context(),
        }

    _KANBAN_DEFAULT_STATES = ('new', 'assigned', 'on_hold', 'done')
    _KANBAN_HELP_REQUEST_STATES = (
        'help_open', 'help_supervisor_accepted', 'help_tech_requested',
        'help_awaiting_operator', 'help_completed', 'help_rejected',
    )

    @api.model
    def _expand_states(self, states, domain, order):
        if self.env.context.get('mes_help_request_dashboard'):
            return list(self._KANBAN_HELP_REQUEST_STATES)
        return list(self._KANBAN_DEFAULT_STATES)

    @api.model
    def kiosk_create_help_request(self, workcenter_id, operator_session_id, payload):
        workcenter = self.env['mrp.workcenter'].sudo().browse(workcenter_id)
        if not workcenter.exists():
            raise UserError("Unknown workcenter.")
        if workcenter.has_open_help_request:
            raise UserError("A help request is already open for this workcenter.")
        operator_session = self.env['mes.kiosk.session'].sudo().browse(operator_session_id)
        if not operator_session.exists() or not operator_session.is_active:
            raise UserError("Operator session expired.")
        if not workcenter._kiosk_allowed_session('help', operator_session.id):
            raise UserError(_("Log in with a role that may call for help on this machine."))

        area = payload.get('problem_area') or 'manual'
        if area not in ('alarm', 'rejection', 'defect', 'manual'):
            raise UserError(_("Unknown problem area: %s", area))

        reason_event = self.env['mes.event'].sudo().browse(
            payload.get('reason_event_id') or payload.get('reason_manual_event_id') or 0)
        reason_count = self.env['mes.counts'].sudo().browse(payload.get('reason_count_id') or 0)
        reason_process = self.env['mes.process'].sudo().browse(payload.get('reason_process_id') or 0)
        reason_defect = self.env['mes.defect'].sudo().browse(payload.get('reason_defect_id') or 0)
        description = (payload.get('operator_description') or '').strip()

        if area == 'alarm' and not reason_event.exists():
            raise UserError(_("Select a stoppage reason for the Alarm area."))
        if area == 'rejection' and not reason_count.exists():
            raise UserError(_("Select a reject count for the Rejection area."))
        if area == 'defect' and not reason_defect.exists():
            raise UserError(_("Select a defect type for the Defect area."))
        if area == 'manual' and not description:
            raise UserError(_("Describe the problem for the Manual area."))

        if area != 'alarm':
            reason_event = self.env['mes.event']
        if area != 'rejection':
            reason_count = self.env['mes.counts']
        if area != 'defect':
            reason_defect = self.env['mes.defect']

        reason_source = payload.get('reason_source')
        if reason_source not in ('auto_event', 'auto_count', 'manual'):
            reason_source = {'alarm': 'auto_event', 'rejection': 'auto_count'}.get(area, 'manual')

        sequence = self.env['ir.sequence'].sudo().next_by_code('mes.task.kiosk_help') or '/'
        operator_employee = operator_session.employee_id
        area_label = dict(self._fields['problem_area'].selection).get(area, area)
        reason_label = (reason_event.name if reason_event else
                        reason_count.name if reason_count else
                        reason_defect.name if reason_defect else '—')
        initial_summary = f"Category: {area_label}; Reason: {reason_label}"
        if description:
            initial_summary += f"; Description: {description}"
        task = self.sudo().with_context(
            mes_acting_employee_id=operator_employee.id if operator_employee else False,
            mes_status_comment=initial_summary,
        ).create({
            'name': self._compose_help_request_name(workcenter, sequence, reason_label),
            'task_type': 'kiosk_help',
            'state': 'help_open',
            'machine_id': workcenter.id,
            'operator_session_id': operator_session.id,
            'author_id': (operator_session.user_id or operator_session.employee_id.user_id or self.env.user).id,
            'problem_area': area,
            'reason_source': reason_source,
            'reason_event_id': reason_event.id if reason_event else False,
            'reason_count_id': reason_count.id if reason_count else False,
            'reason_process_id': reason_process.id if reason_process.exists() else False,
            'reason_defect_id': reason_defect.id if reason_defect else False,
            'operator_description': description,
            'reason_snapshot': (payload.get('reason_label') or '').strip()
                               or self._capture_reason_snapshot(workcenter, reason_event,
                                                                reason_count, reason_defect),
            'priority': reason_event.priority if (area == 'alarm' and reason_event) else '0',
            'part_id': self._safe_part_id(workcenter, payload.get('part_id')),
            'piece_ids': [(6, 0, self._safe_piece_ids(payload.get('part_id'),
                                                     payload.get('piece_ids')))],
        })
        task._record_reason_contribution('Operator', operator_employee,
                                         task._format_reason_label(), description)
        task._notify_supervisors_new_request()
        return task

    @api.model
    def _safe_part_id(self, workcenter, raw_part_id):
        if not raw_part_id:
            return False
        part = self.env['mes.part'].sudo().browse(int(raw_part_id)).exists()
        return part.id if part else False

    @api.model
    def _safe_piece_ids(self, raw_part_id, raw_piece_ids):
        if not raw_part_id or not raw_piece_ids:
            return []
        pieces = self.env['mes.piece'].sudo().browse(
            [int(pid) for pid in raw_piece_ids]).exists()
        return pieces.filtered(lambda p: p.part_id.id == int(raw_part_id)).ids

    @api.model
    def _resolve_help_prefill(self, workcenter):
        alarm_current = workcenter._get_current_open_alarm()
        is_current_stop = False
        suggested_event = self.env['mes.event']
        if alarm_current and alarm_current.loss_id:
            is_operator_stop = (workcenter.operator_stop_event_id
                                and alarm_current.loss_id == workcenter.operator_stop_event_id)
            if not is_operator_stop:
                suggested_event = alarm_current.loss_id
                is_current_stop = True
        if not suggested_event:
            suggested_event = workcenter._get_most_frequent_alarm(hours=24)
        alarm_options = workcenter._get_alarm_frequency_options(days=7, limit=15)
        known_ids = {o['id'] for o in alarm_options}
        if suggested_event and suggested_event.id not in known_ids:
            alarm_options.insert(0, {'id': suggested_event.id,
                                     'label': suggested_event.name, 'count': 0})
            known_ids.add(suggested_event.id)
        for event in self._catalogue_alarm_events(workcenter):
            if event.id not in known_ids:
                alarm_options.append({'id': event.id, 'label': event.name, 'count': 0})
                known_ids.add(event.id)

        reject_candidates = workcenter._get_reject_rate_candidates(hours=8)
        suggested_count = reject_candidates[0] if reject_candidates else None

        defects = self.env['mes.defect'].sudo().search([], limit=200)

        return {
            'default_area': 'alarm' if is_current_stop else 'manual',
            'stream_layout': workcenter.get_stream_layout(),
            'alarm': {
                'suggested_event_id': suggested_event.id or None,
                'suggested_label': suggested_event.name if suggested_event else None,
                'is_current_stop': is_current_stop,
                'options': alarm_options,
            },
            'rejection': {
                'suggested_count_id': suggested_count['id'] if suggested_count else None,
                'suggested_label': suggested_count['label'] if suggested_count else None,
                'suggested_rate': suggested_count['rate'] if suggested_count else None,
                'options': reject_candidates,
            },
            'defect': {
                'options': [{'id': d.id, 'label': d.name} for d in defects],
            },
        }

    @api.model
    def _resolve_stream_reasons(self, workcenter, stream):
        empty = {'alarm': [], 'manual': [], 'rejection': [], 'defect': [], 'parts': {}}
        if stream not in workcenter._get_available_streams():
            return empty

        stream_domain = ['|', ('stream_id', '=', stream.id), ('stream_id', '=', False),
                         ('hide_in_kiosk', '=', False)]
        Event = self.env['mes.event'].sudo()
        Count = self.env['mes.counts'].sudo()
        Defect = self.env['mes.defect'].sudo()

        events = Event.search(stream_domain, order='complete_name')
        counts = Count.search(stream_domain, order='complete_name')
        defects = Defect.search([], order='name')

        def serialize(record):
            return {
                'id': record.id,
                'label': record.name,
                'part_id': record.part_id.id if 'part_id' in record._fields
                           and record.part_id else False,
                'piece_id': record.piece_id.id if 'piece_id' in record._fields
                            and record.piece_id else False,
            }

        alarm = [dict(serialize(e), is_manual_stop=False)
                 for e in events if not e.is_manual_stop]
        manual = [dict(serialize(e), is_manual_stop=True)
                  for e in events if e.is_manual_stop]
        rejection = [serialize(c) for c in counts]
        defect = [{'id': d.id, 'label': d.name, 'part_id': False, 'piece_id': False}
                  for d in defects]

        parts = {}
        for key, rows, records in (('alarm', alarm, events.filtered(lambda e: not e.is_manual_stop)),
                                   ('manual', manual, events.filtered('is_manual_stop')),
                                   ('rejection', rejection, counts)):
            used = records.mapped('part_id')
            parts[key] = [{
                'id': part.id,
                'name': part.name,
                'pieces': [{'id': piece.id, 'name': piece.name}
                           for piece in part.piece_ids.sorted(lambda p: (p.sequence, p.name))],
            } for part in used.sorted('name')]
        parts['defect'] = []

        return {'alarm': alarm, 'manual': manual, 'rejection': rejection,
                'defect': defect, 'parts': parts}

    @api.model
    def _catalogue_alarm_events(self, workcenter):
        Event = self.env['mes.event'].sudo()
        stream_ids = workcenter._get_available_streams().ids
        domain = [('hide_in_kiosk', '=', False)]
        if stream_ids:
            domain = ['|', ('stream_id', 'in', stream_ids), ('stream_id', '=', False),
                      ('hide_in_kiosk', '=', False)]
        return Event.search(domain, order='complete_name', limit=500)

    @api.model
    def _compose_help_request_name(self, workcenter, sequence, reason_label):
        parts = [workcenter.name, str(sequence)]
        if reason_label and reason_label != '—':
            parts.append(reason_label)
        return " ".join(part for part in parts if part)

    @api.model
    def _resolve_help_request_reason(self, workcenter):
        alarm = workcenter._get_current_open_alarm()
        if alarm and alarm.loss_id:
            is_operator_stop = workcenter.operator_stop_event_id and alarm.loss_id == workcenter.operator_stop_event_id
            if not is_operator_stop:
                return {
                    'reason_source': 'auto_event',
                    'reason_event_id': alarm.loss_id.id,
                }
        count = workcenter._get_top_recent_non_production_count()
        if count:
            return {
                'reason_source': 'auto_count',
                'reason_count_id': count.id,
            }
        return {'reason_source': 'manual'}

    def _notify_supervisors_new_request(self):
        self.ensure_one()
        supervisor_group = self.env.ref('mes_core.group_mes_supervisor', raise_if_not_found=False)
        if supervisor_group:
            self.env['bus.bus']._sendone(
                'mes.help.supervisor',
                'help_request_created',
                {
                    'task_id': self.id,
                    'workcenter_id': self.machine_id.id,
                    'workcenter_name': self.machine_id.name,
                    'priority': self.priority,
                    'reason_name': self._format_reason_label(),
                },
            )
            for supervisor in supervisor_group.users:
                self.activity_schedule(
                    'mail.mail_activity_data_todo',
                    user_id=supervisor.id,
                    summary=f"Help request from {self.machine_id.name}",
                    note=self.operator_description or '',
                )
        self.env['bus.bus']._sendone(
            f'mes.kiosk.workcenter.{self.machine_id.id}',
            'help_request_state',
            {'task_id': self.id, 'state': self.state},
        )

    @api.model
    def _capture_reason_snapshot(self, workcenter, reason_event, reason_count, reason_defect):
        for ref in (reason_event, reason_count, reason_defect):
            if ref:
                return ref.name
        resolved = self._resolve_help_request_reason(workcenter) or {}
        if resolved.get('reason_event_id'):
            return self.env['mes.event'].sudo().browse(resolved['reason_event_id']).name
        if resolved.get('reason_count_id'):
            return self.env['mes.counts'].sudo().browse(resolved['reason_count_id']).name
        return False

    def _format_reason_label(self):
        self.ensure_one()
        for fld in ('reason_event_id', 'reason_count_id', 'reason_process_id', 'reason_defect_id'):
            ref = self[fld]
            if ref:
                return ref.name
        return self.reason_snapshot or self.operator_description or '—'

    def action_supervisor_accept(self, supervisor_session_id):
        self.ensure_one()
        if self.state != 'help_open':
            raise UserError("Help request is not in 'Open' state.")
        session = self.env['mes.kiosk.session'].sudo().browse(supervisor_session_id)
        if not session.exists() or not session.is_active or session.role != 'supervisor':
            raise UserError("Invalid supervisor session.")
        if session.workcenter_id != self.machine_id:
            raise UserError("Supervisor must scan at the requesting machine.")
        self.with_context(
            mes_acting_employee_id=session.employee_id.id if session.employee_id else False,
        ).write({
            'state': 'help_supervisor_accepted',
            'supervisor_session_id': session.id,
            'supervisor_accepted_at': fields.Datetime.now(),
        })
        self._broadcast_state()

    def action_supervisor_close(self, comment=None):
        self.ensure_one()
        if self.state != 'help_supervisor_accepted':
            raise UserError("Only an accepted help request can be closed by supervisor.")
        comment = (comment or '').strip()
        if not comment:
            raise UserError(_("A comment is required to close the help request."))
        now = fields.Datetime.now()
        sup_emp = self.supervisor_session_id.employee_id
        delay_min = self.machine_id.operator_feedback_delay or 0
        self.with_context(
            mes_acting_employee_id=sup_emp.id if sup_emp else False,
            mes_status_comment=comment,
        ).write({
            'state': 'help_awaiting_operator',
            'closed_at': now,
            'operator_confirmation_state': 'pending',
            'confirmation_prompt_due_at': fields.Datetime.add(now, minutes=delay_min),
        })
        author = sup_emp.name if sup_emp else self.env.user.name
        self.message_post(body=f"<b>{author} (close):</b> {comment}")
        self._broadcast_state()

    def action_supervisor_request_technician(self, payload=None):
        self.ensure_one()
        if self.state != 'help_supervisor_accepted':
            raise UserError("Only an accepted help request can be escalated to technician.")
        payload = payload or {}
        sup_emp = self.supervisor_session_id.employee_id
        ctx = {'mes_acting_employee_id': sup_emp.id if sup_emp else False}
        subtask_vals = self._inherit_fields_for_subtask(payload)
        subtask = self.sudo().with_context(**ctx).create(subtask_vals)
        self.with_context(**ctx).write({'state': 'help_tech_requested'})
        self._broadcast_state()
        self._record_reason_contribution('Supervisor', sup_emp,
                                         self._format_reason_label(),
                                         (payload.get('description') or '').strip())
        subtask._push_help_request_to_maintainx(acting_employee=sup_emp)
        subtask._notify_technicians_new_request()
        return subtask

    def _mx_procedure_template_id(self):
        self.ensure_one()
        raw = (self.machine_id.maintainx_procedure_template_id or '').strip()
        if not raw:
            return False
        try:
            value = int(raw)
        except (TypeError, ValueError):
            _logger.warning("Invalid MaintainX procedure template id %r on work center %s",
                            raw, self.machine_id.display_name)
            return False
        return value if value > 0 else False

    def _push_help_request_to_maintainx(self, acting_employee=False):
        self.ensure_one()
        company = self.machine_id.company_id or self.company_id or self.env.company
        if not company.maintainx_enabled or not company.maintainx_api_token:
            return False
        if self.maintainx_id:
            return False
        try:
            client, _is_personal = self._get_api_client_as(company, acting_employee)
            payload = {
                'title': self.name,
                'description': self._build_maintainx_description(),
                'priority': self._mx_priority_value(),
                'type': 'REACTIVE',
            }
            asset_id = self._mx_asset_id()
            if asset_id:
                payload['assetId'] = asset_id
            default_assignee = self._build_mx_assignee(company.maintainx_default_assignee_id)
            if default_assignee:
                payload['assignees'] = [default_assignee]
            procedure_template_id = self._mx_procedure_template_id()
            if procedure_template_id:
                payload['procedureTemplateId'] = procedure_template_id
            created = client.create_workorder(payload)
            if not created and payload.get('procedureTemplateId'):
                _logger.warning("MaintainX work order creation retried without procedure template %s "
                                "for subtask %s", payload['procedureTemplateId'], self.id)
                payload.pop('procedureTemplateId')
                created = client.create_workorder(payload)
            if not created and payload.get('assignees'):
                _logger.warning("MaintainX work order creation retried without assignees for subtask %s",
                                self.id)
                payload.pop('assignees')
                created = client.create_workorder(payload)
        except Exception:
            _logger.exception("MaintainX push failed for help subtask %s", self.id)
            return False
        if not created or not created.get('id'):
            _logger.error("MaintainX work order not created for help subtask %s (payload=%s)",
                          self.id, payload)
            return False
        _logger.info("MaintainX work order %s created for help subtask %s",
                     created.get('id'), self.id)
        self._post_initiator_comment(acting_employee)
        self.sudo().with_context(skip_auto_status_history=True).write({
            'maintainx_id': str(created.get('id')),
        })
        if self.state != self.TECH_STATE_REQUESTED:
            self._sync_state_to_maintainx()
        return True

    def _handover_technician(self, session):
        self.ensure_one()
        if not session or not session.employee_id:
            return False
        if self.technician_session_id == session:
            return False

        previous = self.technician_session_id.employee_id
        self.sudo().with_context(skip_auto_status_history=True).write({
            'technician_session_id': session.id,
            'assigned_id': session.employee_id.id,
        })
        self._assign_technician_in_maintainx(session.employee_id)

        handover_note = (f"Technician handover: {previous.name} to {session.employee_id.name}"
                         if previous else f"Technician assigned: {session.employee_id.name}")
        self.message_post(body=f"<b>{handover_note}</b>")
        self._post_maintainx_comment(handover_note, author_employee=session.employee_id)
        _logger.info("Task %s handed over to %s", self.id, session.employee_id.name)
        return True

    def _assign_technician_in_maintainx(self, employee):
        self.ensure_one()
        if not self.maintainx_id or not employee:
            return False
        if self.env.context.get('skip_maintainx_push'):
            return False
        company = self.machine_id.company_id or self.company_id or self.env.company
        if not company.maintainx_enabled or not company.maintainx_api_token:
            return False

        assignee = self._build_mx_assignee(employee)
        if not assignee:
            _logger.warning("MaintainX assignee skipped: employee %s has no MaintainX ID set",
                            employee.name)
            return False
        try:
            client, _is_personal = self._get_api_client_as(company, employee)
            updated = client.update_workorder_assignees(self.maintainx_id, [assignee])
        except Exception:
            _logger.exception("MaintainX assignee push failed for task %s", self.id)
            return False
        if updated:
            _logger.info("MaintainX work order %s assigned to %s (task %s)",
                         self.maintainx_id, employee.name, self.id)
        else:
            _logger.error("MaintainX assignee push rejected for work order %s (task %s)",
                          self.maintainx_id, self.id)
        return bool(updated)

    def _post_status_change_comment(self, target_status, acting_employee, comment):
        self.ensure_one()
        if not acting_employee and not comment:
            return False
        has_personal_token = bool(acting_employee and acting_employee.sudo().maintainx_api_token)
        parts = [f"Status changed to {target_status}."]
        if acting_employee and not has_personal_token:
            parts.append(f"By: {acting_employee.name}")
        if comment:
            parts.append(comment.strip())
        return self._post_maintainx_comment(" ".join(parts), author_employee=acting_employee)

    def _post_initiator_comment(self, acting_employee=False):
        self.ensure_one()
        root = self.parent_id or self
        requester = root.operator_session_id
        operator = requester.employee_id
        supervisor = root.supervisor_session_id.employee_id
        lines = []
        if operator:
            lines.append(f"Reported by {requester.role or 'operator'}: {operator.name}")
        if supervisor:
            lines.append(f"Escalated by supervisor: {supervisor.name}")
        if not lines and acting_employee:
            lines.append(f"Requested by: {acting_employee.name}")
        if not lines:
            return False
        return self._post_maintainx_comment("\n".join(lines),
                                            author_employee=acting_employee or supervisor)

    def _post_maintainx_comment(self, content, author_employee=False):
        self.ensure_one()
        if not self.maintainx_id or not content:
            return False
        company = self.machine_id.company_id or self.company_id or self.env.company
        if not company.maintainx_enabled or not company.maintainx_api_token:
            return False
        try:
            client, is_personal = self._get_api_client_as(company, author_employee)
            posted = client.create_workorder_comment(self.maintainx_id, content)
        except Exception:
            _logger.exception("MaintainX comment push failed for task %s", self.id)
            return False
        if posted:
            _logger.info("MaintainX comment posted on work order %s (task %s)",
                         self.maintainx_id, self.id)
        else:
            _logger.error("MaintainX comment rejected for work order %s (task %s)",
                          self.maintainx_id, self.id)
        return bool(posted)

    MX_ASSIGNEE_TYPE_USER = 'USER'

    def _build_mx_assignee(self, employee):
        if not employee:
            return None
        raw_id = (employee.maintainx_id or '').strip() if employee.maintainx_id else ''
        if not raw_id:
            return None
        try:
            assignee_id = int(raw_id)
        except (TypeError, ValueError):
            _logger.warning("MaintainX assignee skipped: employee %s has a non numeric MaintainX ID %r",
                            employee.name, raw_id)
            return None
        return {'type': self.MX_ASSIGNEE_TYPE_USER, 'id': assignee_id}

    def _mx_asset_id(self):
        self.ensure_one()
        part_line = self._resolve_part_line()
        raw_asset_id = part_line.maintainx_id if part_line else self.machine_id.maintainx_id
        try:
            return int(raw_asset_id)
        except (TypeError, ValueError):
            if part_line:
                _logger.warning("Part %s on %s has a non numeric MaintainX id %r, "
                                "falling back to the machine asset",
                                part_line.part_id.name, self.machine_id.name, raw_asset_id)
                try:
                    return int(self.machine_id.maintainx_id)
                except (TypeError, ValueError):
                    return None
            return None

    def _resolve_part_line(self):
        self.ensure_one()
        if not self.part_id or not self.machine_id:
            return False
        return self.env['mes.workcenter.part'].sudo().search([
            ('workcenter_id', '=', self.machine_id.id),
            ('part_id', '=', self.part_id.id),
        ], limit=1)

    def _mx_priority_value(self):
        self.ensure_one()
        if self.priority == '3':
            return 'HIGH'
        reverse_priority = {value: key for key, value in self._PRIORITY_MAPPING.items()}
        return reverse_priority.get(self.priority, 'NONE')

    reason_contribution_ids = fields.One2many(
        'mes.task.reason.contribution', 'task_id', string='Reason Contributions')

    def _record_reason_contribution(self, role_label, employee, reason_label, comment):
        self.ensure_one()
        role_map = {'Operator': 'operator', 'Supervisor': 'supervisor', 'Technician': 'technician'}
        role = role_map.get(role_label)
        if not role:
            return False
        self.env['mes.task.reason.contribution'].sudo().create({
            'task_id': self.id,
            'role': role,
            'employee_id': employee.id if employee else False,
            'reason_label': reason_label or '',
            'comment': comment or '',
        })
        self._apply_composed_description()
        for subtask in self._maintainx_linked_subtasks():
            if subtask != self:
                subtask._apply_composed_description()
        return True

    def _apply_composed_description(self):
        self.ensure_one()
        composed = self._build_maintainx_description()
        if composed and composed != (self.description or ''):
            self.sudo().with_context(skip_auto_status_history=True,
                                     skip_maintainx_push=True).write({'description': composed})
        return self._sync_maintainx_description()

    def _sync_maintainx_description(self):
        self.ensure_one()
        if not self.maintainx_id:
            return False
        company = self.machine_id.company_id or self.company_id or self.env.company
        if not company.maintainx_enabled or not company.maintainx_api_token:
            return False
        try:
            client = self._get_api_client(company)
            updated = client.update_workorder_description(
                self.maintainx_id, self._build_maintainx_description())
        except Exception:
            _logger.exception("MaintainX description push failed for task %s", self.id)
            return False
        return bool(updated)

    def _build_maintainx_description(self):
        self.ensure_one()
        root = self.parent_id or self
        lines = [c.format_line() for c in root.reason_contribution_ids]
        if lines:
            return "\n".join(lines)

        parts = []
        reason_label = self._format_reason_label()
        if reason_label and reason_label != '—':
            parts.append(reason_label)
        if self.operator_description and self.operator_description != reason_label:
            parts.append(self.operator_description)
        return "\n".join(p for p in parts if p)

    def _sync_state_to_maintainx(self, acting_employee=False, comment=None):
        self.ensure_one()
        if not self.maintainx_id:
            return False
        if self.env.context.get('skip_maintainx_push'):
            return False
        target_status = self._STATE_TO_MX.get(self.state)
        if not target_status:
            return False
        company = self.machine_id.company_id or self.company_id or self.env.company
        if not company.maintainx_enabled or not company.maintainx_api_token:
            return False
        try:
            client, _is_personal = self._get_api_client_as(company, acting_employee)
            updated = client.update_workorder_status(self.maintainx_id, target_status)
        except Exception:
            _logger.exception("MaintainX status push failed for task %s", self.id)
            return False
        if updated:
            _logger.info("MaintainX work order %s status set to %s for task %s",
                         self.maintainx_id, target_status, self.id)
            self._post_status_change_comment(target_status, acting_employee, comment)
            self.sudo().with_context(skip_auto_status_history=True).write({
                'maintainx_updated_at': self._parse_date(updated.get('updatedAt')),
                'maintainx_data_hash': self._calculate_hash(updated),
            })
        else:
            _logger.error("MaintainX status push to %s failed for work order %s (task %s)",
                          target_status, self.maintainx_id, self.id)
        return bool(updated)

    def _next_subtask_name(self):
        self.ensure_one()
        sequence_number = len(self.child_ids) + 1
        return self.name if sequence_number == 1 else f"{self.name} {sequence_number}"

    def _inherit_fields_for_subtask(self, overrides):
        self.ensure_one()
        base = {
            'name': self._next_subtask_name(),
            'task_type': 'kiosk_help',
            'state': self.TECH_STATE_REQUESTED,
            'machine_id': self.machine_id.id,
            'parent_id': self.id,
            'reason_source': overrides.get('reason_source') or self.reason_source,
            'reason_event_id': overrides.get('reason_event_id') or self.reason_event_id.id,
            'reason_count_id': overrides.get('reason_count_id') or self.reason_count_id.id,
            'reason_process_id': overrides.get('reason_process_id') or self.reason_process_id.id,
            'reason_defect_id': overrides.get('reason_defect_id') or self.reason_defect_id.id,
            'operator_description': overrides.get('description') or self.operator_description,
            'reason_snapshot': self.reason_snapshot,
            'priority': self.priority,
            'part_id': overrides.get('part_id') or self.part_id.id,
            'piece_ids': [(6, 0, overrides.get('piece_ids') or self.piece_ids.ids)],
        }
        return base

    def _notify_technicians_new_request(self):
        self.ensure_one()
        group = self.env.ref('mes_core.group_mes_technician', raise_if_not_found=False)
        if group:
            self.env['bus.bus']._sendone(
                'mes.help.technician',
                'help_request_created',
                {'task_id': self.id, 'workcenter_id': self.machine_id.id},
            )
            for tech in group.users:
                self.activity_schedule(
                    'mail.mail_activity_data_todo',
                    user_id=tech.id,
                    summary=f"Tech request from {self.machine_id.name}",
                    note=self.operator_description or '',
                )

    def action_technician_arrive(self, technician_session_id):
        self.ensure_one()
        if self.state != self.TECH_STATE_REQUESTED:
            raise UserError("Subtask must be in 'Technician Requested' state.")
        session = self.env['mes.kiosk.session'].sudo().browse(technician_session_id)
        if not session.exists() or not session.is_active or session.role != 'technician':
            raise UserError("Invalid technician session.")
        if session.workcenter_id != self.machine_id:
            raise UserError("Technician must scan at the requesting machine.")
        self.with_context(
            mes_acting_employee_id=session.employee_id.id if session.employee_id else False,
        ).write({
            'state': self.TECH_STATE_IN_PROGRESS,
            'technician_session_id': session.id,
            'technician_arrived_at': fields.Datetime.now(),
            'assigned_id': session.employee_id.id if session.employee_id else False,
        })
        self._assign_technician_in_maintainx(session.employee_id)
        self._sync_state_to_maintainx(acting_employee=session.employee_id)
        self._broadcast_state()

    def action_technician_pause(self, comment=None, reason_overrides=None):
        self._technician_transition(self.TECH_STATE_PAUSED, comment, reason_overrides)

    def action_technician_resume(self, comment=None, reason_overrides=None):
        self.ensure_one()
        if self.state != self.TECH_STATE_PAUSED:
            raise UserError(_("Only a paused request can be resumed."))
        resuming_employee = self.technician_session_id.employee_id
        if resuming_employee and self.assigned_id != resuming_employee:
            self.sudo().with_context(skip_auto_status_history=True).write({
                'assigned_id': resuming_employee.id,
            })
        self._assign_technician_in_maintainx(resuming_employee)
        self._technician_transition(self.TECH_STATE_IN_PROGRESS, comment, reason_overrides,
                                    comment_required=False)

    def action_technician_reject(self, comment=None, reason_overrides=None):
        self._technician_transition(self.TECH_STATE_REJECTED, comment, reason_overrides)

    def action_technician_close(self, comment=None, reason_overrides=None):
        self._technician_transition(self.TECH_STATE_CLOSED, comment, reason_overrides)
        if not self._is_supervisor_confirmation_required() and self.parent_id:
            self._hand_over_to_operator(self.technician_session_id.employee_id)

    def _is_supervisor_confirmation_required(self):
        self.ensure_one()
        company = self.machine_id.company_id or self.env.company
        return company.mes_supervisor_confirmation_required

    _TECH_TRANSITIONABLE_STATES = ('new', 'assigned', 'on_hold')

    def _sync_active_technician_session(self):
        self.ensure_one()
        active = self.env['mes.kiosk.session'].sudo().search([
            ('workcenter_id', '=', self.machine_id.id),
            ('role', '=', 'technician'),
            ('logout_at', '=', False),
        ], order='login_at desc', limit=1)
        if active and active != self.technician_session_id:
            self._handover_technician(active)
        return self.technician_session_id

    def _technician_transition(self, target_state, comment, reason_overrides, comment_required=True):
        self.ensure_one()
        self._sync_active_technician_session()
        if self.state not in self._TECH_TRANSITIONABLE_STATES:
            raise UserError(_("Technician transitions allowed only from requested, in-progress or paused states."))
        comment = (comment or '').strip()
        if comment_required and not comment:
            raise UserError(_("A comment is required for pause, reject and close."))
        vals = {'state': target_state}
        if reason_overrides:
            for fld in ('reason_event_id', 'reason_count_id', 'reason_process_id', 'reason_defect_id', 'reason_source'):
                if fld in reason_overrides:
                    vals[fld] = reason_overrides[fld]
        tech_emp = self.technician_session_id.employee_id
        ctx = {
            'mes_acting_employee_id': tech_emp.id if tech_emp else False,
            'mes_status_comment': comment,
        }
        self.with_context(**ctx).write(vals)
        self._sync_state_to_maintainx(acting_employee=tech_emp, comment=comment)
        author = tech_emp.name if tech_emp else self.env.user.name
        state_label = dict(self._fields['state'].selection).get(target_state, target_state)
        self.message_post(body=f"<b>{author}:</b> {comment}" if comment
                          else f"<b>{author}:</b> {state_label}")

        if target_state == self.TECH_STATE_REJECTED and self.parent_id:
            self.parent_id.message_post(body=f"<b>{author} (reject):</b> {comment}")
        self._broadcast_state()

    _REASON_EDITABLE_STATES = (
        'help_open', 'help_supervisor_accepted', 'help_tech_requested',
        'new', 'assigned', 'on_hold',
    )

    def action_update_reason(self, payload, actor_role):
        self.ensure_one()
        root = self.parent_id or self
        if root.state not in self._REASON_EDITABLE_STATES:
            raise UserError(_("The reason can no longer be changed in state %s.",
                              dict(self._fields['state'].selection).get(root.state)))

        comment = (payload.get('comment') or '').strip()
        if not comment:
            raise UserError(_("A comment is required to change the category or reason."))

        if actor_role == 'technician':
            subtask = root.child_ids.filtered(
                lambda t: t.state in self._TECH_TRANSITIONABLE_STATES)[:1]
            actor_emp = (subtask.technician_session_id.employee_id
                         if subtask and subtask.technician_session_id else False)
            role_label = 'Technician'
        else:
            actor_emp = root.supervisor_session_id.employee_id if root.supervisor_session_id else False
            role_label = 'Supervisor'

        area = payload.get('problem_area') or root.problem_area or 'manual'
        if area not in ('alarm', 'rejection', 'defect', 'manual'):
            raise UserError(_("Unknown problem area: %s", area))
        reason_event = self.env['mes.event'].sudo().browse(
            payload.get('reason_event_id') or payload.get('reason_manual_event_id') or 0)
        reason_count = self.env['mes.counts'].sudo().browse(payload.get('reason_count_id') or 0)
        reason_defect = self.env['mes.defect'].sudo().browse(payload.get('reason_defect_id') or 0)
        description = payload.get('operator_description')

        if area == 'alarm' and not reason_event.exists():
            raise UserError(_("Select a stoppage reason for the Alarm area."))
        if area == 'rejection' and not reason_count.exists():
            raise UserError(_("Select a reject count for the Rejection area."))
        if area == 'defect' and not reason_defect.exists():
            raise UserError(_("Select a defect type for the Defect area."))
        if area == 'manual' and not (description or root.operator_description or '').strip():
            raise UserError(_("Describe the problem for the Manual area."))

        area_sel = dict(self._fields['problem_area'].selection)
        old_summary = f"{area_sel.get(root.problem_area, root.problem_area)} / {root._format_reason_label()}"

        vals = {
            'problem_area': area,
            'reason_source': 'manual',
            'reason_event_id': reason_event.id if (area in ('alarm', 'manual')
                                                    and reason_event) else False,
            'reason_count_id': reason_count.id if (area == 'rejection' and reason_count) else False,
            'reason_defect_id': reason_defect.id if (area == 'defect' and reason_defect) else False,
        }
        if 'part_id' in payload:
            part_id = self._safe_part_id(root.machine_id, payload.get('part_id'))
            vals['part_id'] = part_id
            vals['piece_ids'] = [(6, 0, self._safe_piece_ids(payload.get('part_id'),
                                                            payload.get('piece_ids')))]
        if description is not None:
            vals['operator_description'] = description
        if area == 'alarm' and reason_event:
            vals['priority'] = reason_event.priority

        live_sub = root.child_ids.filtered(lambda t: t.state in self._TECH_TRANSITIONABLE_STATES)
        targets = live_sub if role_label == 'Technician' else (root | live_sub)
        targets.sudo().write(vals)

        root._record_reason_contribution(role_label, actor_emp,
                                         root._format_reason_label() if role_label != 'Technician'
                                         else (live_sub[:1] or root)._format_reason_label(),
                                         comment)

        reference = root if role_label != 'Technician' else (live_sub[:1] or root)
        new_summary = f"{area_sel.get(area, area)} / {reference._format_reason_label()}"
        History = self.env['mes.task.status.history'].sudo()
        History.create({
            'task_id': root.id,
            'status': f"Reason updated ({role_label})",
            'change_date': fields.Datetime.now(),
            'source': 'odoo',
            'user_id': self.env.user.id,
            'employee_id': actor_emp.id if actor_emp else False,
            'comment': f"{old_summary} → {new_summary}. {comment}",
        })
        author = actor_emp.name if actor_emp else self.env.user.name
        root.message_post(body=f"<b>{author} ({role_label}, reason change):</b> "
                               f"{old_summary} → {new_summary}<br/>{comment}")
        root._broadcast_state()
        return True

    def action_supervisor_confirm_subtask(self):
        self.ensure_one()
        if self.state != self.TECH_STATE_CLOSED:
            raise UserError("Only a technician-closed subtask can be confirmed.")
        if not self.parent_id:
            raise UserError("This is not a subtask.")
        sup_emp = self.parent_id.supervisor_session_id.employee_id
        self._hand_over_to_operator(sup_emp)

    def _hand_over_to_operator(self, acting_employee):
        self.ensure_one()
        now = fields.Datetime.now()
        ctx = {'mes_acting_employee_id': acting_employee.id if acting_employee else False}
        delay_min = self.parent_id.machine_id.operator_feedback_delay or 0
        self.with_context(**ctx).write({'state': 'help_supervisor_confirmed'})
        self.parent_id.with_context(**ctx).write({
            'state': 'help_awaiting_operator',
            'closed_at': now,
            'operator_confirmation_state': 'pending',
            'confirmation_prompt_due_at': fields.Datetime.add(now, minutes=delay_min),
        })
        self._broadcast_state()
        self.parent_id._broadcast_state()

    def action_operator_confirm(self, operator_session_id, confirmed, comment=None):
        self.ensure_one()
        if self.state != 'help_awaiting_operator':
            raise UserError(_("Confirmation is only available for tasks awaiting operator."))
        if not self._is_operator_prompt_ready():
            raise UserError(_("The confirmation prompt is not available yet for this request."))
        session = self.env['mes.kiosk.session'].sudo().browse(int(operator_session_id))
        if not session.exists():
            raise UserError(_("Operator session not found."))
        if session.logout_at:
            raise UserError(_("Operator session has been closed. Please log in again."))
        if session.role != 'operator':
            raise UserError(_("Confirmation requires an operator session, got '%s'.", session.role))
        if session.workcenter_id.id != self.machine_id.id:
            raise UserError(_("Operator must confirm at the requesting machine (%s).", self.machine_id.name))
        now = fields.Datetime.now()
        author_user = (session.user_id or session.employee_id.user_id or self.env.user).id
        op_ctx = {
            'mes_acting_employee_id': session.employee_id.id if session.employee_id else False,
            'mes_status_comment': comment or False,
        }
        if confirmed:
            self.with_context(**op_ctx).write({
                'state': 'help_completed',
                'operator_confirmation_state': 'confirmed',
                'operator_confirmation_comment': comment or '',
                'operator_confirmed_at': now,
            })
        else:
            self.with_context(**op_ctx).write({
                'state': 'help_open',
                'operator_confirmation_state': 'not_done',
                'operator_confirmation_comment': comment or '',
                'operator_confirmed_at': now,
                'reopened_at': now,
                'reopened_by_id': author_user,
                'supervisor_session_id': False,
                'supervisor_accepted_at': False,
                'technician_session_id': False,
                'technician_arrived_at': False,
                'closed_at': False,
                'confirmation_prompt_due_at': False,
            })
            self._notify_supervisors_new_request()
        self._push_operator_verdict_to_maintainx(session.employee_id, confirmed, comment)
        self._broadcast_state()

    def _push_operator_verdict_to_maintainx(self, operator_employee, confirmed, comment):
        self.ensure_one()
        company = self.machine_id.company_id or self.company_id or self.env.company
        author = company.maintainx_comment_author_id or operator_employee
        posts_personally = bool(author and author.sudo().maintainx_api_token)

        parts = ["Operator proved." if confirmed else "Operator unproved."]
        if comment:
            parts.append(comment.strip())
        if operator_employee and not (posts_personally and author == operator_employee):
            parts.append(f"Operator: {operator_employee.name}")
        content = " ".join(part for part in parts if part)

        for subtask in self._maintainx_linked_subtasks():
            subtask._post_maintainx_comment(content, author_employee=author)

    def _maintainx_linked_subtasks(self):
        self.ensure_one()
        if self.maintainx_id:
            return self
        return self.child_ids.filtered('maintainx_id').sorted('id')

    def _last_closed_subtask_summary(self):
        self.ensure_one()
        closed = self.child_ids.filtered(
            lambda t: t.state == self.TECH_STATE_CLOSED
        ).sorted('write_date')[-1:]
        if not closed:
            return None
        subtask = closed._refresh_from_maintainx()
        return {
            'id': subtask.id,
            'name': subtask.name,
            'description': (subtask.description or subtask.operator_description or '').strip(),
            'technician': subtask.assigned_id.name or '',
        }

    def _refresh_from_maintainx(self):
        self.ensure_one()
        if not self.maintainx_id:
            return self
        company = self.machine_id.company_id or self.company_id or self.env.company
        if not company.maintainx_enabled or not company.maintainx_api_token:
            return self
        try:
            client = self._get_api_client(company)
            remote = client.get_workorder(self.maintainx_id)
        except Exception:
            _logger.exception("MaintainX refresh failed for task %s", self.id)
            return self
        if not remote:
            return self
        description = (remote.get('description') or '').strip()
        if description and description != (self.description or '').strip():
            self.sudo().with_context(skip_auto_status_history=True,
                                     skip_maintainx_push=True).write({
                'description': description,
                'operator_description': description,
            })
            _logger.info("MaintainX description refreshed for task %s before operator prompt", self.id)
        return self

    def _is_operator_prompt_ready(self):
        self.ensure_one()
        if self.state != 'help_awaiting_operator':
            return False
        if not self.confirmation_prompt_due_at:
            return True
        return fields.Datetime.now() >= self.confirmation_prompt_due_at

    def _operator_prompt_remaining_sec(self):
        self.ensure_one()
        if self.state != 'help_awaiting_operator' or not self.confirmation_prompt_due_at:
            return 0
        remaining = (self.confirmation_prompt_due_at - fields.Datetime.now()).total_seconds()
        return max(0, int(remaining))

    def _broadcast_state(self):
        self.ensure_one()
        self.env['bus.bus']._sendone(
            f'mes.kiosk.workcenter.{self.machine_id.id}',
            'help_request_state',
            {'task_id': self.id, 'state': self.state},
        )

    @api.model
    def cron_prompt_operator_confirmation(self):
        now = fields.Datetime.now()
        due = self.search([
            ('task_type', '=', 'kiosk_help'),
            ('state', '=', 'help_awaiting_operator'),
            ('operator_confirmation_state', '=', 'pending'),
            ('confirmation_prompt_due_at', '<=', now),
        ])
        for task in due:
            task.env['bus.bus']._sendone(
                f'mes.kiosk.workcenter.{task.machine_id.id}',
                'operator_confirmation_due',
                {'task_id': task.id},
            )
            task.write({'confirmation_prompt_due_at': False})

    def _get_api_client(self, company):
        if not company.maintainx_api_token:
            raise UserError(f"MaintainX Token not configured for company '{company.name}'.")
        return MaintainXClient(company.maintainx_api_token)

    def _get_api_client_as(self, company, employee):
        personal_token = employee.sudo().maintainx_api_token if employee else None
        if personal_token:
            _logger.info("MaintainX request sent under personal account of %s", employee.name)
            return MaintainXClient(personal_token), True
        return self._get_api_client(company), False

    @api.model
    def _parse_date(self, date_str):
        if not date_str:
            return False
        try:
            dt = parser.parse(date_str)
            return dt.astimezone(pytz.UTC).replace(tzinfo=None)
        except (ValueError, TypeError):
            _logger.warning(f"Failed to parse date: {date_str}")
            return False

    def _calculate_hash(self, data):
        significant_data = {
            'id': data.get('id'),
            'updatedAt': data.get('updatedAt'),
            'status': data.get('status'),
        }
        return hashlib.sha256(json.dumps(significant_data, sort_keys=True).encode('utf-8')).hexdigest()

    @api.model
    def load_tasks_from_maintainx(self):
        companies = self.env['res.company'].sudo().search([
            ('maintainx_enabled', '=', True),
            ('maintainx_api_token', '!=', False),
        ])
        for company in companies:
            try:
                self._load_tasks_for_company(company)
            except Exception:
                _logger.exception(f"MaintainX sync failed for company {company.name}")

    def _load_tasks_for_company(self, company):
        client = self._get_api_client(company)
        workorders_list = client.get_workorders(limit=200)
        if not workorders_list:
            return

        mx_ids = [str(wo.get('id')) for wo in workorders_list if wo.get('id')]
        existing_tasks = self.search([
            ('maintainx_id', 'in', mx_ids),
            ('company_id', '=', company.id),
        ])
        existing_map = {task.maintainx_id: task for task in existing_tasks}

        for wo_data in workorders_list:
            wo_id = str(wo_data.get('id'))
            if not wo_id:
                continue

            new_hash = self._calculate_hash(wo_data)
            existing_task = existing_map.get(wo_id)

            if existing_task and existing_task.maintainx_data_hash == new_hash:
                continue

            self.with_delay(
                channel='root.maintainx',
                description=f"Sync MX Task {wo_id} ({company.name})",
                priority=10,
                identity_key=f"mx_sync_{company.id}_{wo_id}",
            ).action_sync_single_wo_job(wo_id, company.id)

    def action_sync_single_wo_job(self, workorder_id, company_id):
        company = self.env['res.company'].sudo().browse(company_id)
        if not company.exists() or not company.maintainx_api_token:
            return
        client = self._get_api_client(company)
        full_wo_data = client.get_workorder(workorder_id)
        if not full_wo_data:
            return

        cache = {'employees': {}, 'machines': {}}
        self._process_single_wo(full_wo_data, client, cache, company)

    def _process_single_wo(self, wo, client, cache, company):
        if not wo.get('id'):
            _logger.error(f"Received WorkOrder data without ID. Data keys: {list(wo.keys())}")
            return

        wo_id = str(wo.get('id'))
        vals = self._prepare_task_values(wo, client, cache, company)

        task = self.search([
            ('maintainx_id', '=', wo_id),
            ('company_id', '=', company.id),
        ], limit=1)

        sync_result = {
            'action': 'none',
            'task_id': False,
            'changes': {}
        }

        if not task:
            task = self.with_company(company).with_context(skip_auto_status_history=True).create(vals)
            self._create_status_history(task, wo.get('status'))
            self._sync_comments_from_maintainx(task, client, cache, company)

            task.message_post(body="Task created from MaintainX sync.")

            sync_result.update({'action': 'created', 'task_id': task.id, 'changes': vals})
            return sync_result

        if 'maintainx_assignees_history' in vals:
            new_history_entry = vals.pop('maintainx_assignees_history', '')
            if new_history_entry:
                current_hist = task.maintainx_assignees_history or ""
                vals['maintainx_assignees_history'] = current_hist + new_history_entry

        changes = self._compute_task_delta(task, vals)

        old_state = task.state
        is_help_subtask = bool(task.task_type == 'kiosk_help' and task.parent_id)
        if is_help_subtask:
            restricted_vals = {
                'maintainx_updated_at': self._parse_date(wo.get('updatedAt')),
                'maintainx_data_hash': self._calculate_hash(wo),
                'state': self._STATUS_MAPPING.get(
                    str(wo.get('status', '')).upper().strip(), task.state),
            }
            incoming_description = (wo.get('description') or '').strip()
            if incoming_description and incoming_description != (task.description or '').strip():
                restricted_vals['description'] = incoming_description
                restricted_vals['operator_description'] = incoming_description
                _logger.info("MaintainX description applied to task %s", task.id)
            if vals.get('assigned_id'):
                restricted_vals['assigned_id'] = vals['assigned_id']
            vals = restricted_vals
        task.with_context(skip_auto_status_history=True, skip_maintainx_push=True).write(vals)

        if changes:
            html_message = self._format_load_message(changes) 
            if html_message:
                task.message_post(body=html_message)

            sync_result.update({'action': 'updated', 'task_id': task.id, 'changes': changes})
        else:
            sync_result.update({'action': 'silent_update', 'task_id': task.id})

        if old_state != vals.get('state'):
            self._create_status_history(task, wo.get('status'))

        self._sync_comments_from_maintainx(task, client, cache, company)

        return sync_result

    def _prepare_task_values(self, wo, client, cache, company):
        mx_status = str(wo.get('status', '')).upper().strip()
        mx_priority = str(wo.get('priority', 'LOW')).upper().strip()
        existing = self.search([
            ('maintainx_id', '=', str(wo.get('id'))),
            ('company_id', '=', company.id),
        ], limit=1)
        is_help_subtask = bool(existing and existing.task_type == 'kiosk_help' and existing.parent_id)

        assignee_ids = wo.get('assigneeIds', [])
        assignee_names = []
        employee_id = False

        for mx_user_id in assignee_ids:
            emp = self._get_or_create_employee(str(mx_user_id), client, cache, company)
            if emp:
                assignee_names.append(emp.name)
                if not employee_id:
                    employee_id = emp.id

        current_assignees_str = ", ".join(assignee_names)
        assignee_history_line = f"{fields.Datetime.now()}: {current_assignees_str}\n"

        machine_id = False
        part_id = False
        if wo.get('assetId'):
            part_line = self._resolve_part_by_asset(wo.get('assetId'), company)
            if part_line:
                part_id = part_line.part_id.id
            machine_id = self._get_machine_recursive(wo.get('assetId'), client, cache, company)

        return {
            'name': wo.get('title', 'No Title'),
            'description': wo.get('description', ''),
            'company_id': company.id,
            'task_type': 'kiosk_help' if is_help_subtask else 'maintainx',
            'maintainx_id': str(wo.get('id')),
            'maintainx_created_at': self._parse_date(wo.get('createdAt')),
            'maintainx_updated_at': self._parse_date(wo.get('updatedAt')),
            'maintainx_data_hash': self._calculate_hash(wo),
            'state': self._STATUS_MAPPING.get(mx_status, 'new'),
            'priority': self._PRIORITY_MAPPING.get(mx_priority, '0'),
            'assigned_id': employee_id,
            'machine_id': machine_id,
            'part_id': part_id,
            'maintainx_assignees_history': assignee_history_line
        }

    def _sync_comments_from_maintainx(self, task, client, cache, company):
        if not task.maintainx_id:
            return 0
        try:
            entries = client.get_workorder_comments(task.maintainx_id)
        except Exception:
            _logger.exception("MaintainX comment fetch failed for task %s", task.id)
            return 0
        if not entries:
            return 0

        _logger.info("MaintainX comment payload sample for task %s: %s",
                     task.id, json.dumps(entries[0], default=str)[:1000])
        try:
            expanded = client.get_workorder_expanded(task.maintainx_id)
            if expanded:
                _logger.info("MaintainX work order %s time/assignee data: times=%s timeItems=%s",
                             task.maintainx_id,
                             json.dumps(expanded.get('times'), default=str)[:600],
                             json.dumps(expanded.get('timeItems'), default=str)[:600])
        except Exception:
            _logger.exception("MaintainX expanded fetch failed for task %s", task.id)

        History = self.env['mes.task.status.history'].sudo()
        known = set(History.search([
            ('task_id', '=', task.id),
            ('source', '=', 'maintainx'),
            ('mx_comment_id', '!=', False),
        ]).mapped('mx_comment_id'))

        created = 0
        for entry in entries:
            comment_id = str(entry.get('id') or '')
            content = (entry.get('content') or entry.get('comment') or '').strip()
            if not comment_id or comment_id in known or not content:
                continue
            author_id = (entry.get('userId') or entry.get('creatorId')
                         or entry.get('authorId') or entry.get('createdById')
                         or (entry.get('user') or {}).get('id')
                         or (entry.get('author') or {}).get('id'))
            employee = self._get_or_create_employee(str(author_id), client, cache, company) if author_id else False
            History.create({
                'task_id': task.id,
                'status': task.state,
                'change_date': self._parse_date(entry.get('createdAt')) or fields.Datetime.now(),
                'source': 'maintainx',
                'employee_id': employee.id if employee else False,
                'comment': content,
                'mx_comment_id': comment_id,
            })
            known.add(comment_id)
            created += 1
        _logger.info("MaintainX comments imported for task %s: %s new of %s fetched",
                     task.id, created, len(entries))
        return created

    def _create_status_history(self, task, raw_status, employee=False, changed_at=None, comment=None):
        self.env['mes.task.status.history'].sudo().create({
            'task_id': task.id,
            'status': str(raw_status).upper(),
            'change_date': changed_at or fields.Datetime.now(),
            'source': 'maintainx',
            'employee_id': employee.id if employee else False,
            'comment': comment or '',
        })

    def _get_or_create_employee(self, mx_user_id, client, cache, company):
        cache_key = f"{company.id}:{mx_user_id}"
        if cache_key in cache['employees']:
            return cache['employees'][cache_key]

        employee = self.env['hr.employee'].search([
            ('maintainx_id', '=', mx_user_id),
            ('company_id', '=', company.id),
        ], limit=1)
        if employee:
            cache['employees'][cache_key] = employee
            return employee

        user_data = client.get_user(mx_user_id)
        if not user_data:
            return False

        first = user_data.get('firstName', '').strip()
        last = user_data.get('lastName', '').strip()
        full_name = f"{first} {last}".strip()

        employee = self.env['hr.employee'].search([
            ('name', 'ilike', full_name),
            ('company_id', '=', company.id),
        ], limit=1)
        if employee:
            employee.write({'maintainx_id': mx_user_id})
        else:
            try:
                employee = self.env['hr.employee'].with_company(company).create({
                    'name': full_name or f"MX User {mx_user_id}",
                    'maintainx_id': mx_user_id,
                    'work_email': user_data.get('email'),
                    'company_id': company.id,
                })
            except Exception as e:
                _logger.error(f"Cannot create employee for {company.name}: {e}")
                return False

        cache['employees'][cache_key] = employee
        return employee

    def _resolve_part_by_asset(self, asset_id, company):
        if not asset_id:
            return self.env['mes.workcenter.part']
        return self.env['mes.workcenter.part'].sudo().search([
            ('maintainx_id', '=', str(asset_id)),
            ('workcenter_id.company_id', '=', company.id),
        ], limit=1)

    def _get_machine_recursive(self, asset_id, client, cache, company, depth=0):
        if not asset_id or depth > 5:
            return False

        cache_key = f"{company.id}:{asset_id}"
        if cache_key in cache['machines']:
            return cache['machines'][cache_key]

        part_line = self._resolve_part_by_asset(asset_id, company)
        if part_line:
            cache['machines'][cache_key] = part_line.workcenter_id.id
            return part_line.workcenter_id.id

        machine = self.env['mrp.workcenter'].search([
            ('maintainx_id', '=', asset_id),
            ('company_id', '=', company.id),
        ], limit=1)
        if machine:
            cache['machines'][cache_key] = machine.id
            return machine.id

        asset_data = client.get_asset(asset_id)
        if not asset_data:
            return False

        parent_id = asset_data.get('parentId')
        if parent_id:
            res = self._get_machine_recursive(parent_id, client, cache, company, depth + 1)
            if res:
                cache['machines'][cache_key] = res
            return res

        return False
    
    def _compute_task_delta(self, task, vals):
        changes = {}
        IGNORED_FIELDS = {
            'maintainx_data_hash', 
            'maintainx_updated_at', 
            'maintainx_assignees_history',
            'maintainx_created_at',
            'maintainx_id'
        }

        for field, new_value in vals.items():
            if field in IGNORED_FIELDS:
                continue
            
            try:
                current_value = task[field]
                
                if hasattr(current_value, 'id'): 
                    current_id = current_value.id or False
                    new_id = new_value or False
                    
                    if current_id != new_id:
                        changes[field] = {
                            'old': current_value.display_name if current_value else 'Empty', 
                            'new': self._get_name_from_id(field, new_value)
                        }
                
                else:
                    c_val = current_value or False
                    n_val = new_value or False
                    
                    if c_val != n_val:
                        changes[field] = {'old': c_val, 'new': n_val}
                        
            except KeyError:
                continue
                
        return changes

    def _get_name_from_id(self, field_name, res_id):
        if not res_id:
            return 'Empty'
            
        model_map = {
            'machine_id': 'mrp.workcenter',
            'assigned_id': 'hr.employee',
            'author_id': 'res.users'
        }
        
        model = model_map.get(field_name)
        if model:
            rec = self.env[model].browse(res_id)
            return rec.name if rec.exists() else f"ID {res_id}"
        return str(res_id)

    def _format_load_message(self, changes):
        if not changes:
            return None
            
        msg_body = "<b>Updated from MaintainX:</b><ul>"
        for f, diff in changes.items():
            field_label = f.replace('_', ' ').capitalize()
            msg_body += f"<li>{field_label}: {diff['old']} &rarr; {diff['new']}</li>"
        msg_body += "</ul>"
        return msg_body