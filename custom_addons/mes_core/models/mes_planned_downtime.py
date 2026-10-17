from odoo import models, fields, api, _
from datetime import datetime, timedelta, time
import pytz

import logging

from odoo.exceptions import UserError, ValidationError

_logger = logging.getLogger(__name__)

WEEKDAY_SELECTION = [
    ('0', 'Monday'),
    ('1', 'Tuesday'),
    ('2', 'Wednesday'),
    ('3', 'Thursday'),
    ('4', 'Friday'),
    ('5', 'Saturday'),
    ('6', 'Sunday'),
]

DAY_END_OF_DAY = 24.0
PM_OPEN_DAYS = 30
PM_EXTEND_BEFORE_DAYS = 7
MANAGER_GROUP = 'mes_core.group_mes_manager'


class MesPlannedDowntime(models.Model):
    _name = 'mes.planned.downtime'
    _description = 'Planned Downtime Rule'

    name = fields.Char(required=True)
    active = fields.Boolean(default=True)

    rule_type = fields.Selection([
        ('one_time', 'One Time'),
        ('weekly', 'Weekly Schedule'),
    ], default='weekly', required=True)

    machine_ids = fields.Many2many('mrp.workcenter', string='Affected Machines')
    event_id = fields.Many2one('mes.event', string='Downtime Event')
    origin = fields.Selection([
        ('manual', 'Manual'),
        ('kiosk', 'Kiosk'),
        ('workcenter', 'Work Center List'),
        ('dashboard', 'Supervisor Dashboard'),
    ], default='manual', required=True)
    open_ended = fields.Boolean(
        string='Until Stopped', readonly=True,
        help="Started from the work center list or the supervisor dashboard: it lasts until someone "
             "stops it or an operator logs in on the machine.")
    started_by = fields.Char(string='Started By', readonly=True)
    started_at = fields.Datetime(string='Started At', readonly=True)
    finished_by = fields.Char(string='Stopped By', readonly=True)
    finished_at = fields.Datetime(string='Stopped At', readonly=True)
    closed_early = fields.Boolean(
        string='Closed Before Shift End', default=False,
        help="Set when a technician finished the kiosk downtime manually, so that it "
             "is not continued into the next shift.")

    date_start = fields.Datetime(string='Start')
    date_end = fields.Datetime(string='End')

    valid_from = fields.Date(string='Active From')
    valid_to = fields.Date(string='Active Until')

    weekly_line_ids = fields.One2many(
        'mes.planned.downtime.line', 'rule_id', string='Weekly Intervals')

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if not vals.get('event_id'):
                vals['event_id'] = self._default_downtime_event_id()
        return super().create(vals_list)

    @api.model
    def _default_downtime_event_id(self):
        return self.env.company.mes_planned_downtime_event_id.id or False

    def action_apply_default_event(self):
        default_event = self._default_downtime_event_id()
        if not default_event:
            return False
        self.filtered(lambda r: not r.event_id).write({'event_id': default_event})
        return True

    @api.constrains('rule_type', 'date_start', 'date_end', 'weekly_line_ids')
    def _check_rule_definition(self):
        for rule in self:
            if rule.rule_type == 'one_time':
                if not rule.date_start or not rule.date_end:
                    raise ValidationError("A one time rule needs both a start and an end.")
                if rule.date_end <= rule.date_start:
                    raise ValidationError("The end must be later than the start.")
            elif not rule.weekly_line_ids:
                raise ValidationError("A weekly rule needs at least one interval.")

    @api.model
    def generate_flat_schedule_for_week(self, days_ahead=14):
        if isinstance(days_ahead, (list, tuple)) or not isinstance(days_ahead, int):
            days_ahead = 14

        flat_model = self.env['mes.flat.downtime']
        now_utc = datetime.utcnow()

        schedule_tz = (self.env.company.tz or self.env.context.get('tz')
                       or self.env.user.tz or 'UTC')
        tz = pytz.timezone(schedule_tz)

        local_now = pytz.utc.localize(now_utc).astimezone(tz)
        start_date = local_now.date() - timedelta(days=1)
        end_date = local_now.date() + timedelta(days=days_ahead)

        vals_list = []
        rules = self if self else self.search([('active', '=', True)])

        for rule in rules:
            if rule.rule_type == 'one_time':
                flat_model.search([('rule_id', '=', rule.id)]).unlink()
                vals_list.extend(rule._build_one_time_slots())
            else:
                window_start_utc = tz.localize(
                    datetime.combine(start_date, time(0, 0))
                ).astimezone(pytz.utc).replace(tzinfo=None)
                flat_model.search([
                    ('rule_id', '=', rule.id),
                    ('end_time', '>', window_start_utc),
                ]).unlink()
                vals_list.extend(rule._build_weekly_slots(tz, start_date, end_date))

        if vals_list:
            flat_model.create(vals_list)

    @api.model
    def _active_pm_slots(self, workcenter):
        now_utc = fields.Datetime.now()
        return self.env['mes.flat.downtime'].sudo().search([
            ('machine_id', '=', workcenter.id),
            ('rule_id.rule_type', '=', 'one_time'),
            ('start_time', '<=', now_utc),
            ('end_time', '>', now_utc),
        ], order='start_time desc')

    @api.model
    def _finish_now(self, slots, who):
        now_utc = fields.Datetime.now()
        pm_slots = slots.filtered(lambda item: item.rule_id.rule_type == 'one_time')
        for slot in pm_slots:
            end = max(now_utc, slot.start_time + timedelta(seconds=1))
            rule = slot.rule_id.sudo()
            if len(rule.machine_ids) > 1:
                rule = self._detach_machine(slot)
            slot.write({'end_time': end})
            rule.write({
                'date_end': end,
                'closed_early': True,
                'open_ended': False,
                'finished_by': who or False,
                'finished_at': now_utc,
            })
        return bool(pm_slots)

    @api.model
    def _detach_machine(self, slot):
        shared = slot.rule_id.sudo()
        own = shared.copy({
            'name': f"{shared.name} ({slot.machine_id.name})",
            'machine_ids': [(6, 0, [slot.machine_id.id])],
        })
        shared.write({'machine_ids': [(3, slot.machine_id.id)]})
        slot.write({'rule_id': own.id})
        return own

    @api.model
    def start_workcenter_pm(self, workcenter, user, origin='workcenter'):
        active = self._active_pm_slots(workcenter)
        if active:
            return active[:1]
        now_utc = fields.Datetime.now().replace(microsecond=0)
        end_utc = now_utc + timedelta(days=PM_OPEN_DAYS)
        company = workcenter.company_id or self.env.company
        rule = self.sudo().create({
            'name': f"PM {workcenter.name} {self._format_local(workcenter, now_utc)} ({user.name})",
            'rule_type': 'one_time',
            'origin': origin,
            'open_ended': True,
            'event_id': company.mes_planned_downtime_event_id.id or False,
            'machine_ids': [(6, 0, [workcenter.id])],
            'date_start': now_utc,
            'date_end': end_utc,
            'started_by': user.name,
            'started_at': now_utc,
        })
        return self.env['mes.flat.downtime'].sudo().create({
            'machine_id': workcenter.id,
            'rule_id': rule.id,
            'start_time': now_utc,
            'end_time': end_utc,
        })

    @api.model
    def get_active_kiosk_downtime(self, workcenter):
        now_utc = fields.Datetime.now()
        return self.env['mes.flat.downtime'].sudo().search([
            ('machine_id', '=', workcenter.id),
            ('rule_id.origin', '=', 'kiosk'),
            ('start_time', '<=', now_utc),
            ('end_time', '>', now_utc),
        ], order='start_time desc', limit=1)

    @api.model
    def start_kiosk_downtime(self, workcenter, employee=False):
        active = self._active_pm_slots(workcenter)[:1]
        if active:
            return active

        now_utc = fields.Datetime.now()
        shift_end_utc = self._current_shift_end_utc(workcenter)
        if not shift_end_utc or shift_end_utc <= now_utc:
            raise UserError("The current shift is already over, cannot plan a downtime.")

        reopened = self._reopen_shift_downtime(workcenter, shift_end_utc)
        if reopened:
            return reopened

        author = employee.name if employee else self.env.user.name
        local_label = self._format_local(workcenter, now_utc)
        rule = self.sudo().create({
            'name': f"Kiosk downtime {workcenter.name} {local_label} ({author})",
            'rule_type': 'one_time',
            'origin': 'kiosk',
            'machine_ids': [(6, 0, [workcenter.id])],
            'date_start': now_utc,
            'date_end': shift_end_utc,
            'started_by': author,
            'started_at': now_utc,
        })
        return self.env['mes.flat.downtime'].sudo().create({
            'machine_id': workcenter.id,
            'rule_id': rule.id,
            'start_time': now_utc,
            'end_time': shift_end_utc,
        })

    @api.model
    def _reopen_shift_downtime(self, workcenter, shift_end_utc):
        shift_start_utc = self._current_shift_start_utc(workcenter)
        if not shift_start_utc:
            return False
        existing = self.env['mes.flat.downtime'].sudo().search([
            ('machine_id', '=', workcenter.id),
            ('rule_id.origin', '=', 'kiosk'),
            ('start_time', '>=', shift_start_utc),
            ('start_time', '<', shift_end_utc),
        ], order='start_time asc', limit=1)
        if not existing:
            return False
        existing.write({'end_time': shift_end_utc})
        existing.rule_id.sudo().write({'date_end': shift_end_utc, 'closed_early': False,
                                       'finished_by': False, 'finished_at': False})
        _logger.info("Kiosk downtime %s extended to the end of the shift for %s",
                     existing.id, workcenter.name)
        return existing

    @api.model
    def finish_on_operator_login(self, workcenter, who=False):
        return self._finish_now(self._active_pm_slots(workcenter), who)

    @api.model
    def _short_local(self, workcenter, moment_utc):
        if not moment_utc:
            return ''
        mac_tz = pytz.timezone(workcenter.company_id.tz or self.env.user.tz or 'UTC')
        local = pytz.utc.localize(moment_utc).astimezone(mac_tz)
        today = pytz.utc.localize(fields.Datetime.now()).astimezone(mac_tz).date()
        return local.strftime('%H:%M') if local.date() == today else local.strftime('%d/%m %H:%M')

    @api.model
    def _format_local(self, workcenter, moment_utc):
        mac_tz = pytz.timezone(workcenter.company_id.tz or self.env.user.tz or 'UTC')
        local = pytz.utc.localize(moment_utc).astimezone(mac_tz)
        return local.strftime('%Y-%m-%d %H:%M')

    @api.model
    def _current_shift_end_utc(self, workcenter):
        return self._current_shift_bound_utc(workcenter, 'end')

    @api.model
    def _current_shift_start_utc(self, workcenter):
        return self._current_shift_bound_utc(workcenter, 'start')

    @api.model
    def _current_shift_bound_utc(self, workcenter, bound):
        Shift = self.env['mes.shift'].sudo()
        s_loc, e_loc = Shift.get_current_shift_window(workcenter)
        target = s_loc if bound == 'start' else e_loc
        if not target:
            return None
        mac_tz = pytz.timezone(workcenter.company_id.tz or 'UTC')
        return mac_tz.localize(target, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)

    @api.model
    def extend_open_ended_pm(self):
        now_utc = fields.Datetime.now().replace(microsecond=0)
        rules = self.sudo().search([
            ('open_ended', '=', True),
            ('rule_type', '=', 'one_time'),
            ('date_end', '>', now_utc),
            ('date_end', '<', now_utc + timedelta(days=PM_EXTEND_BEFORE_DAYS)),
        ])
        new_end = now_utc + timedelta(days=PM_OPEN_DAYS)
        Flat = self.env['mes.flat.downtime'].sudo()
        for rule in rules:
            Flat.search([('rule_id', '=', rule.id), ('end_time', '>', now_utc)]).write({'end_time': new_end})
            rule.write({'date_end': new_end})
        return len(rules)

    @api.model
    def roll_over_kiosk_downtime(self):
        self.extend_open_ended_pm()
        Flat = self.env['mes.flat.downtime'].sudo()
        now_utc = fields.Datetime.now()
        Session = self.env['mes.kiosk.session'].sudo()

        expired = Flat.search([
            ('rule_id.origin', '=', 'kiosk'),
            ('end_time', '<=', now_utc),
            ('end_time', '>', fields.Datetime.subtract(now_utc, minutes=30)),
            ('rule_id.closed_early', '=', False),
        ])
        for slot in expired:
            workcenter = slot.machine_id
            if self.get_active_kiosk_downtime(workcenter):
                continue
            operator = Session.search([
                ('workcenter_id', '=', workcenter.id),
                ('role', '=', 'operator'),
                ('logout_at', '=', False),
            ], limit=1)
            if operator:
                continue
            shift_end_utc = self._current_shift_end_utc(workcenter)
            if not shift_end_utc or shift_end_utc <= slot.end_time:
                continue
            rule = self.sudo().create({
                'name': f"Kiosk downtime {workcenter.name} continued "
                        f"{self._format_local(workcenter, slot.end_time)}",
                'rule_type': 'one_time',
                'origin': 'kiosk',
                'started_by': slot.rule_id.started_by,
                'started_at': slot.rule_id.started_at,
                'event_id': slot.rule_id.event_id.id,
                'machine_ids': [(6, 0, [workcenter.id])],
                'date_start': slot.end_time,
                'date_end': shift_end_utc,
            })
            Flat.create({
                'machine_id': workcenter.id,
                'rule_id': rule.id,
                'start_time': slot.end_time,
                'end_time': shift_end_utc,
            })
            _logger.info("Kiosk downtime rolled over into the new shift for %s", workcenter.name)

    def _build_one_time_slots(self):
        self.ensure_one()
        if not self.date_start or not self.date_end:
            return []
        return [{
            'machine_id': machine.id,
            'rule_id': self.id,
            'start_time': self.date_start,
            'end_time': self.date_end,
        } for machine in self.machine_ids]

    def _build_weekly_slots(self, tz, start_date, end_date):
        self.ensure_one()
        slots = []
        for offset in range((end_date - start_date).days + 1):
            target_date = start_date + timedelta(days=offset)
            if self.valid_from and target_date < self.valid_from:
                continue
            if self.valid_to and target_date > self.valid_to:
                continue

            for line in self.weekly_line_ids:
                if int(line.weekday) != target_date.weekday():
                    continue
                local_start, local_end = line._local_bounds(target_date)
                utc_start = tz.localize(local_start).astimezone(pytz.utc).replace(tzinfo=None)
                utc_end = tz.localize(local_end).astimezone(pytz.utc).replace(tzinfo=None)
                slots.extend({
                    'machine_id': machine.id,
                    'rule_id': self.id,
                    'start_time': utc_start,
                    'end_time': utc_end,
                } for machine in self.machine_ids)
        return slots


class MesPlannedDowntimeLine(models.Model):
    _name = 'mes.planned.downtime.line'
    _description = 'Planned Downtime Weekly Interval'
    _order = 'weekday, hour_from'

    rule_id = fields.Many2one('mes.planned.downtime', required=True, ondelete='cascade')
    weekday = fields.Selection(WEEKDAY_SELECTION, string='Day', required=True, default='0')
    hour_from = fields.Float(string='From', default=0.0)
    hour_to = fields.Float(string='To', default=DAY_END_OF_DAY)

    @api.constrains('hour_from', 'hour_to')
    def _check_hours(self):
        for line in self:
            if not (0.0 <= line.hour_from < DAY_END_OF_DAY):
                raise ValidationError("Start time must be between 00:00 and 23:59.")
            if not (0.0 < line.hour_to <= DAY_END_OF_DAY):
                raise ValidationError("End time must be between 00:01 and 24:00.")
            if line.hour_to <= line.hour_from:
                raise ValidationError("The end time must be later than the start time.")

    def _local_bounds(self, target_date):
        self.ensure_one()
        start = datetime.combine(target_date, self._float_to_time(self.hour_from))
        if self.hour_to >= DAY_END_OF_DAY:
            end = datetime.combine(target_date + timedelta(days=1), time(0, 0))
        else:
            end = datetime.combine(target_date, self._float_to_time(self.hour_to))
        return start, end

    @api.model
    def _float_to_time(self, value):
        hours = int(value)
        minutes = int(round((value - hours) * 60))
        if minutes == 60:
            hours, minutes = hours + 1, 0
        return time(min(hours, 23), minutes)


class MesFlatDowntime(models.Model):
    _name = 'mes.flat.downtime'
    _description = 'Generated Downtime Schedule'
    _order = 'start_time asc'

    machine_id = fields.Many2one('mrp.workcenter', required=True, ondelete='cascade', string='Workcenter')
    rule_id = fields.Many2one('mes.planned.downtime', required=True, ondelete='cascade', string='Downtime Rule')
    event_id = fields.Many2one('mes.event', related='rule_id.event_id', store=True, string='Downtime Event')
    start_time = fields.Datetime(required=True)
    end_time = fields.Datetime(required=True)

    duration = fields.Float(compute='_compute_duration', string='Duration (Hours)')

    @api.depends('start_time', 'end_time')
    def _compute_duration(self):
        for rec in self:
            if rec.start_time and rec.end_time:
                delta = rec.end_time - rec.start_time
                rec.duration = delta.total_seconds() / 3600.0
            else:
                rec.duration = 0.0

    def action_delete_from_wizard(self):
        wizard_id = self.env.context.get('active_id')
        wizard_model = self.env.context.get('active_model')
        
        for rec in self:
            rec.unlink()
            
        if wizard_id and wizard_model:
            return {
                'type': 'ir.actions.act_window',
                'res_model': wizard_model,
                'res_id': wizard_id,
                'view_mode': 'form',
                'target': 'new',
            }
            
        return {'type': 'ir.actions.client', 'tag': 'reload'}


class MrpWorkcenter(models.Model):
    _inherit = 'mrp.workcenter'

    pm_active = fields.Boolean(
        string='PM', compute='_compute_pm_active', inverse='_inverse_pm_active',
        help="On while a one-time planned downtime (PM) runs on the machine; the weekly downtime schedule "
             "is not shown here. Managers can switch it in the work center list or the supervisor "
             "dashboard: on starts PM now until it is switched off or an operator logs in, also on top of "
             "scheduled downtime; off ends it now and leaves scheduled downtime as it is.")

    def _current_pm_slots(self):
        now_utc = fields.Datetime.now()
        return self.env['mes.flat.downtime'].sudo().search([
            ('machine_id', 'in', [workcenter.id for workcenter in self if isinstance(workcenter.id, int)]),
            ('rule_id.rule_type', '=', 'one_time'),
            ('start_time', '<=', now_utc),
            ('end_time', '>', now_utc),
        ])

    def _compute_pm_active(self):
        active = set(self._current_pm_slots().mapped('machine_id').ids)
        for workcenter in self:
            workcenter.pm_active = workcenter.id in active

    def _inverse_pm_active(self):
        if not self.env.user.has_group(MANAGER_GROUP):
            raise UserError(_("Only MES managers can start or stop planned downtime here."))
        Downtime = self.env['mes.planned.downtime'].sudo()
        for workcenter in self:
            slots = Downtime._active_pm_slots(workcenter)
            if workcenter.pm_active and not slots:
                Downtime.start_workcenter_pm(workcenter, self.env.user)
            elif not workcenter.pm_active and slots:
                Downtime._finish_now(slots, self.env.user.name)
