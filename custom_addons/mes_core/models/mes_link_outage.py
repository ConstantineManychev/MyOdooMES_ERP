import logging
import re
from collections import namedtuple
from datetime import datetime, timedelta

import pytz
from markupsafe import Markup, escape

from odoo import models, fields, api, _

_logger = logging.getLogger(__name__)

DEFAULT_LOSS_MINUTES = 10
MIN_LOSS_MINUTES = 3
MISSED_RUN = timedelta(seconds=150)
EPOCH = datetime(1970, 1, 1)
PULSE_BREAK = timedelta(seconds=90)
PULSE_FRESH = timedelta(seconds=90)
DATA_MARGIN = timedelta(seconds=60)
PLANNED_GRACE = timedelta(minutes=5)
CUT_REPORT_WINDOW = timedelta(minutes=15)
CUT_LOOKBACK = timedelta(days=1)
CHECK_GIVE_UP = timedelta(hours=2)
WATCH_SINCE_PARAM = 'mes_core.link_check_watch_since'
EMAIL_SPLIT = re.compile(r'[\s,;]+')
TELEMETRY_TABLES = ('telemetry_count', 'telemetry_event', 'telemetry_process')
CLOSED_STATES = ('checking', 'recovered', 'lost', 'planned')
GAP_STATES = ('ongoing', 'checking', 'lost')

LinkSignals = namedtuple('LinkSignals', 'pulse pulse_since ads_ok ads_down_since ads_up_since')


def parse_emails(text):
    return [part for part in EMAIL_SPLIT.split(text or '') if '@' in part]


def as_seconds(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return int(round(number)) if number >= 0 else None


def pulse_values(previous_pulse, previous_since, payload, now):
    values = {'log_pulse_dt': now, 'log_ads_ok': bool(payload.get('ads_ok'))}
    if not previous_pulse or not previous_since or now - previous_pulse > PULSE_BREAK:
        values['log_pulse_since_dt'] = now
    if values['log_ads_ok']:
        values['log_ads_ok_dt'] = now
        values['log_ads_down_since_dt'] = False
        values['log_ads_up_since_dt'] = now - timedelta(seconds=as_seconds(payload.get('ads_up_sec')) or 0)
    else:
        values['log_ads_up_since_dt'] = False
        values['log_ads_down_since_dt'] = now - timedelta(seconds=as_seconds(payload.get('ads_down_sec')) or 0)
    backlog = as_seconds(payload.get('backlog'))
    if backlog is not None:
        values['log_backlog'] = backlog
        age = as_seconds(payload.get('backlog_age_sec'))
        values['log_backlog_from_dt'] = now - timedelta(seconds=age) if backlog and age is not None else False
    try:
        values['log_clock_skew_sec'] = int(round(float(payload['clock']) - (now - EPOCH).total_seconds()))
    except (KeyError, TypeError, ValueError):
        pass
    cut_ago = as_seconds(payload.get('cut_ago_sec'))
    if cut_ago is not None:
        values['log_cut_dt'] = now - timedelta(seconds=cut_ago)
    uptime = as_seconds(payload.get('uptime_sec'))
    if uptime is not None:
        values['log_started_dt'] = now - timedelta(seconds=uptime)
    return values


def link_condition(signals, now, threshold, watch_since):
    if not signals.pulse:
        return None, None
    if now - max(signals.pulse, watch_since or signals.pulse) > threshold:
        return 'logger', signals.pulse
    if now - signals.pulse > PULSE_FRESH:
        return None, None
    if signals.ads_ok:
        return 'ok', None
    if signals.ads_down_since and signals.pulse - signals.ads_down_since >= threshold:
        return 'ads', signals.ads_down_since
    return None, None


def resume_time(cause, signals, start):
    if cause == 'logger':
        moments = [start, signals.pulse_since or signals.pulse]
        if signals.ads_up_since:
            moments.append(signals.ads_up_since)
        return max(moments)
    return max(start, signals.ads_up_since or signals.pulse)


def chain_boundary(cause, signals, start):
    if cause == 'ads':
        return max(start, signals.pulse)
    return max(start, signals.pulse_since or signals.pulse)


def kept_reading(signals, start):
    return bool(signals.ads_up_since) and signals.ads_up_since <= start + DATA_MARGIN


def backlog_clear(backlog, backlog_from, end):
    return not backlog or bool(backlog_from and backlog_from > end)


def classify(cause, has_data, delivered, end, now):
    if has_data:
        return 'recovered'
    if has_data is None:
        return None
    if cause == 'ads' or delivered or now - end >= CHECK_GIVE_UP:
        return 'lost'
    return None


def cut_belongs(cut, start, end):
    return bool(cut) and start <= cut <= end + CUT_REPORT_WINDOW


def in_windows(windows, moment):
    return any(start - PLANNED_GRACE <= moment < end + PLANNED_GRACE for start, end in windows)


class MesLinkOutage(models.Model):
    _name = 'mes.link.outage'
    _description = 'Machine Data Gap'
    _order = 'date_start desc, id desc'

    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', required=True,
                                    ondelete='cascade', index=True)
    company_id = fields.Many2one('res.company', related='workcenter_id.company_id', store=True,
                                 index=True, string='Company')
    cause = fields.Selection([
        ('logger', 'Logger Not Responding'),
        ('ads', 'PLC Not Readable'),
    ], string='What Failed', required=True, default='logger', index=True,
        help="Logger Not Responding: the logger stopped reporting to Odoo (logger PC off, logger "
             "stopped or no network to Odoo). PLC Not Readable: the logger kept reporting but could "
             "not read the PLC over ADS (PLC off or not in RUN, ADS route or TwinCAT problem).")
    date_start = fields.Datetime(string='No Data Since', required=True, index=True)
    date_end = fields.Datetime(string='Data Back At', index=True)
    state = fields.Selection([
        ('ongoing', 'Ongoing'),
        ('checking', 'Checking Data'),
        ('recovered', 'Data Recovered'),
        ('lost', 'Data Lost'),
        ('planned', 'During Planned Downtime'),
    ], string='Result', required=True, default='ongoing', index=True,
        help="Checking Data: the connection is back and the logger is still sending what it stored. "
             "Data Recovered: the period was filled with data afterwards. Data Lost: nothing was "
             "read during the period; when the counters jumped on reconnect, the jump was not "
             "counted. During Planned Downtime: started and ended within planned downtime, not "
             "reported.")
    duration_minutes = fields.Float(string='Duration (min)', compute='_compute_duration_minutes',
                                    digits=(16, 0))
    under_planned = fields.Boolean(string='Started In Planned Downtime', readonly=True,
                                   help="Not reported while the planned downtime lasts; reported when it ends "
                                        "if the data is still missing.")
    alerted_at = fields.Datetime(string='Reported At', readonly=True,
                                 help="When the gap became reportable: at once, or when the planned "
                                      "downtime it started in was over.")
    reading_kept = fields.Boolean(string='Logger Kept Reading', readonly=True,
                                  help="The logger read the PLC all along and only could not reach Odoo.")
    counts_cut = fields.Boolean(string='Counter Jump Removed', readonly=True,
                                help="The logger took the first counter values after reconnecting as the new "
                                     "starting point, so production made during the gap was not added at "
                                     "the moment of reconnect.")
    loss_notified = fields.Boolean(string='Loss Emailed', readonly=True)
    restore_notified = fields.Boolean(string='Restore Emailed', readonly=True)

    def _compute_duration_minutes(self):
        now = fields.Datetime.now()
        for outage in self:
            end = outage.date_end or now
            outage.duration_minutes = max((end - outage.date_start).total_seconds(), 0.0) / 60.0

    @api.model
    def cron_check_links(self):
        now = fields.Datetime.now()
        watch_since = self._watch_since(now)
        Outage = self.sudo()
        workcenters = self.env['mrp.workcenter'].sudo().search([
            ('machine_settings_id.log_pulse_dt', '!=', False)])
        open_by_wc = {outage.workcenter_id.id: outage for outage in Outage.search([('state', '=', 'ongoing')])}
        for workcenter in workcenters:
            try:
                with self.env.cr.savepoint():
                    self._check_machine(workcenter, open_by_wc.get(workcenter.id), now, watch_since)
            except Exception:
                _logger.exception("Connection check failed for %s", workcenter.name)
        self._classify_closed(now)
        self._report(now)

    @api.model
    def _watch_since(self, now):
        cron = self.env.ref('mes_core.ir_cron_link_check', raise_if_not_found=False)
        previous = self.env.context.get('lastcall') or (cron.sudo().lastcall if cron else False)
        previous = fields.Datetime.to_datetime(previous) if previous else False
        params = self.env['ir.config_parameter'].sudo()
        stored = params.get_param(WATCH_SINCE_PARAM)
        if previous and stored and now - previous <= MISSED_RUN:
            return fields.Datetime.to_datetime(stored)
        params.set_param(WATCH_SINCE_PARAM, fields.Datetime.to_string(now))
        _logger.info("Connection checks resumed at %s; loggers get the full limit to report again", now)
        return now

    @api.model
    def _signals(self, workcenter):
        machine = workcenter.machine_settings_id
        return LinkSignals(machine.log_pulse_dt, machine.log_pulse_since_dt, machine.log_ads_ok,
                           machine.log_ads_down_since_dt, machine.log_ads_up_since_dt)

    @api.model
    def _check_machine(self, workcenter, current, now, watch_since):
        signals = self._signals(workcenter)
        threshold = timedelta(minutes=self._loss_minutes(workcenter.company_id))
        condition, start = link_condition(signals, now, threshold, watch_since)
        if condition is None or (current and current.cause == condition):
            return
        if condition == 'ok':
            if current:
                current._close(resume_time(current.cause, signals, current.date_start),
                               current.cause == 'logger' and kept_reading(signals, current.date_start))
            return
        if self._has_data(workcenter, now - threshold, now + DATA_MARGIN):
            return
        if current:
            boundary = chain_boundary(current.cause, signals, current.date_start)
            current._close(boundary, False)
            start = max(start, boundary)
        self.sudo().create({
            'workcenter_id': workcenter.id,
            'cause': condition,
            'date_start': start,
            'under_planned': self._in_planned(workcenter, start),
        })

    def _close(self, end, reading_kept):
        self.write({'date_end': end, 'state': 'checking', 'reading_kept': reading_kept})

    @api.model
    def _classify_closed(self, now):
        Outage = self.sudo()
        for outage in Outage.search([('state', '=', 'checking')]):
            if not outage.alerted_at:
                outage.write({'state': 'planned'})
                continue
            machine = outage.workcenter_id.machine_settings_id
            if outage.reading_kept:
                has_data = True
            else:
                has_data = self._has_data(outage.workcenter_id, outage.date_start + DATA_MARGIN,
                                          outage.date_end - DATA_MARGIN)
            delivered = backlog_clear(machine.log_backlog, machine.log_backlog_from_dt, outage.date_end)
            result = classify(outage.cause, has_data, delivered, outage.date_end, now)
            if result:
                outage.write({'state': result})
        for outage in Outage.search([
            ('state', '=', 'lost'),
            ('counts_cut', '=', False),
            ('date_end', '>=', now - CUT_LOOKBACK),
        ]):
            if cut_belongs(outage.workcenter_id.machine_settings_id.log_cut_dt, outage.date_start, outage.date_end):
                outage.counts_cut = True

    @api.model
    def _report(self, now):
        Outage = self.sudo()
        due = Outage
        for outage in Outage.search([('state', '=', 'ongoing'), ('alerted_at', '=', False)]):
            if outage.under_planned and self._in_planned(outage.workcenter_id, now):
                continue
            due |= outage
        if due:
            due.write({'alerted_at': now})
            self._notify(due, 'lost')
        busy = set(Outage.search([('state', '=', 'ongoing')]).mapped('workcenter_id').ids)
        back = Outage.search([
            ('state', 'in', CLOSED_STATES),
            ('loss_notified', '=', True),
            ('restore_notified', '=', False),
        ]).filtered(lambda outage: outage.workcenter_id.id not in busy)
        if back:
            self._notify(back, 'restored')

    @api.model
    def _loss_minutes(self, company):
        return max(MIN_LOSS_MINUTES, company.mes_link_loss_minutes or DEFAULT_LOSS_MINUTES)

    @api.model
    def _in_planned(self, workcenter, moment):
        slots = self.env['mes.flat.downtime'].sudo().search([
            ('machine_id', '=', workcenter.id),
            ('start_time', '<=', moment + PLANNED_GRACE),
            ('end_time', '>', moment - PLANNED_GRACE),
        ])
        return in_windows([(slot.start_time, slot.end_time) for slot in slots], moment)

    @api.model
    def _machine_tz(self, workcenter):
        return pytz.timezone(workcenter.company_id.tz or 'UTC')

    @api.model
    def _has_data(self, workcenter, start, end):
        if end <= start:
            return False
        tz = self._machine_tz(workcenter)
        skew = timedelta(seconds=workcenter.machine_settings_id.log_clock_skew_sec or 0)
        bounds = [pytz.utc.localize(moment + skew).astimezone(tz).strftime('%Y-%m-%d %H:%M:%S')
                  for moment in (start, end)]
        probes = " UNION ALL ".join(
            f"(SELECT 1 FROM {table} WHERE machine_name = %s AND time > %s AND time < %s LIMIT 1)"
            for table in TELEMETRY_TABLES)
        params = []
        for _table in TELEMETRY_TABLES:
            params += [workcenter.machine_settings_id.name] + bounds
        try:
            with self.env['mes.timescale.base']._connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT EXISTS ({probes})", params)
                    return bool(cursor.fetchone()[0])
        except Exception:
            _logger.warning("Connection check could not read telemetry for %s", workcenter.name)
            return None

    def _local(self, moment, workcenter):
        if not moment:
            return ''
        local = pytz.utc.localize(moment).astimezone(self._machine_tz(workcenter))
        return local.strftime('%d/%m/%Y %H:%M')

    def _notify(self, outages, kind):
        by_company = {}
        for outage in outages:
            by_company[outage.company_id] = by_company.get(outage.company_id, self.browse()) | outage
        sent = False
        for company, group in by_company.items():
            recipients = parse_emails(company.mes_link_loss_emails)
            if not recipients:
                continue
            localized = group.with_context(lang=company.partner_id.lang or 'en_US')
            subject, body = localized._compose(kind)
            email_from = company.email_formatted
            if not email_from and hasattr(self.env['ir.mail_server'], '_get_default_from_address'):
                email_from = self.env['ir.mail_server'].sudo()._get_default_from_address()
            self.env['mail.mail'].sudo().create({
                'subject': subject,
                'body_html': body,
                'email_to': ', '.join(recipients),
                'email_from': email_from or False,
                'auto_delete': False,
            })
            group.write({'loss_notified': True} if kind == 'lost' else {'restore_notified': True})
            sent = True
        if sent:
            scheduler = self.env.ref('mail.ir_cron_mail_scheduler_action', raise_if_not_found=False)
            if scheduler:
                scheduler.sudo()._trigger()

    def _subject(self, kind):
        names = ', '.join(self.mapped('workcenter_id.name'))
        if kind == 'restored':
            return _("MES: data is coming in again — %s", names)
        causes = set(self.mapped('cause'))
        if causes == {'logger'}:
            return _("MES: logger not responding — %s", names)
        if causes == {'ads'}:
            return _("MES: PLC not readable — %s", names)
        return _("MES: machines not sending data — %s", names)

    def _what_to_check(self):
        if self.cause == 'ads':
            return _("PLC power and RUN mode, TwinCAT runtime, ADS route on the logger PC")
        return _("Logger PC power, the logger task, network between the logger PC and Odoo")

    def _result_text(self, labels):
        text = labels.get(self.state, self.state)
        if self.counts_cut:
            text = _("%s, counter jump removed", text)
        return text

    def _compose(self, kind):
        causes = dict(self._fields['cause']._description_selection(self.env))
        states = dict(self._fields['state']._description_selection(self.env))
        if kind == 'lost':
            intro = _("These machines are not sending data. Their stops and counts are not being recorded.")
            header = [_("Machine"), _("Not responding"), _("No data since"), _("Note"), _("What to check")]
            rows = [[outage.workcenter_id.name,
                     causes.get(outage.cause, outage.cause),
                     outage._local(outage.date_start, outage.workcenter_id),
                     _("Lost during planned downtime, still not back after it ended")
                     if outage.under_planned else '',
                     outage._what_to_check()] for outage in self]
        else:
            intro = _("These machines are sending data again.")
            header = [_("Machine"), _("Not responding"), _("No data since"), _("Data back at"),
                      _("Duration (min)"), _("Result")]
            rows = [[outage.workcenter_id.name,
                     causes.get(outage.cause, outage.cause),
                     outage._local(outage.date_start, outage.workcenter_id),
                     outage._local(outage.date_end, outage.workcenter_id),
                     f"{outage.duration_minutes:.0f}",
                     outage._result_text(states)] for outage in self]
        cell = 'style="border:1px solid #ccc;padding:4px 8px;text-align:left;vertical-align:top"'
        table = Markup('<tr>%s</tr>') % Markup('').join(Markup(f'<th {cell}>%s</th>') % escape(h) for h in header)
        for row in rows:
            table += Markup('<tr>%s</tr>') % Markup('').join(Markup(f'<td {cell}>%s</td>') % escape(v) for v in row)
        body = Markup('<p>%s</p><table style="border-collapse:collapse">%s</table>') % (escape(intro), table)
        return self._subject(kind), body


class MrpWorkcenter(models.Model):
    _inherit = 'mrp.workcenter'

    link_outage_ids = fields.One2many('mes.link.outage', 'workcenter_id', string='Data Gaps')
