import pytz
import logging
from datetime import datetime, timedelta, time
from odoo import models, fields, api

_logger = logging.getLogger(__name__)

class MesMachinePerformance(models.Model):
    _name = 'mes.machine.performance'
    _description = 'Machine Performance Data (OEE)'
    _order = 'date desc, shift_id'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    FSM_SETTLE_SEC = 15
    REASON_LOOKBACK_SEC = 180
    PROVISIONAL_MAX_WAIT_SEC = 300

    name = fields.Char(string='Doc ID', default='New', readonly=True, copy=False)
    date = fields.Date(string='Date', required=True, default=fields.Date.context_today)
    shift_id = fields.Many2one('mes.shift', string='Shift', required=True)
    machine_id = fields.Many2one('mrp.workcenter', string='Machine', required=True)
    company_id = fields.Many2one('res.company', string='Company', related='machine_id.company_id', store=True, readonly=True)
    state = fields.Selection([('draft', 'Draft'), ('done', 'Locked')], string='Status', default='draft', tracking=True)

    alarm_ids = fields.One2many('mes.performance.alarm', 'performance_id', string='Alarms')
    running_ids = fields.One2many('mes.performance.running', 'performance_id', string='Running Logs')
    slowing_ids = fields.One2many('mes.performance.slowing', 'performance_id', string='Slowing Logs')
    rejection_ids = fields.One2many('mes.performance.rejection', 'performance_id', string='Rejections')
    production_ids = fields.One2many('mes.performance.production', 'performance_id', string='Production Output')
    login_entry_ids = fields.Many2many(
        'mes.performance.login.entry',
        string='Login History',
        compute='_compute_login_entry_ids',
    )

    @api.depends('machine_id', 'date', 'shift_id.start_hour', 'shift_id.end_hour')
    def _compute_login_entry_ids(self):
        Entry = self.env['mes.performance.login.entry']
        for rec in self:
            if not rec.machine_id or not rec.date:
                rec.login_entry_ids = Entry.browse()
                continue
            shift_start, shift_end = rec._performance_window()
            rec.login_entry_ids = Entry.search([
                ('workcenter_id', '=', rec.machine_id.id),
                ('login_at', '>=', shift_start),
                ('login_at', '<', shift_end),
            ])

    def _performance_window(self):
        self.ensure_one()
        from datetime import datetime as _dt, time as _t, timedelta as _td
        start_h = self.shift_id.start_hour if self.shift_id else 0.0
        end_h = self.shift_id.end_hour if self.shift_id else 24.0
        start = _dt.combine(self.date, _t(int(start_h), int((start_h % 1) * 60)))
        if end_h <= start_h:
            end = _dt.combine(self.date + _td(days=1), _t(int(end_h), int((end_h % 1) * 60)))
        else:
            end = _dt.combine(self.date, _t(min(23, int(end_h)), int((end_h % 1) * 60)))
        return start, end

    _sql_constraints = [
        ('uniq_report', 'unique(machine_id, date, shift_id)', 'Report for this shift already exists!')
    ]

    @api.model_create_multi
    def create(self, vals_list):
        machine_ids = [v.get('machine_id') for v in vals_list if v.get('machine_id')]
        machines = {m.id: m.name for m in self.env['mrp.workcenter'].browse(machine_ids)}

        for vals in vals_list:
            if vals.get('name', 'New') == 'New':
                doc_date = vals.get('date') or fields.Date.context_today(self)
                machine_id = vals.get('machine_id')
                machine_name = machines.get(machine_id, str(machine_id))
                vals['name'] = f"PERF/{doc_date}/{machine_name}"
        
        return super().create(vals_list)

    def action_set_draft(self):
        for rec in self:
            if rec.state == 'done':
                rec.write({'state': 'draft'})

    @api.model
    def cron_process_pending_events(self):
        workcenters = self.env['mrp.workcenter'].search([('machine_settings_id', '!=', False)])
        for wc in workcenters:
            if wc.is_hist_syncing:
                continue
            try:
                with self.env.cr.savepoint():
                    self._sync_machine_fsm(wc)
                    self.env['mes.pending.comment'].apply_for_workcenter(wc)
            except Exception as e:
                _logger.error("CRON FSM FAULT | WC: %s | Err: %s", wc.name, str(e))

    @api.model
    def _sync_machine_fsm_job(self, workcenter_id):
        wc = self.env['mrp.workcenter'].browse(workcenter_id).exists()
        if not wc or not wc.machine_settings_id:
            return False
        if wc.is_hist_syncing:
            return False
        try:
            with self.env.cr.savepoint():
                self._sync_machine_fsm(wc)
        except Exception as e:
            _logger.error("FSM JOB FAULT | WC: %s | Err: %s", wc.name, str(e))
            return False
        self.env.flush_all()
        self.env['mes.pending.comment'].apply_for_workcenter(wc)
        wc._notify_kiosk_state_refresh()
        return True

    def _sync_machine_fsm(self, wc):
        self.env.flush_all()

        machine_settings = wc.machine_settings_id
        tz_name = wc.company_id.tz or 'UTC'
        local_tz = pytz.timezone(tz_name)

        if machine_settings:
            self._patch_provisional_alarms(wc, machine_settings, local_tz)
            self.env.flush_all()

        open_states = []
        for model in ['mes.performance.running', 'mes.performance.alarm', 'mes.performance.slowing']:
            records = self.env[model].search([
                ('performance_id.machine_id', '=', wc.id),
                ('end_time', '=', False)
            ])
            open_states.extend(records)

        active_state = None
        if open_states:
            open_states.sort(key=lambda x: x.start_time, reverse=True)
            active_state = open_states[0]
            
            for orphan in open_states[1:]:
                if orphan.performance_id.state != 'done':
                    orphan.write({'end_time': active_state.start_time})

        latest_ts = None
        for model in ['mes.performance.running', 'mes.performance.alarm', 'mes.performance.slowing']:
            rec_start = self.env[model].search([('performance_id.machine_id', '=', wc.id)], order='start_time desc', limit=1)
            if rec_start and (not latest_ts or rec_start.start_time > latest_ts):
                latest_ts = rec_start.start_time
            
            rec_end = self.env[model].search([('performance_id.machine_id', '=', wc.id), ('end_time', '!=', False)], order='end_time desc', limit=1)
            if rec_end and (not latest_ts or rec_end.end_time > latest_ts):
                latest_ts = rec_end.end_time

        last_utc_ts = latest_ts if latest_ts else (fields.Datetime.now() - timedelta(days=1))

        last_local_ts = pytz.utc.localize(last_utc_ts).astimezone(local_tz).replace(tzinfo=None)
        start_time_string = last_local_ts.strftime('%Y-%m-%d %H:%M:%S.%f')

        end_local = datetime.now(local_tz).replace(tzinfo=None) - timedelta(seconds=self.FSM_SETTLE_SEC)
        if end_local <= last_local_ts:
            return
        end_time_string = end_local.strftime('%Y-%m-%d %H:%M:%S.%f')

        events = self._fetch_telemetry_events(
            workcenter=wc, 
            machine_settings=machine_settings, 
            start_time_string=start_time_string, 
            end_time_string=end_time_string, 
            include_initial=True
        )

        if not events:
            return

        _logger.debug("=== FSM EVENTS for %s ===", wc.name)
        for row in events:
            _logger.debug("FSM %s event %s", wc.name, row)

        for row in events:
            ts_raw, _, tag, val = row[:4]
            is_unresolved = bool(row[4]) if len(row) > 4 else False
            evt_utc = self._convert_raw_time_to_utc(ts_raw, local_tz)
            
            if active_state and evt_utc <= active_state.start_time:
                continue

            tgt_model, evt_id = self._classify_telemetry_event(wc, machine_settings, tag, val)
            
            _logger.debug("FSM %s STEP | tag=%s val=%s tgt=%s evt_id=%s | active=%s active_loss=%s",
                wc.name, tag, val, tgt_model, evt_id,
                active_state._name if active_state else None,
                active_state.loss_id.id if active_state else None
            )

            if tgt_model == 'mes.performance.alarm' and (val is None or int(float(val)) == 0):
                continue

            if not tgt_model or not evt_id:
                _logger.debug("FSM %s SKIP: no tgt_model or evt_id", wc.name)
                continue

            active_model = active_state._name if active_state else None
            active_loss_id = active_state.loss_id.id if active_state else None

            if active_model == tgt_model and active_loss_id == evt_id:
                continue

            if active_model == 'mes.performance.alarm' and tgt_model == 'mes.performance.slowing':
                continue

            evt_utc_stored = evt_utc.replace(microsecond=0)
            if self.env[tgt_model].search_count([
                ('performance_id.machine_id', '=', wc.id),
                ('loss_id', '=', evt_id),
                ('start_time', '=', evt_utc_stored),
            ]):
                continue

            perf_doc, is_new = self._get_or_create_doc(wc, evt_utc)
            
            if is_new:
                break

            if active_state and active_state.performance_id.state != 'done':
                current_doc = active_state.performance_id
                
                if perf_doc and current_doc.id != perf_doc.id:
                    _, shift_end_loc = current_doc._get_local_shift_times()
                    shift_end_utc = current_doc._get_utc_time(shift_end_loc)
                    
                    safe_old_end = max(active_state.start_time, min(evt_utc, shift_end_utc))
                    active_state.write({'end_time': safe_old_end})
                    
                    if perf_doc.state != 'done' and shift_end_utc < evt_utc:
                        self.env[active_model].create({
                            'performance_id': perf_doc.id,
                            'loss_id': active_loss_id,
                            'start_time': shift_end_utc,
                            'end_time': evt_utc
                        })
                else:
                    active_state.write({'end_time': evt_utc})

            if not perf_doc or perf_doc.state == 'done':
                active_state = None
                continue

            new_vals = {
                'performance_id': perf_doc.id,
                'loss_id': evt_id,
                'start_time': evt_utc
            }
            if tgt_model == 'mes.performance.alarm' and is_unresolved:
                new_vals['reason_provisional'] = True
            active_state = self.env[tgt_model].create(new_vals)

    def _safe_plc_value(self, raw_value):
        try:
            return int(float(raw_value))
        except (TypeError, ValueError):
            return None

    def _patch_provisional_alarms(self, wc, machine_settings, local_tz):
        Alarm = self.env['mes.performance.alarm']
        provisional = Alarm.search([
            ('performance_id.machine_id', '=', wc.id),
            ('performance_id.state', '=', 'draft'),
            ('reason_provisional', '=', True),
            ('start_time', '!=', False),
        ], order='start_time asc', limit=200)
        if not provisional:
            return

        now_utc = fields.Datetime.now()
        starts = provisional.mapped('start_time')
        win_start_utc = min(starts) - timedelta(seconds=self.REASON_LOOKBACK_SEC)
        win_end_utc = max(alarm.end_time or now_utc for alarm in provisional)

        def to_local_str(dt_utc):
            return pytz.utc.localize(dt_utc).astimezone(local_tz).replace(tzinfo=None) \
                          .strftime('%Y-%m-%d %H:%M:%S.%f')

        try:
            with self.env['mes.timescale.base']._connection() as connection:
                with connection.cursor() as cursor:
                    cursor.execute("""
                        SELECT time, value
                        FROM telemetry_event
                        WHERE machine_name = %s
                          AND tag_name = 'OEE.nStopRootReason'
                          AND value <> 0
                          AND time > %s AND time <= %s
                        ORDER BY time ASC
                    """, (machine_settings.name, to_local_str(win_start_utc), to_local_str(win_end_utc)))
                    rows = cursor.fetchall()
        except Exception as exc:
            _logger.error("FSM_PATCH_FETCH_FAIL | WC: %s | Err: %s", wc.name, exc)
            return

        reasons = [(self._convert_raw_time_to_utc(t, local_tz), v) for t, v in rows]
        patched = False

        for alarm in provisional:
            start = alarm.start_time
            upper = alarm.end_time or now_utc
            lower = start - timedelta(seconds=self.REASON_LOOKBACK_SEC)

            after = [r for r in reasons if start <= r[0] <= upper]
            before = [r for r in reasons if lower <= r[0] < start]

            if not after:
                stop_is_over = bool(alarm.end_time)
                waited_sec = (now_utc - start).total_seconds()
                if stop_is_over or waited_sec > self.PROVISIONAL_MAX_WAIT_SEC:
                    alarm.reason_provisional = False
                continue

            nearest = after[0]
            if before and (start - before[-1][0]) <= (after[0][0] - start):
                nearest = before[-1]

            plc_val = self._safe_plc_value(nearest[1])
            if plc_val is None:
                alarm.reason_provisional = False
                continue

            event = self._resolve_event(machine_settings, 'OEE.nStopRootReason', plc_val)
            vals = {'reason_provisional': False}
            if event and event.id != alarm.loss_id.id:
                vals['loss_id'] = event.id
                patched = True
                _logger.info("FSM_REASON_PATCHED | WC: %s | alarm %s: %s -> %s",
                             wc.name, alarm.id, alarm.loss_id.display_name, event.display_name)
            alarm.write(vals)

        if patched:
            self.env.flush_all()
            self._merge_adjacent_alarms(wc)

    def _merge_adjacent_alarms(self, wc):
        Alarm = self.env['mes.performance.alarm']
        records = Alarm.search([
            ('performance_id.machine_id', '=', wc.id),
            ('performance_id.state', '=', 'draft'),
            ('start_time', '!=', False),
        ], order='start_time asc')
        previous = None
        for rec in records:
            if not rec.end_time:
                previous = rec
                continue
            if (previous is not None
                    and previous.performance_id.id == rec.performance_id.id
                    and previous.loss_id.id == rec.loss_id.id
                    and previous.end_time and previous.end_time == rec.start_time):
                previous.write({'end_time': rec.end_time})
                rec.unlink()
                continue
            previous = rec

    @api.model
    def classify_fsm_transition(self, wc, tag, val):
        plc_val = int(val) if val is not None else 0
        mac = wc.machine_settings_id
        
        evt = self._resolve_event(mac, tag, plc_val)
        if not evt:
            return None, None

        is_run = False
        if wc.runtime_event_id:
            run_sig = mac.event_tag_ids.filtered(lambda x: x.event_id == wc.runtime_event_id)
            if run_sig:
                is_run = (run_sig[0].tag_name == tag and run_sig[0].plc_value == plc_val)
            else:
                is_run = (wc.runtime_event_id.default_event_tag_type == tag and wc.runtime_event_id.default_plc_value == plc_val)
        
        if is_run:
            return 'mes.performance.running', evt.id

        stop_tag = mac.get_alarm_tag_name('OEE.nStopRootReason').replace('%', '')
        if tag == stop_tag or tag == 'OEE.nStopRootReason':
            return 'mes.performance.alarm', evt.id

        return 'mes.performance.slowing', evt.id

    @api.model
    def _resolve_event(self, mac, tag, val):
        sig = self.env['mes.signal.event'].search([('machine_id', '=', mac.id), ('tag_name', '=', tag), ('plc_value', '=', val)], limit=1)
        if sig: 
            return sig.event_id
            
        evt = self.env['mes.event'].search([('default_event_tag_type', '=', tag), ('default_plc_value', '=', val)], limit=1)
        if evt: 
            return evt
            
        grp = self.env['mes.event'].search([('name', '=', 'Unknown'), ('parent_id', '=', False)], limit=1)
        if not grp: 
            grp = self.env['mes.event'].create({'name': 'Unknown'})
            
        return self.env['mes.event'].create({
            'name': f'Unknown {tag} Code {val}',
            'parent_id': grp.id, 
            'default_event_tag_type': tag, 
            'default_plc_value': val
        })

    @api.model
    def cron_manage_shifts(self):
        now_utc = fields.Datetime.now()
        draft_docs = self.search([('state', '=', 'draft')], order='date asc', limit=50)

        for doc in draft_docs:
            try:
                shift_start_loc, shift_end_loc = doc._get_local_shift_times()
                shift_end_utc = doc._get_utc_time(shift_end_loc)
                
                closure_threshold_utc = shift_end_utc + timedelta(minutes=5)

                if now_utc >= closure_threshold_utc:
                    doc._close_shift_and_calculate_totals(shift_start_loc, shift_end_loc, shift_end_utc)
                    self.env.cr.commit()
            except Exception as exc:
                self.env.cr.rollback()
                _logger.error("SHIFT_CLOSE_FAIL | Doc: %s | Err: %s", doc.id, str(exc))

    def _close_shift_and_calculate_totals(self, shift_start_loc, shift_end_loc, shift_end_utc):
        workcenter = self.machine_id
        machine_settings = workcenter.machine_settings_id

        if not machine_settings:
            self.write({'state': 'done'})
            return

        annotations = self._snapshot_user_annotations()
        self._clear_previous_shift_data()

        now_utc = fields.Datetime.now()
        mac_tz = pytz.timezone(workcenter.company_id.tz or 'UTC')
        
        if now_utc < shift_end_utc:
            calc_end_utc = now_utc
            calc_end_loc = pytz.utc.localize(now_utc).astimezone(mac_tz).replace(tzinfo=None)
        else:
            calc_end_utc = shift_end_utc
            calc_end_loc = shift_end_loc

        start_time_string = shift_start_loc.strftime('%Y-%m-%d %H:%M:%S.%f')
        end_time_string = calc_end_loc.strftime('%Y-%m-%d %H:%M:%S.%f')

        telemetry_events = self._fetch_telemetry_events(workcenter, machine_settings, start_time_string, end_time_string, include_initial=True)
        self._process_telemetry_events(workcenter, machine_settings, telemetry_events, calc_end_utc)
        self._process_telemetry_counts(machine_settings, start_time_string, end_time_string)
        self._restore_user_annotations(annotations)

        if self._is_empty_shift():
            self.unlink()
        else:
            if calc_end_utc == shift_end_utc:
                self.write({'state': 'done'})

    def _clear_previous_shift_data(self):
        self.running_ids.unlink()
        self.alarm_ids.unlink()
        self.slowing_ids.unlink()
        self.production_ids.unlink()
        self.rejection_ids.unlink()

    _ANNOTATED_LINES = (
        ('alarm_ids', 'mes.performance.alarm'),
        ('running_ids', 'mes.performance.running'),
        ('slowing_ids', 'mes.performance.slowing'),
    )

    def _snapshot_user_annotations(self):
        snapshot = {}
        for field_name, model_name in self._ANNOTATED_LINES:
            for record in self[field_name]:
                if record.comment:
                    snapshot.setdefault(model_name, []).append({
                        'loss_id': record.loss_id.id,
                        'start_time': record.start_time,
                        'comment': record.comment,
                    })
        return snapshot

    def _restore_user_annotations(self, snapshot):
        if not snapshot:
            return
        for model_name, entries in snapshot.items():
            candidates = self.env[model_name].search([('performance_id', '=', self.id)])
            if not candidates:
                continue
            used = set()

            def _pick(predicate):
                for candidate in candidates:
                    if candidate.id not in used and predicate(candidate):
                        return candidate
                return None

            for entry in entries:
                start = entry['start_time']

                def _delta_ok(candidate, tolerance=90):
                    return (candidate.start_time and start
                            and abs((candidate.start_time - start).total_seconds()) <= tolerance)

                match = _pick(lambda c: c.loss_id.id == entry['loss_id'] and c.start_time == start)
                if not match:
                    match = _pick(lambda c: c.loss_id.id == entry['loss_id'] and _delta_ok(c))
                if not match:
                    match = _pick(_delta_ok)
                if not match:
                    _logger.warning("ANNOTATION_ORPHANED | Doc: %s | %s | start=%s | comment=%r",
                                    self.id, model_name, start, entry['comment'])
                    continue
                used.add(match.id)
                match.comment = (f"{match.comment} | {entry['comment']}"
                                 if match.comment else entry['comment'])

    def _fetch_telemetry_events(self, workcenter, machine_settings, start_time_string, end_time_string=None, include_initial=True):
        is_state_logic = workcenter.telemetry_state_logic == 'states'
        
        ctes = []
        params = []
        
        if include_initial:
            ctes.append("""
                LastTags AS 
                (
                    SELECT tag_name, value FROM 
                    (
                        SELECT tag_name, value, ROW_NUMBER() OVER(PARTITION BY tag_name ORDER BY time DESC) as rn
                        FROM telemetry_event
                        WHERE machine_name = %s AND time <= %s 
                          AND (tag_name = 'OEE.nMachineState' OR (tag_name = 'OEE.nStopRootReason' AND value <> 0))
                    ) sub WHERE rn = 1
                )
            """)
            params.extend([machine_settings.name, start_time_string])
            
            base_query = """
                SELECT CAST(%s AS TIMESTAMP) AS time,
                    CASE WHEN (SELECT value FROM LastTags WHERE tag_name = 'OEE.nMachineState') = 1 THEN 'OEE.nStopRootReason' ELSE 'OEE.nMachineState' END AS tag_name,
                    CASE WHEN (SELECT value FROM LastTags WHERE tag_name = 'OEE.nMachineState') = 1 THEN (SELECT value FROM LastTags WHERE tag_name = 'OEE.nStopRootReason') ELSE (SELECT value FROM LastTags WHERE tag_name = 'OEE.nMachineState') END AS value,
                    0 AS is_raw_reason
                WHERE (SELECT value FROM LastTags WHERE tag_name = 'OEE.nMachineState') IS NOT NULL
                UNION ALL
            """
            params.append(start_time_string)
        else:
            base_query = ""

        time_filter = "time > %s"
        time_params = [start_time_string]
        
        if end_time_string:
            time_filter += " AND time <= %s"
            time_params.append(end_time_string)

        if is_state_logic:
            reason_filter = ("time > CAST(%s AS TIMESTAMP) - INTERVAL '{} seconds'"
                             .format(int(self.REASON_LOOKBACK_SEC)))
            reason_params = [start_time_string]
            if end_time_string:
                reason_filter += " AND time <= %s"
                reason_params.append(end_time_string)
            base_query += f"""
                SELECT time, tag_name, value,
                       CASE WHEN tag_name = 'OEE.nStopRootReason' THEN 1 ELSE 0 END AS is_raw_reason
                FROM telemetry_event
                WHERE machine_name = %s AND (
                    (tag_name = 'OEE.nMachineState' AND {time_filter})
                    OR (tag_name = 'OEE.nStopRootReason' AND value <> 0 AND {reason_filter})
                )
            """
            params.extend([machine_settings.name] + time_params + reason_params)
        else:
            base_query += f"""
                SELECT time, tag_name, value,
                       CASE WHEN tag_name = 'OEE.nStopRootReason' THEN 1 ELSE 0 END AS is_raw_reason
                FROM telemetry_event 
                WHERE machine_name = %s AND {time_filter} AND (tag_name = 'OEE.nMachineState' OR (tag_name = 'OEE.nStopRootReason' AND value <> 0))
            """
            params.extend([machine_settings.name] + time_params)
        
        ctes.append(f"BaseEvents AS\n(\n{base_query}\n)")

        if is_state_logic:
            ctes.append("""
                ReasonTimeline AS 
                (
                    SELECT time, tag_name, value, is_raw_reason,
                           CASE WHEN tag_name = 'OEE.nStopRootReason' THEN time END AS reason_time,
                           CASE WHEN tag_name = 'OEE.nStopRootReason' THEN value END AS reason_value
                    FROM BaseEvents
                ),
                ReasonGroups AS 
                (
                    SELECT time, tag_name, value, is_raw_reason, reason_value,
                           COUNT(reason_value) OVER (ORDER BY time ASC, is_raw_reason DESC
                                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS prev_grp,
                           COUNT(reason_value) OVER (ORDER BY time DESC, is_raw_reason DESC
                                                     ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS next_grp
                    FROM ReasonTimeline
                ),
                ReasonTimelineWithPrevNext AS 
                (
                    SELECT time, tag_name, value, is_raw_reason,
                           FIRST_VALUE(reason_value) OVER (PARTITION BY prev_grp ORDER BY time ASC, is_raw_reason DESC) AS prev_val,
                           FIRST_VALUE(time) OVER (PARTITION BY prev_grp ORDER BY time ASC, is_raw_reason DESC) AS prev_time,
                           FIRST_VALUE(reason_value) OVER (PARTITION BY next_grp ORDER BY time DESC, is_raw_reason DESC) AS next_val,
                           FIRST_VALUE(time) OVER (PARTITION BY next_grp ORDER BY time DESC, is_raw_reason DESC) AS next_time
                    FROM ReasonGroups
                ),
                ResolvedEvents AS 
                (
                    SELECT time, 
                           CASE WHEN tag_name = 'OEE.nMachineState' AND value = 1 THEN 'OEE.nStopRootReason' ELSE tag_name END AS tag_name,
                           CASE WHEN tag_name = 'OEE.nMachineState' AND value = 1 THEN
                               CASE WHEN prev_time IS NULL AND next_time IS NULL THEN value
                                    WHEN prev_time IS NULL THEN next_val
                                    WHEN next_time IS NULL THEN prev_val
                                    WHEN EXTRACT(EPOCH FROM (next_time - time)) < EXTRACT(EPOCH FROM (time - prev_time)) THEN next_val
                                    ELSE prev_val END
                           ELSE value END AS value,
                           is_raw_reason,
                           CASE WHEN tag_name = 'OEE.nMachineState' AND value = 1
                                     AND next_time IS NULL
                                THEN 1 ELSE 0 END AS is_unresolved_stop
                    FROM ReasonTimelineWithPrevNext
                )
            """)
            target_table = "ResolvedEvents"
        else:
            target_table = "BaseEvents"

        clean_filter = "WHERE is_raw_reason = 0" if is_state_logic else ""
        unresolved_expr = "is_unresolved_stop" if is_state_logic else "0"

        trans_vals = []
        raw_trans = getattr(machine_settings, 'transitional_state_values', False) or '3,5'
        if str(raw_trans).strip().lower() != 'none':
            for tok in str(raw_trans).split(','):
                tok = tok.strip()
                if tok.lstrip('-').isdigit():
                    trans_vals.append(int(tok))

        if trans_vals:
            flicker_where = ("WHERE NOT (tag_name = 'OEE.nMachineState' AND value = ANY(%s) "
                             "AND prev_tag = 'OEE.nStopRootReason' AND prev_value = next_value)")
        else:
            flicker_where = ""

        ctes.append(f"""
            CleanedEvents AS 
            (
                SELECT time, tag_name, value, {unresolved_expr} AS is_unresolved_stop
                FROM {target_table} 
                {clean_filter}
            ),
            FlickerFilter AS 
            (
                SELECT time, tag_name, value, is_unresolved_stop,
                       LAG(tag_name) OVER (ORDER BY time) AS prev_tag,
                       LAG(value) OVER (ORDER BY time) AS prev_value,
                       LEAD(tag_name) OVER (ORDER BY time) AS next_tag,
                       LEAD(value) OVER (ORDER BY time) AS next_value
                FROM CleanedEvents
            ),
            FilteredEvents AS 
            (
                SELECT time, tag_name, value, is_unresolved_stop
                FROM FlickerFilter
                {flicker_where}
            )
        """)
        if trans_vals:
            params.append(trans_vals)

        if end_time_string:
            end_time_expr = f"""
                COALESCE(
                    LEAD(time) OVER (ORDER BY time), 
                    CASE WHEN EXISTS (SELECT 1 FROM telemetry_event WHERE machine_name = %s AND time > %s) THEN CAST(%s AS TIMESTAMP) ELSE NULL END
                )
            """
            params.extend([machine_settings.name, end_time_string, end_time_string])
        else:
            end_time_expr = "LEAD(time) OVER (ORDER BY time)"

        query = f"""
            WITH {','.join(ctes)}
            SELECT time AS start_time, {end_time_expr} AS end_time, tag_name, value, is_unresolved_stop
            FROM FilteredEvents
            ORDER BY time ASC
        """
        
        if not end_time_string:
            query += " LIMIT 5000"

        with self.env['mes.timescale.base']._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                return cursor.fetchall()

    def _process_telemetry_events(self, workcenter, machine_settings, telemetry_events, shift_end_utc):
        timezone_name = workcenter.company_id.tz or 'UTC'
        local_timezone = pytz.timezone(timezone_name)

        current_target_model = None
        current_loss_identifier = None
        current_event_start_utc = None
        last_event_end_utc = None

        for event_row in telemetry_events:
            raw_start_time, raw_end_time, tag_name, value = event_row[:4]
            
            start_time_utc = self._convert_raw_time_to_utc(raw_start_time, local_timezone)
            end_time_utc = self._convert_raw_time_to_utc(raw_end_time, local_timezone) if raw_end_time else None

            target_model, loss_identifier = self._classify_telemetry_event(workcenter, machine_settings, tag_name, value)

            if target_model == 'mes.performance.alarm' and (value is None or int(float(value)) == 0):
                last_event_end_utc = end_time_utc
                continue

            if target_model and loss_identifier:
                
                if current_target_model == 'mes.performance.alarm' and target_model != 'mes.performance.running':
                    last_event_end_utc = end_time_utc
                    continue

                if target_model != current_target_model or loss_identifier != current_loss_identifier:
                    if current_target_model and current_loss_identifier and current_event_start_utc:
                        self.env[current_target_model].create(
                        {
                            'performance_id': self.id,
                            'loss_id': current_loss_identifier,
                            'start_time': current_event_start_utc,
                            'end_time': start_time_utc
                        })
                    
                    current_target_model = target_model
                    current_loss_identifier = loss_identifier
                    current_event_start_utc = start_time_utc

            last_event_end_utc = end_time_utc

        if current_target_model and current_loss_identifier and current_event_start_utc:
            final_end_time = last_event_end_utc if last_event_end_utc else shift_end_utc
            
            self.env[current_target_model].create(
            {
                'performance_id': self.id,
                'loss_id': current_loss_identifier,
                'start_time': current_event_start_utc,
                'end_time': final_end_time
            })
            
            if final_end_time == shift_end_utc:
                self._handle_shift_overflow(workcenter, shift_end_utc, current_target_model, current_loss_identifier)

    def _classify_telemetry_event(self, workcenter, machine_settings, tag_name, value):
        plc_value = int(float(value)) if value is not None else 0
        
        event = self._resolve_event(machine_settings, tag_name, plc_value)
        if not event:
            return None, None

        is_running_state = False
        if workcenter.runtime_event_id:
            running_signals = machine_settings.event_tag_ids.filtered(lambda tag_config: tag_config.event_id == workcenter.runtime_event_id)
            if running_signals:
                is_running_state = (running_signals[0].tag_name == tag_name and running_signals[0].plc_value == plc_value)
            else:
                is_running_state = (workcenter.runtime_event_id.default_event_tag_type == tag_name and workcenter.runtime_event_id.default_plc_value == plc_value)

        if is_running_state:
            return 'mes.performance.running', workcenter.runtime_event_id.id

        stop_tag = 'OEE.nStopRootReason'
        if hasattr(machine_settings, 'get_alarm_tag_name'):
            stop_tag = machine_settings.get_alarm_tag_name('OEE.nStopRootReason').replace('%', '')

        if tag_name == stop_tag or tag_name == 'OEE.nStopRootReason':
            return 'mes.performance.alarm', event.id

        return 'mes.performance.slowing', event.id

    def _convert_raw_time_to_utc(self, raw_time, local_timezone):
        if not raw_time:
            return None
        if isinstance(raw_time, str):
            cleaned = raw_time.replace('T', ' ').replace('Z', '')
            try:
                event_datetime = datetime.strptime(cleaned[:26], '%Y-%m-%d %H:%M:%S.%f')
            except ValueError:
                event_datetime = fields.Datetime.to_datetime(cleaned[:19])
        else:
            event_datetime = raw_time.replace(tzinfo=None)
            
        return local_timezone.localize(event_datetime).astimezone(pytz.utc).replace(tzinfo=None)

    def _handle_shift_overflow(self, workcenter, shift_end_utc, target_model, loss_identifier):
        next_document, is_new = self._get_or_create_doc(workcenter, shift_end_utc + timedelta(seconds=1))
            
        if is_new:
            return
            
        if next_document and next_document.state == 'draft':
            existing_seed_record = self.env[target_model].search(
            [
                ('performance_id', '=', next_document.id),
                ('start_time', '=', shift_end_utc),
                ('end_time', '=', False)
            ])
            
            if not existing_seed_record:
                self.env[target_model].create(
                {
                    'performance_id': next_document.id,
                    'loss_id': loss_identifier,
                    'start_time': shift_end_utc
                })

    def _process_telemetry_counts(self, machine_settings, start_time_string, end_time_string):
        production_values = []
        rejection_values = []

        count_query = """
            SELECT 
                t.tag_name, 
                COALESCE(SUM(t.value), 0) AS sum_val, 
                COALESCE(MAX(t.value), 0) - CASE
                    WHEN (SELECT value FROM telemetry_count start_t
                          WHERE start_t.machine_name = %s
                            AND start_t.tag_name = t.tag_name
                            AND start_t.time <= %s
                          ORDER BY start_t.time DESC LIMIT 1) <= COALESCE(MIN(t.value), 0)
                    THEN (SELECT value FROM telemetry_count start_t
                          WHERE start_t.machine_name = %s
                            AND start_t.tag_name = t.tag_name
                            AND start_t.time <= %s
                          ORDER BY start_t.time DESC LIMIT 1)
                    ELSE COALESCE(MIN(t.value), 0)
                END AS cum_val
            FROM telemetry_count t
            WHERE t.machine_name = %s AND t.time > %s AND t.time <= %s
            GROUP BY t.tag_name
        """

        with self.env['mes.timescale.base']._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(count_query, (machine_settings.name, start_time_string, machine_settings.name, start_time_string, machine_settings.name, start_time_string, end_time_string))
                
                for row_data in cursor.fetchall():
                    tag_name, sum_value, cumulative_value = row_data
                    tag_configuration = machine_settings.count_tag_ids.filtered(lambda tag_setting: tag_setting.tag_name == tag_name)
                    
                    if tag_configuration:
                        quantity = cumulative_value if tag_configuration[0].is_cumulative else sum_value
                        
                        if quantity > 0:
                            count_dictionary_id = tag_configuration[0].count_id
                            value_dictionary = \
                            {
                                'performance_id': self.id,
                                'qty': float(quantity),
                                'reason_id': count_dictionary_id.id
                            }
                            
                            if count_dictionary_id == self.machine_id.production_count_id:
                                production_values.append(value_dictionary)
                            else:
                                rejection_values.append(value_dictionary)

        if production_values:
            self.env['mes.performance.production'].create(production_values)
            
        if rejection_values:
            self.env['mes.performance.rejection'].create(rejection_values)

    def _is_empty_shift(self):
        has_production_records = bool(self.production_ids)
        has_running_records = bool(self.running_ids)
        
        return not (has_production_records or has_running_records)
    
    def _get_or_create_doc(self, workcenter, timestamp_utc):
        machine_timezone = pytz.timezone(workcenter.company_id.tz or 'UTC')
        timestamp_local = pytz.utc.localize(timestamp_utc).astimezone(machine_timezone).replace(tzinfo=None)
        
        current_hour_decimal = timestamp_local.hour + timestamp_local.minute / 60.0 + timestamp_local.second / 3600.0
        available_shifts = self.env['mes.shift'].search([('company_id', '=', workcenter.company_id.id)])
        valid_shifts = [shift for shift in available_shifts if not (shift.workcenter_ids and workcenter.id not in shift.workcenter_ids.ids)]
        
        current_shift = None
        
        for shift in valid_shifts:
            if (shift.start_hour < shift.end_hour and shift.start_hour <= current_hour_decimal < shift.end_hour) or \
               (shift.start_hour >= shift.end_hour and (current_hour_decimal >= shift.start_hour or current_hour_decimal < shift.end_hour)):
                current_shift = shift
                break

        if not current_shift:
            return None, False
            
        start_date = timestamp_local.date()
        
        if current_shift.start_hour > current_shift.end_hour and current_hour_decimal < current_shift.end_hour:
            start_date -= timedelta(days=1)
            
        document = self.search(
        [
            ('machine_id', '=', workcenter.id), 
            ('shift_id', '=', current_shift.id), 
            ('date', '=', start_date)
        ], limit=1)
        
        is_new = False
        
        if not document:
            document = self.create(
            {
                'machine_id': workcenter.id, 
                'shift_id': current_shift.id, 
                'date': start_date
            })
            is_new = True
            
            shift_start_loc, shift_end_loc = document._get_local_shift_times()
            shift_end_utc = document._get_utc_time(shift_end_loc)
            
            now_utc = fields.Datetime.now()
            if now_utc < shift_end_utc:
                calc_end_utc = now_utc
                calc_end_loc = pytz.utc.localize(now_utc).astimezone(machine_timezone).replace(tzinfo=None)
            else:
                calc_end_utc = shift_end_utc
                calc_end_loc = shift_end_loc
                
            start_str = shift_start_loc.strftime('%Y-%m-%d %H:%M:%S.%f')
            end_str = calc_end_loc.strftime('%Y-%m-%d %H:%M:%S.%f')
            
            mac = workcenter.machine_settings_id
            if mac:
                evts = document._fetch_telemetry_events(workcenter, mac, start_str, end_str, include_initial=True)
                document._process_telemetry_events(workcenter, mac, evts, calc_end_utc)
                document._process_telemetry_counts(mac, start_str, end_str)

        return document, is_new

    def _get_local_shift_times(self):
        self.ensure_one()
        s_time = datetime.combine(
            self.date,
            time(hour=int(self.shift_id.start_hour), minute=int(round((self.shift_id.start_hour % 1) * 60)))
        )
        e_time = s_time + timedelta(hours=self.shift_id.duration)
        return s_time, e_time

    def _get_utc_time(self, local_naive_dt):
        self.ensure_one()
        tz_name = self.machine_id.company_id.tz or 'UTC'
        mac_tz = pytz.timezone(tz_name)
        local_dt = mac_tz.localize(local_naive_dt.replace(tzinfo=None))
        return local_dt.astimezone(pytz.utc).replace(tzinfo=None)

    def _process_historical_shift_job(self, params):
        workcenter = self.env['mrp.workcenter'].browse(params['workcenter_id'])
        shift = self.env['mes.shift'].browse(params['shift_id'])
        target_date = fields.Date.from_string(params['target_date'])
        doc = self._prepare_historical_doc(workcenter, shift, target_date)
        shift_start_loc, shift_end_loc = doc._get_local_shift_times()
        shift_end_utc = doc._get_utc_time(shift_end_loc)
        doc._close_shift_and_calculate_totals(shift_start_loc, shift_end_loc, shift_end_utc)
        self._maybe_finalize_hist_sync(workcenter)

    def _prepare_historical_doc(self, workcenter, shift, target_date):
        doc = self.search([
            ('machine_id', '=', workcenter.id),
            ('shift_id', '=', shift.id),
            ('date', '=', target_date),
        ], limit=1)
        if doc:
            (doc.alarm_ids | doc.running_ids | doc.slowing_ids
             | doc.production_ids | doc.rejection_ids).unlink()
            doc.write({'state': 'draft'})
            return doc
        return self.create({
            'machine_id': workcenter.id,
            'shift_id': shift.id,
            'date': target_date,
        })

    def _maybe_finalize_hist_sync(self, workcenter):
        Job = self.env.get('queue.job')
        if Job is None:
            workcenter.write({'is_hist_syncing': False})
            return
        remaining = Job.search_count([
            ('channel', '=', 'root.mes_hist'),
            ('state', 'in', ['pending', 'enqueued']),
            ('identity_key', 'like', f"hist_{workcenter.id}_%"),
        ])
        if remaining == 0 and workcenter.is_hist_syncing:
            workcenter.write({'is_hist_syncing': False})

    def _recalc_downtimes_job(self, doc_id):
        doc = self.browse(doc_id).exists()
        if not doc:
            return
        machine_settings = doc.machine_id.machine_settings_id
        if not machine_settings:
            return
        shift_start_loc, shift_end_loc = doc._get_local_shift_times()
        mac_tz = pytz.timezone(doc.machine_id.company_id.tz or 'UTC')
        now_local = pytz.utc.localize(fields.Datetime.now()).astimezone(mac_tz).replace(tzinfo=None)
        calc_end_loc = min(shift_end_loc, now_local)
        reason_tag = machine_settings.get_alarm_tag_name('OEE.nStopRootReason').replace('%', '')
        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT time, tag_name, value
                    FROM telemetry_event
                    WHERE machine_name = %s AND time >= %s AND time <= %s
                    ORDER BY time ASC
                """, (machine_settings.name, shift_start_loc, calc_end_loc))
                rows = cur.fetchall()
        doc.alarm_ids.unlink()
        alarms_to_create = []
        for idx, (ts_raw, tag, val) in enumerate(rows):
            if tag != reason_tag:
                continue
            plc_val = self._safe_int(val)
            if plc_val is None or plc_val == 0:
                continue
            end_raw = rows[idx + 1][0] if idx + 1 < len(rows) else calc_end_loc
            start_naive = self._strip_tzinfo(ts_raw)
            end_naive = self._strip_tzinfo(end_raw)
            evt = self._resolve_event(machine_settings, tag, plc_val)
            if not evt:
                continue
            alarms_to_create.append({
                'performance_id': doc.id,
                'loss_id': evt.id,
                'start_time': doc._get_utc_time(start_naive),
                'end_time': doc._get_utc_time(end_naive),
            })
        if alarms_to_create:
            self.env['mes.performance.alarm'].create(alarms_to_create)

    @staticmethod
    def _safe_int(value):
        if value is None:
            return None
        try:
            return int(float(str(value).strip()))
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _strip_tzinfo(dt_value):
        if hasattr(dt_value, 'replace') and getattr(dt_value, 'tzinfo', None):
            return dt_value.replace(tzinfo=None)
        return dt_value


class MesPerformanceAlarm(models.Model):
    _name = 'mes.performance.alarm'
    _description = 'Machine Alarms'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True,
                                     index=True)
    loss_id = fields.Many2one('mes.event', string='Alarm Reason', required=True)
    start_time = fields.Datetime(string='Start Time', index=True)
    end_time = fields.Datetime(string='End Time', index=True)
    duration = fields.Float(string='Duration (Min)', compute='_compute_duration', store=True)
    comment = fields.Char(string='Comment')
    reason_provisional = fields.Boolean(
        string='Reason Pending',
        default=False,
        help="The stop was recorded before its root reason arrived from the PLC; "
             "the reason is refined by the FSM cron as soon as it lands.")

    @api.depends('start_time', 'end_time')
    def _compute_duration(self):
        for rec in self:
            if rec.start_time and rec.end_time:
                delta = rec.end_time - rec.start_time
                rec.duration = delta.total_seconds() / 60.0
            else:
                rec.duration = 0.0

    def init(self):
        self.env.cr.execute(
            'CREATE INDEX IF NOT EXISTS mes_performance_alarm_open_idx '
            'ON mes_performance_alarm (performance_id) WHERE end_time IS NULL'
        )


class MesPerformanceRunning(models.Model):
    _name = 'mes.performance.running'
    _description = 'Machine Runnings'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True,
                                     index=True)
    loss_id = fields.Many2one('mes.event', string='Activity Type', required=True) 
    start_time = fields.Datetime(string='Start Time', index=True)
    end_time = fields.Datetime(string='End Time', index=True)
    duration = fields.Float(string='Duration (Min)', compute='_compute_duration', store=True)
    comment = fields.Char(string='Comment')

    @api.depends('start_time', 'end_time')
    def _compute_duration(self):
        for rec in self:
            if rec.start_time and rec.end_time:
                delta = rec.end_time - rec.start_time
                rec.duration = delta.total_seconds() / 60.0
            else:
                rec.duration = 0.0

    def init(self):
        self.env.cr.execute(
            'CREATE INDEX IF NOT EXISTS mes_performance_running_open_idx '
            'ON mes_performance_running (performance_id) WHERE end_time IS NULL'
        )


class MesPerformanceSlowing(models.Model):
    _name = 'mes.performance.slowing'
    _description = 'Machine Slowing Logs'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True,
                                     index=True)
    loss_id = fields.Many2one('mes.event', string='Slowing Reason') 
    start_time = fields.Datetime(string='Start Time', index=True)
    end_time = fields.Datetime(string='End Time', index=True)
    duration = fields.Float(string='Duration (Min)', compute='_compute_duration', store=True)
    comment = fields.Char(string='Comment')

    @api.depends('start_time', 'end_time')
    def _compute_duration(self):
        for rec in self:
            if rec.start_time and rec.end_time:
                delta = rec.end_time - rec.start_time
                rec.duration = delta.total_seconds() / 60.0
            else:
                rec.duration = 0.0

    def init(self):
        self.env.cr.execute(
            'CREATE INDEX IF NOT EXISTS mes_performance_slowing_open_idx '
            'ON mes_performance_slowing (performance_id) WHERE end_time IS NULL'
        )


class MesPerformanceRejection(models.Model):
    _name = 'mes.performance.rejection'
    _description = 'Machine Rejections'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True)
    product_id = fields.Many2one('product.product', string='Product', required=False)
    qty = fields.Float(string='Quantity', default=0.0)
    reason_id = fields.Many2one('mes.counts', string='Rejection Reason') 

class MesPerformanceProduction(models.Model):
    _name = 'mes.performance.production'
    _description = 'Machine Production'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade', required=True)
    product_id = fields.Many2one('product.product', string='Product', required=False)
    qty = fields.Float(string='Quantity', default=0.0)
    reason_id = fields.Many2one('mes.counts', string='Count Type')
