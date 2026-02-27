import re
from datetime import datetime, timedelta
from odoo import models, fields, api

class MesMachineSettings(models.Model):
    _name = 'mes.machine.settings'
    _description = 'Machine Connection Settings'
    _inherit = ['mail.thread', 'mail.activity.mixin', 'mes.timescale.base']

    name = fields.Char(string='Machine Name', required=True, copy=False, tracking=True)
    ip_connection = fields.Char(string='Connection IP', tracking=True)
    ip_data = fields.Char(string='TwinCAT/Data IP', tracking=True)
    
    count_tag_ids = fields.One2many('mes.signal.count', 'machine_id', string='Counts')
    event_tag_ids = fields.One2many('mes.signal.event', 'machine_id', string='Events')
    process_tag_ids = fields.One2many('mes.signal.process', 'machine_id', string='Processes')

    _sql_constraints = [('name_uniq', 'unique (name)', 'Machine Name must be unique!')]

    def init(self):
        if hasattr(self.env['mes.timescale.db.manager'], '_init_DB'):
            self.env['mes.timescale.db.manager']._init_DB()
            self.env['mes.timescale.db.manager']._init_local_fdw()

    @api.model
    def create(self, vals):
        rec = super().create(vals)
        self._execute_from_file('upsert_machine.sql', (rec.name, rec.ip_connection, rec.ip_data))
        return rec

    def write(self, vals):
        res = super().write(vals)
        for rec in self:
            self._execute_from_file('upsert_machine.sql', (rec.name, rec.ip_connection, rec.ip_data))
        return res

    def unlink(self):
        for rec in self:
            self._execute_from_file('delete_machine.sql', (rec.name,))
        return super().unlink()

    def _get_current_shift_window(self):
        now = fields.Datetime.now()
        current_hour = now.hour + now.minute / 60.0 + now.second / 3600.0
        
        shifts = self.env['mes.shift'].search([])
        current_shift = None
        
        for shift in shifts:
            if shift.start_hour < shift.end_hour:
                if shift.start_hour <= current_hour < shift.end_hour:
                    current_shift = shift
                    break
            else:
                if current_hour >= shift.start_hour or current_hour < shift.end_hour:
                    current_shift = shift
                    break
        
        if not current_shift:
            return None, None

        start_date = now
        if current_shift.start_hour > current_shift.end_hour and current_hour < current_shift.end_hour:
            start_date = now - timedelta(days=1)
            
        start_time = start_date.replace(
            hour=int(current_shift.start_hour), 
            minute=int((current_shift.start_hour % 1) * 60), 
            second=0, 
            microsecond=0
        )
        
        end_time = start_time + timedelta(hours=current_shift.duration)
        
        return start_time, end_time

    def get_alarm_tag_name(self, default_type='OEE.nStopRootReason'):
        self.ensure_one()
        override = self.env['mes.signal.event'].search([
            ('machine_id', '=', self.id),
            ('event_id.default_event_tag_type', '=', default_type)
        ], limit=1)
        
        if override and override.tag_name:
            return override.tag_name
        
        return f"%{default_type}%"

    def resolve_plc_value_to_name(self, plc_value):
        self.ensure_one()
        plc_str = str(plc_value)
        
        if not plc_str.isdigit():
            return plc_str
            
        plc_int = int(plc_str)
        
        override = self.env['mes.signal.event'].search([
            ('machine_id', '=', self.id),
            ('plc_value', '=', plc_int)
        ], limit=1)
        
        if override and override.event_id:
            return override.event_id.name
            
        dict_event = self.env['mes.event'].search([
            ('default_plc_value', '=', plc_int)
        ], limit=1)
        
        if dict_event:
            return dict_event.name
            
        return plc_str
    
    def get_top_alarm_str(self, cursor, start_time, end_time):
        alarm_tag = self.get_alarm_tag_name('OEE.nStopRootReason')

        alarm_query = f"""
            WITH alarm_boundary AS (
                SELECT %s::timestamptz as time, value::text FROM telemetry_event
                WHERE machine_name = %s AND tag_name LIKE %s AND time < %s ORDER BY time DESC LIMIT 1
            ),
            alarm_events AS (
                SELECT time, value::text FROM alarm_boundary UNION ALL
                SELECT time, value::text FROM telemetry_event
                WHERE machine_name = %s AND tag_name LIKE %s AND time >= %s AND time <= %s
            ),
            alarm_durations AS (
                SELECT value as alarm_code, EXTRACT(EPOCH FROM (COALESCE(LEAD(time) OVER (ORDER BY time), %s) - time)) as duration_sec
                FROM alarm_events
            )
            SELECT alarm_code, SUM(duration_sec) as total_dur FROM alarm_durations
            WHERE alarm_code != '0' AND alarm_code != '' AND alarm_code IS NOT NULL
            GROUP BY alarm_code ORDER BY total_dur DESC LIMIT 1;
        """
        cursor.execute(alarm_query, (
            start_time, self.name, alarm_tag, start_time,
            self.name, alarm_tag, start_time, end_time, end_time
        ))
        res_al = cursor.fetchone()
        
        if res_al and res_al[0]:
            duration_min = int((res_al[1] or 0) // 60)
            alarm_name = self.resolve_plc_value_to_name(res_al[0])
            return f"{alarm_name} ({duration_min} min)"
            
        return "None"

    def get_top_rejection_str(self, cursor, start_time, end_time, good_production_count_id):
        top_rej_count = 0
        top_rej_name = "None"
        
        all_reject_counts = self.env['mes.counts'].search([('id', '!=', good_production_count_id)])
        
        for count_def in all_reject_counts:
            tag, is_cum = count_def.get_count_config_for_machine(self)
            if not tag:
                continue
                
            if is_cum:
                q_rej = "SELECT COALESCE(MAX(value) - MIN(value), 0) FROM telemetry_count WHERE machine_name=%s AND tag_name=%s AND time >= %s AND time <= %s"
            else:
                q_rej = "SELECT COALESCE(SUM(value), 0) FROM telemetry_count WHERE machine_name=%s AND tag_name=%s AND time >= %s AND time <= %s"
            
            cursor.execute(q_rej, (self.name, tag, start_time, end_time))
            val = cursor.fetchone()[0] or 0
            
            if val > top_rej_count:
                top_rej_count = val
                top_rej_name = count_def.name
                
        if top_rej_count > 0:
            return f"{top_rej_name} ({int(top_rej_count)})"
        return "None"

    def _build_downtime_cte(self, start_time, end_time, workcenter):
        if not workcenter:
            empty_cte = "SELECT '1970-01-01 UTC'::timestamptz as dt_start, '1970-01-01 UTC'::timestamptz as dt_end WHERE FALSE"
            return empty_cte, 0.0

        downtimes = self.env['mes.flat.downtime'].search([
            ('machine_id', '=', workcenter.id),
            ('start_time', '<', end_time),
            ('end_time', '>', start_time)
        ])
        
        dt_values = []
        total_planned_downtime_sec = 0.0
        
        for dt in downtimes:
            dt_values.append(f"('{dt.start_time} UTC'::timestamptz, '{dt.end_time} UTC'::timestamptz)")
            
            dt_s = max(dt.start_time, start_time)
            dt_e = min(dt.end_time, end_time)
            if dt_e > dt_s:
                total_planned_downtime_sec += (dt_e - dt_s).total_seconds()
                
        if dt_values:
            dt_cte = "SELECT * FROM (VALUES " + ", ".join(dt_values) + ") AS dt(dt_start, dt_end)"
        else:
            dt_cte = "SELECT '1970-01-01 UTC'::timestamptz as dt_start, '1970-01-01 UTC'::timestamptz as dt_end WHERE FALSE"
            
        return dt_cte, total_planned_downtime_sec

    def _fetch_raw_oee_data(self, cursor, start_time, end_time, state_tag, plc_value, good_count_tag, is_cumulative, dt_cte):
        prod_sql = "SELECT COALESCE(MAX(value) - MIN(value), 0) as total_produced" if is_cumulative else "SELECT COALESCE(SUM(value), 0) as total_produced"
        
        query = f"""
            WITH boundary_state AS (
                SELECT %s::timestamptz as time, value, 0::bigint as id 
                FROM telemetry_event
                WHERE machine_name = %s AND tag_name = %s AND time < %s
                ORDER BY time DESC, id DESC LIMIT 1
            ),
            shift_events AS (
                SELECT time, value, id FROM boundary_state WHERE value = %s
                UNION ALL
                SELECT time, value, id FROM telemetry_event
                WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time <= %s
            ),
            state_durations AS (
                SELECT time as state_start, value as state,
                    COALESCE(LEAD(time) OVER (ORDER BY time ASC, id ASC), %s) as state_end
                FROM shift_events
            ),
            running_durations AS (
                SELECT state_start, state_end FROM state_durations WHERE state = %s
            ),
            planned_downtimes AS ( {dt_cte} ),
            running_with_downtime AS (
                SELECT r.state_start, r.state_end,
                    EXTRACT(EPOCH FROM (r.state_end - r.state_start)) as raw_duration,
                    COALESCE(SUM(
                        CASE WHEN fd.dt_start IS NOT NULL THEN
                            GREATEST(0, EXTRACT(EPOCH FROM (
                                LEAST(r.state_end, fd.dt_end) - GREATEST(r.state_start, fd.dt_start)
                            )))
                        ELSE 0 END
                    ), 0) as planned_downtime_overlap
                FROM running_durations r
                LEFT JOIN planned_downtimes fd ON fd.dt_start < r.state_end AND fd.dt_end > r.state_start
                GROUP BY r.state_start, r.state_end
            ),
            availability_stats AS (
                SELECT COALESCE(SUM(raw_duration - planned_downtime_overlap), 0) as total_running_sec 
                FROM running_with_downtime
            ),
            production_stats AS (
                {prod_sql} FROM telemetry_count
                WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time <= %s
            )
            SELECT a.total_running_sec, p.total_produced
            FROM availability_stats a CROSS JOIN production_stats p;
        """
        params = (
            start_time, self.name, state_tag, start_time,          
            plc_value,                                           
            self.name, state_tag, start_time, end_time,       
            end_time,                                         
            plc_value,                                           
            self.name, good_count_tag, start_time, end_time   
        )
        cursor.execute(query, params)
        res = cursor.fetchone() or (0, 0)
        return (res[0] or 0), (res[1] or 0)

    def _fetch_first_start_time(self, cursor, start_time, end_time, state_tag, plc_value, dt_cte):
        query = f"""
            WITH boundary AS (
                SELECT value FROM telemetry_event
                WHERE machine_name = %s AND tag_name = %s AND time < %s 
                ORDER BY time DESC, id DESC LIMIT 1
            ),
            planned_downtimes AS ( {dt_cte} ),
            first_running_in_shift AS (
                SELECT te.time FROM telemetry_event te
                WHERE te.machine_name = %s AND te.tag_name = %s AND te.time >= %s AND te.time <= %s
                AND te.value = %s
                AND NOT EXISTS (
                    SELECT 1 FROM planned_downtimes fd WHERE te.time >= fd.dt_start AND te.time < fd.dt_end
                )
                ORDER BY te.time ASC, te.id ASC LIMIT 1
            )
            SELECT 
                CASE 
                    WHEN (SELECT value FROM boundary) = %s THEN %s::timestamptz
                    ELSE (SELECT time FROM first_running_in_shift)
                END
        """
        cursor.execute(query, (
            self.name, state_tag, start_time,                                  
            self.name, state_tag, start_time, end_time, plc_value, 
            plc_value, start_time                                            
        ))
        res = cursor.fetchone()
        return res[0].replace(tzinfo=None) if res and res[0] else False

    def _calculate_kpi(self, total_running_sec, total_produced, start_time, end_time, total_planned_dt_sec, workcenter):
        h, m, s = int(total_running_sec // 3600), int((total_running_sec % 3600) // 60), int(total_running_sec % 60)
        runtime_formatted = f"{h:02d}:{m:02d}:{s:02d}"

        planned_production_time_sec = max(0.0, (end_time - start_time).total_seconds() - total_planned_dt_sec)
        ideal_rate_per_sec = (workcenter.ideal_capacity_per_min / 60.0) if (workcenter and workcenter.ideal_capacity_per_min > 0) else 1.0 
            
        raw_availability = total_running_sec / planned_production_time_sec if planned_production_time_sec > 0 else 0
        availability = min(raw_availability, 1.0)
        
        raw_performance = total_produced / (total_running_sec * ideal_rate_per_sec) if total_running_sec > 0 else 0
        performance = min(raw_performance, 1.0)
        
        quality = 1.0 
        oee = availability * performance * quality

        downtime_losses = max(0.0, 1.0 - raw_availability) if planned_production_time_sec > 0 else 0.0
        perfect_amount_for_runtime = total_running_sec * ideal_rate_per_sec
        waste_losses = 1 - (total_produced / perfect_amount_for_runtime) if (planned_production_time_sec > 0 and perfect_amount_for_runtime > 0) else 0.0

        return {
            'availability': round(availability * 100, 2),
            'performance': round(performance * 100, 2),
            'quality': round(quality * 100, 2),
            'oee': round(oee * 100, 2),
            'waste_losses': round(waste_losses * 100, 2),
            'downtime_losses': round(downtime_losses * 100, 2),
            'total_produced': total_produced,
            'runtime_formatted': runtime_formatted,
        }

    def get_realtime_oee(self, runtime_event, production_count, workcenter=None):
        self.ensure_one()
        
        start_time, shift_end = self._get_current_shift_window()
        if not start_time or not shift_end:
            return {'error': 'Configuration error: No active shift. Calculation is not possible.'}
            
        calc_end_time = min(fields.Datetime.now(), shift_end)
        state_tag, running_plc_value = runtime_event.get_mapping_for_machine(self)
        good_count_tag, is_cumulative = production_count.get_count_config_for_machine(self)

        if not state_tag or not good_count_tag:
            return {'error': 'Configuration error: Missing state or count tag for this machine.'}

        if not workcenter:
            workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', self.id)], limit=1)

        dt_cte, total_planned_dt_sec = self._build_downtime_cte(start_time, calc_end_time, workcenter)

        ts_manager = self.env['mes.timescale.base']
        with ts_manager._connection() as conn:
            with conn.cursor() as cur:
                total_running_sec, total_produced = self._fetch_raw_oee_data(
                    cur, start_time, calc_end_time, state_tag, running_plc_value, good_count_tag, is_cumulative, dt_cte
                )
                
                first_running_time = self._fetch_first_start_time(
                    cur, start_time, calc_end_time, state_tag, running_plc_value, dt_cte
                )
                
                top_alarm_str = self.get_top_alarm_str(cur, start_time, calc_end_time)
                top_rejection_str = self.get_top_rejection_str(cur, start_time, calc_end_time, production_count.id)

        kpi_results = self._calculate_kpi(
            total_running_sec, total_produced, start_time, calc_end_time, total_planned_dt_sec, workcenter
        )
        
        kpi_results.update({
            'first_running_time': first_running_time,
            'top_alarm': top_alarm_str,
            'top_rejection': top_rejection_str
        })

        return kpi_results


class MesSignalBase(models.AbstractModel):
    _name = 'mes.signal.base'
    _description = 'Base Signal Config'
    _inherit = ['mes.timescale.base']

    tag_name = fields.Char(string='Signal Tag', required=True)
    poll_type = fields.Selection([('cyclic', 'Cyclic'), ('on_change', 'On Change')], default='cyclic', required=True)
    poll_frequency = fields.Integer(string='Freq (ms)', default=1000)
    param_type = fields.Selection([
        ('auto', 'Auto'), ('bool', 'Boolean'), ('int', 'Integer'),
        ('double', 'Double/Real'), ('string', 'String')
    ], string='Data Type', default='auto', required=True)

    @api.model
    def create(self, vals):
        rec = super().create(vals)
        self._sync(rec)
        return rec

    def write(self, vals):
        res = super().write(vals)
        for rec in self:
            self._sync(rec)
        return res

    def _sync(self, rec):
        self._execute_from_file('upsert_signal.sql', (
            rec.machine_id.name, rec.tag_name, 
            rec.poll_type, rec.poll_frequency, rec.param_type, self._signal_type
        ))


class MesSignalCount(models.Model):
    _name = 'mes.signal.count'
    _inherit = 'mes.signal.base'
    _description = 'Count Signals'
    _signal_type = 'count'

    machine_id = fields.Many2one('mes.machine.settings', string='Machine', required=True, ondelete='cascade')
    count_id = fields.Many2one('mes.counts', string='Dictionary Count', required=True)

    is_cumulative = fields.Boolean(string='Cumulative (MAX-MIN)', default=False)

    _sql_constraints = [
        ('tag_uniq', 'unique(machine_id, tag_name)', 'Count Tag must be unique for this machine!'),
        ('dict_uniq', 'unique(machine_id, count_id)', 'This dictionary count is already added for this machine!')
    ]

    def unlink(self):
        for rec in self:
            self._execute_from_file('delete_signal.sql', (rec.machine_id.name, rec.tag_name))
        return super().unlink()

    @api.onchange('count_id')
    def _onchange_count_id(self):
        if self.count_id:
            self.is_cumulative = self.count_id.is_cumulative


class MesSignalEvent(models.Model):
    _name = 'mes.signal.event'
    _inherit = 'mes.signal.base'
    _description = 'Event Signals'
    _signal_type = 'event'

    machine_id = fields.Many2one('mes.machine.settings', string='Machine', required=True, ondelete='cascade')
    event_id = fields.Many2one('mes.event', string='Dictionary Event', required=True)
    plc_value = fields.Integer(string='PLC Value', required=True)

    _sql_constraints = [
        ('tag_val_uniq', 'unique(machine_id, tag_name, plc_value)', 'Tag + Value pair already exists for this machine!'),
        ('dict_uniq', 'unique(machine_id, event_id)', 'This dictionary event is already configured for this machine!')
    ]

    def unlink(self):
        for rec in self:
            remaining = self.search_count([
                ('machine_id', '=', rec.machine_id.id),
                ('tag_name', '=', rec.tag_name),
                ('id', '!=', rec.id)
            ])
            if remaining == 0:
                self._execute_from_file('delete_signal.sql', (rec.machine_id.name, rec.tag_name))
        return super().unlink()


class MesSignalProcess(models.Model):
    _name = 'mes.signal.process'
    _inherit = 'mes.signal.base'
    _description = 'Process Signals'
    _signal_type = 'process'

    machine_id = fields.Many2one('mes.machine.settings', string='Machine', required=True, ondelete='cascade')
    process_id = fields.Many2one('mes.process', string='Dictionary Process', required=True)

    _sql_constraints = [
        ('tag_uniq', 'unique(machine_id, tag_name)', 'Process Tag must be unique for this machine!'),
        ('dict_uniq', 'unique(machine_id, process_id)', 'This dictionary process is already added for this machine!')
    ]

    def unlink(self):
        for rec in self:
            self._execute_from_file('delete_signal.sql', (rec.machine_id.name, rec.tag_name))
        return super().unlink()