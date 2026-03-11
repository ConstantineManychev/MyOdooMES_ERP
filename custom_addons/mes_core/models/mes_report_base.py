from odoo import models, fields, api
from datetime import datetime, time, timedelta
import pytz

class MesReportBaseWizard(models.TransientModel):
    _name = 'mes.report.base.wizard'
    _description = 'Base SKD Report Engine'

    start_datetime = fields.Datetime(string="Start Date & Time", required=True)
    end_datetime = fields.Datetime(string="End Date & Time", required=True)
    
    machine_filter_type = fields.Selection([
        ('in', 'In List'),
        ('not_in', 'Not in List')
    ], string="Machine Condition", default='in', required=True)
    machine_ids = fields.Many2many('mes.machine.settings', string="Machines")
    
    time_scale = fields.Selection([
        ('shift', 'By Shift (Date + Shift)'),
        ('day', 'By Logical Day'),
        ('month', 'By Month'),
        ('period', 'Entire Selected Period')
    ], string="Time Aggregation", default='month', required=True)

    record_limit = fields.Integer(default=0)    

    limit_by = fields.Selection(
            selection='_get_limit_by_options',
            default='total_time',
            required=True
        )

    row_by_machine = fields.Boolean("Machine", default=True)
    row_by_period = fields.Boolean("Period", default=False)
    col_by_machine = fields.Boolean("Machine", default=False)
    col_by_period = fields.Boolean("Period", default=True)

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        tz = pytz.timezone(self.env.user.tz or 'UTC')
        today = datetime.now(tz).date()

        shifts = self.env['mes.shift'].search([], order='start_hour asc')
        if shifts:
            h_s = int(shifts[0].start_hour)
            m_s = int((shifts[0].start_hour - h_s) * 60)
            res['start_datetime'] = tz.localize(datetime.combine(today, time(h_s, m_s))).astimezone(pytz.UTC).replace(tzinfo=None)

            h_e = int(shifts[-1].end_hour)
            m_e = int((shifts[-1].end_hour - h_e) * 60)
            end_local = tz.localize(datetime.combine(today, time(h_e, m_e)))
            if shifts[-1].end_hour <= shifts[-1].start_hour:
                end_local += timedelta(days=1)
            res['end_datetime'] = end_local.astimezone(pytz.UTC).replace(tzinfo=None)
        else:
            res['start_datetime'] = datetime.now().replace(hour=0, minute=0, second=0)
            res['end_datetime'] = datetime.now().replace(hour=23, minute=59, second=59)
        return res

    @api.model
    def _get_limit_by_options(self):
        return [
            ('total_time', 'Total Time')
        ]
    
    def _get_filtered_machines(self):
        domain = []
        if self.machine_ids:
            operator = 'in' if self.machine_filter_type == 'in' else 'not in'
            domain.append(('id', operator, self.machine_ids.ids))
        return self.env['mes.machine.settings'].search(domain)

    def _is_item_allowed(self, item_id, filter_ids, filter_type):
        if not filter_ids:
            return True  
        if filter_type == 'in':
            return item_id in filter_ids
        else:
            return item_id not in filter_ids

    def _get_logical_periods(self, start_dt_utc, end_dt_utc, shifts):
        tz = pytz.timezone(self.env.user.tz or 'UTC')
        start_dt_local = pytz.UTC.localize(start_dt_utc).astimezone(tz)
        end_dt_local = pytz.UTC.localize(end_dt_utc).astimezone(tz)

        current_date = (start_dt_local - timedelta(days=1)).date()
        end_date = (end_dt_local + timedelta(days=1)).date()
        
        periods_dict = {}
        while current_date <= end_date:
            for shift in shifts:
                h_s = int(shift.start_hour)
                m_s = int((shift.start_hour - h_s) * 60)
                shift_s_loc = tz.localize(datetime.combine(current_date, time(h_s, m_s)))

                h_e = int(shift.end_hour)
                m_e = int((shift.end_hour - h_e) * 60)
                shift_e_loc = tz.localize(datetime.combine(current_date, time(h_e, m_e)))

                if shift.end_hour <= shift.start_hour:
                    shift_e_loc += timedelta(days=1)

                if shift_s_loc < end_dt_local and shift_e_loc > start_dt_local:
                    actual_s = max(shift_s_loc, start_dt_local).astimezone(pytz.UTC).replace(tzinfo=None)
                    actual_e = min(shift_e_loc, end_dt_local).astimezone(pytz.UTC).replace(tzinfo=None)
                    
                    if self.time_scale == 'shift': 
                        p_name = f"{current_date.strftime('%Y-%m-%d')} {h_s:02d}:{m_s:02d} [{shift.name}]"
                    elif self.time_scale == 'day': 
                        p_name = current_date.strftime('%Y-%m-%d')
                    elif self.time_scale == 'month': 
                        p_name = current_date.strftime('%Y-%B')
                    else: 
                        p_name = f"Total Period"
                        
                    if p_name not in periods_dict:
                        periods_dict[p_name] = []
                    periods_dict[p_name].append((actual_s, actual_e))
            current_date += timedelta(days=1)
        return periods_dict

    def _merge_intervals(self, intervals):
        if not intervals: return []
        intervals.sort(key=lambda x: x[0])
        merged = [intervals[0]]
        for current in intervals[1:]:
            last = merged[-1]
            if current[0] <= last[1]: merged[-1] = (last[0], max(last[1], current[1]))
            else: merged.append(current)
        return merged

    def _build_skd_context(self, measures):
        return {
            'pivot_row_groupby': ['row_group_label'],
            'pivot_column_groupby': ['col_group_label'],
            'pivot_measures': measures,
        }

    def action_generate_report(self):
        pass