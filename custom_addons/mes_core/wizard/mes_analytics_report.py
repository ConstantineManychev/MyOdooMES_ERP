from odoo import models, fields, api
from datetime import datetime, time, timedelta
import pytz

class MesAnalyticsWizard(models.TransientModel):
    _name = 'mes.analytics.wizard'
    _description = 'Shift Analytics Wizard'

    @api.model
    def default_get(self, fields_list):
        res = super().default_get(fields_list)
        tz = pytz.timezone(self.env.user.tz or 'UTC')
        now = datetime.now(tz)
        today = now.date()

        shifts = self.env['mes.shift'].search([], order='start_hour asc')
        if shifts:
            morning_shift = shifts[0]
            night_shift = shifts[-1]

            h_s = int(morning_shift.start_hour)
            m_s = int((morning_shift.start_hour - h_s) * 60)
            start_local = tz.localize(datetime.combine(today, time(h_s, m_s)))
            res['start_datetime'] = start_local.astimezone(pytz.UTC).replace(tzinfo=None)

            h_e = int(night_shift.end_hour)
            m_e = int((night_shift.end_hour - h_e) * 60)
            end_local = tz.localize(datetime.combine(today, time(h_e, m_e)))
            
            if night_shift.end_hour <= night_shift.start_hour:
                end_local += timedelta(days=1)
                
            res['end_datetime'] = end_local.astimezone(pytz.UTC).replace(tzinfo=None)
        else:
            res['start_datetime'] = datetime.now().replace(hour=0, minute=0, second=0)
            res['end_datetime'] = datetime.now().replace(hour=23, minute=59, second=59)
            
        return res

    start_datetime = fields.Datetime(string="Start Date & Time", required=True)
    end_datetime = fields.Datetime(string="End Date & Time", required=True)

    def action_generate_report(self):
        self.env['mes.analytics.report.line'].search([('user_id', '=', self.env.user.id)]).unlink()

        workcenters = self.env['mrp.workcenter'].search([('machine_settings_id', '!=', False)])
        shifts = self.env['mes.shift'].search([], order='start_hour asc')

        tz = pytz.timezone(self.env.user.tz or 'UTC')
        start_dt = pytz.UTC.localize(self.start_datetime).astimezone(tz)
        end_dt = pytz.UTC.localize(self.end_datetime).astimezone(tz)

        current_date = start_dt.date()
        end_date = end_dt.date()

        lines_to_create = []

        while current_date <= end_date:
            for shift in shifts:
                h_s = int(shift.start_hour)
                m_s = int((shift.start_hour - h_s) * 60)
                shift_start_local = tz.localize(datetime.combine(current_date, time(h_s, m_s)))

                h_e = int(shift.end_hour)
                m_e = int((shift.end_hour - h_e) * 60)
                shift_end_local = tz.localize(datetime.combine(current_date, time(h_e, m_e)))

                if shift.end_hour <= shift.start_hour:
                    shift_end_local += timedelta(days=1)

                if shift_start_local < end_dt and shift_end_local > start_dt:
                    actual_start_utc = max(shift_start_local, start_dt).astimezone(pytz.UTC).replace(tzinfo=None)
                    actual_end_utc = min(shift_end_local, end_dt).astimezone(pytz.UTC).replace(tzinfo=None)

                    shift_date_name = f"{shift_start_local.strftime('%Y-%m-%d %H:%M')} [{shift.name}]"

                    for wc in workcenters:
                        kpi = wc.machine_settings_id._calculate_kpi_for_window(wc, actual_start_utc, actual_end_utc)
                        if kpi:
                            lines_to_create.append({
                                'user_id': self.env.user.id,
                                'workcenter_id': wc.id,
                                'date': current_date,
                                'shift_id': shift.id,
                                'shift_date_name': shift_date_name,
                                'first_running_time': kpi.get('first_running_time', False),
                                'oee': kpi.get('oee', 0),
                                'availability': kpi.get('availability', 0),
                                'performance': kpi.get('performance', 0),
                                'quality': kpi.get('quality', 0),
                                'produced': kpi.get('produced', 0),
                                'waste_losses': kpi.get('waste_losses', 0),
                                'downtime_losses': kpi.get('downtime_losses', 0),
                            })
            current_date += timedelta(days=1)

        if lines_to_create:
            self.env['mes.analytics.report.line'].create(lines_to_create)

        return {
            'name': 'Shift Analytics Report',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.analytics.report.line',
            'view_mode': 'tree,pivot',
            'domain': [('user_id', '=', self.env.user.id)],
            'context': {
                'search_default_group_by_machine': 1,
                'search_default_group_by_shift_date': 1,
            }
        }


class MesAnalyticsReportLine(models.Model):
    _name = 'mes.analytics.report.line'
    _description = 'Analytics Report Line'

    user_id = fields.Many2one('res.users', string="User")
    workcenter_id = fields.Many2one('mrp.workcenter', string="Machine")
    date = fields.Date(string="Date")
    shift_id = fields.Many2one('mes.shift', string="Shift")
    shift_date_name = fields.Char(string="Date / Shift")
    first_running_time = fields.Datetime(string="First Start")

    oee = fields.Float("OEE (%)", group_operator="avg")
    availability = fields.Float("Availability (%)", group_operator="avg")
    performance = fields.Float("Performance (%)", group_operator="avg")
    quality = fields.Float("Quality (%)", group_operator="avg")
    produced = fields.Float("Produced", group_operator="sum")
    waste_losses = fields.Float("Waste Loss (%)", group_operator="avg")
    downtime_losses = fields.Float("Downtime Loss (%)", group_operator="avg")