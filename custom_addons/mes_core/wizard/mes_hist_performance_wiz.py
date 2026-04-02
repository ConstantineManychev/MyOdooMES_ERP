from datetime import datetime, timedelta, time
import pytz
from odoo import models, fields

class MesHistPerformanceWiz(models.TransientModel):
    _name = 'mes.hist.performance.wiz'
    _description = 'Historical Shift Generator'

    start_date = fields.Datetime(string='Start DateTime', required=True)
    end_date = fields.Datetime(string='End DateTime', required=True, default=fields.Datetime.now)
    machine_ids = fields.Many2many('mrp.workcenter', string='Machines', required=True)

    def action_generate(self):
        now_utc = fields.Datetime.now()
        for wc in self.machine_ids:
            shifts = self.env['mes.shift'].search([('company_id', '=', wc.company_id.id)])
            val_shifts = [s for s in shifts if not (s.workcenter_ids and wc.id not in s.workcenter_ids.ids)]
            
            curr_t = self.start_date
            while curr_t <= self.end_date:
                self._process_daily_shifts(wc, val_shifts, curr_t.date(), now_utc)
                curr_t += timedelta(days=1)

    def _process_daily_shifts(self, wc, val_shifts, target_date, now_utc):
        for shift in val_shifts:
            s_loc, e_loc = self._calc_window(shift, target_date)
            s_utc = self._get_utc(wc, s_loc)
            e_utc = self._get_utc(wc, e_loc)

            if s_utc < self.start_date or s_utc > self.end_date:
                continue

            is_cur = s_utc <= now_utc < e_utc
            calc_e_utc = now_utc if is_cur else e_utc

            doc = self._prepare_doc(wc, shift, target_date)
            doc._init_initial_state(s_utc)
            doc._process_historical_events(s_utc, calc_e_utc)
            
            if not is_cur:
                doc._close_open_events(calc_e_utc)
                doc._process_telemetry_counts(s_loc, e_loc)
                if doc._is_empty_shift():
                    doc.unlink()
                else:
                    doc.write({'state': 'done'})

    def _calc_window(self, shift, target_date):
        s_t = datetime.combine(
            target_date,
            time(hour=int(shift.start_hour), minute=int((shift.start_hour % 1) * 60))
        )
        e_t = s_t + timedelta(hours=shift.duration)
        return s_t, e_t

    def _get_utc(self, wc, loc_naive):
        tz_name = wc.company_id.tz or 'UTC'
        tz = pytz.timezone(tz_name)
        loc_dt = tz.localize(loc_naive, is_dst=False)
        return loc_dt.astimezone(pytz.utc).replace(tzinfo=None)

    def _prepare_doc(self, wc, shift, target_date):
        doc = self.env['mes.machine.performance'].search([
            ('machine_id', '=', wc.id),
            ('shift_id', '=', shift.id),
            ('date', '=', target_date)
        ], limit=1)
        
        if not doc:
            doc = self.env['mes.machine.performance'].create({
                'machine_id': wc.id,
                'shift_id': shift.id,
                'date': target_date
            })
        else:
            doc.alarm_ids.unlink()
            doc.running_ids.unlink()
            doc.slowing_ids.unlink()
            doc.production_ids.unlink()
            doc.rejection_ids.unlink()
            doc.write({'state': 'draft'})
            
        return doc