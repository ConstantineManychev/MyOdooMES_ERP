import pytz
import logging
from datetime import datetime, timedelta, time
from odoo import models, fields, api

_logger = logging.getLogger(__name__)


class MesHistPerformanceWiz(models.TransientModel):
    _name = 'mes.hist.performance.wiz'
    _description = 'Historical Shift Generator'

    start_date = fields.Datetime(string='Start DateTime', required=True)
    end_date = fields.Datetime(string='End DateTime', required=True, default=fields.Datetime.now)
    machine_ids = fields.Many2many('mrp.workcenter', string='Machines', required=True)

    def action_generate(self):
        self.ensure_one()
        shifts = self._enumerate_shifts()
        if not shifts:
            return self._notify(0)
        self.machine_ids.write({'is_hist_syncing': True})
        Performance = self.env['mes.machine.performance']
        for params in shifts:
            Performance.with_delay(
                channel='root.mes_hist',
                description=f"Hist shift WC={params['workcenter_id']} {params['target_date']} S={params['shift_id']}",
                identity_key=f"hist_{params['workcenter_id']}_{params['target_date']}_{params['shift_id']}",
            )._process_historical_shift_job(params)
        return self._notify(len(shifts))

    def _enumerate_shifts(self):
        Shift = self.env['mes.shift']
        result = []
        for wc in self.machine_ids:
            allowed = Shift.search([('company_id', '=', wc.company_id.id)]).filtered(
                lambda s: not s.workcenter_ids or wc in s.workcenter_ids
            )
            cursor_date = self.start_date.date()
            while cursor_date <= self.end_date.date():
                for shift in allowed:
                    start_local = self._compose_local_start(cursor_date, shift)
                    start_utc = self._to_utc(wc, start_local)
                    if self.start_date <= start_utc <= self.end_date:
                        result.append({
                            'workcenter_id': wc.id,
                            'shift_id': shift.id,
                            'target_date': fields.Date.to_string(cursor_date),
                        })
                cursor_date += timedelta(days=1)
        result.sort(key=lambda x: (x['target_date'], x['shift_id']))
        return result

    @staticmethod
    def _compose_local_start(target_date, shift):
        hour = int(shift.start_hour)
        minute = int(round((shift.start_hour - hour) * 60))
        return datetime.combine(target_date, time(hour=hour, minute=minute))

    @staticmethod
    def _to_utc(workcenter, naive_local):
        tz = pytz.timezone(workcenter.company_id.tz or 'UTC')
        return tz.localize(naive_local, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)

    @staticmethod
    def _from_utc(workcenter, naive_utc):
        tz = pytz.timezone(workcenter.company_id.tz or 'UTC')
        return pytz.utc.localize(naive_utc).astimezone(tz).replace(tzinfo=None)

    def _notify(self, count):
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Historical Sync Queued',
                'message': f"{count} shift(s) queued. Track via Settings → Queue Job → Jobs (channel root.mes_hist).",
                'type': 'success',
                'sticky': False,
            },
        }
