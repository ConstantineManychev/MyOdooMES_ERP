import pytz
import logging
from datetime import timedelta
from odoo import models, fields, api
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)


class MesOperationAssignWizard(models.TransientModel):
    _name = 'mes.operation.assign.wizard'
    _description = 'Operation Assign Wizard'

    operation_id = fields.Many2one('mes.machine.operation', required=True)
    workcenter_id = fields.Many2one('mrp.workcenter')
    machine_id = fields.Many2one(related='workcenter_id.machine_settings_id')
    report_id = fields.Many2one(
        'mes.production.report',
        domain="[('machine_id', '=', machine_id)]",
        required=True,
    )

    def action_confirm(self):
        self.ensure_one()
        self.operation_id.write({
            'op_type': 'job',
            'report_id': self.report_id.id,
            'job_number': self.report_id.name,
        })


class MesOperationSplitWizard(models.TransientModel):
    _name = 'mes.operation.split.wizard'
    _description = 'Operation Split Wizard'

    operation_id = fields.Many2one('mes.machine.operation', required=True)
    split_dt = fields.Datetime(required=True)

    @api.constrains('split_dt')
    def _check_split_dt(self):
        for rec in self:
            op = rec.operation_id
            if rec.split_dt <= op.start_dt or (op.end_dt and rec.split_dt >= op.end_dt):
                raise ValidationError("Split time must be within the operation interval.")

    def action_confirm(self):
        self.ensure_one()
        op = self.operation_id
        op.copy({
            'start_dt': self.split_dt,
            'end_dt': op.end_dt,
            'op_type': op.op_type,
            'report_id': op.report_id.id,
            'job_number': op.job_number,
        })
        op.end_dt = self.split_dt


class MesRecalcDowntimeWiz(models.TransientModel):
    _name = 'mes.recalc.downtime.wiz'
    _description = 'Recalculate Downtimes from Telemetry'

    start_date = fields.Datetime(string='Start DateTime', required=True)
    end_date = fields.Datetime(string='End DateTime', required=True, default=fields.Datetime.now)
    machine_ids = fields.Many2many('mrp.workcenter', string='Machines', required=True)

    def action_recalc(self):
        self.ensure_one()
        docs = self._select_target_docs()
        if not docs:
            return self._notify(0)
        Performance = self.env['mes.machine.performance']
        for doc in docs:
            Performance.with_delay(
                channel='root.mes_recalc',
                description=f"Recalc downtime doc={doc.id} {doc.date} WC={doc.machine_id.name}",
                identity_key=f"recalc_{doc.id}",
            )._recalc_downtimes_job(doc.id)
        return self._notify(len(docs))

    def _select_target_docs(self):
        docs = self.env['mes.machine.performance'].search([
            ('machine_id', 'in', self.machine_ids.ids),
            ('date', '>=', self.start_date.date() - timedelta(days=1)),
            ('date', '<=', self.end_date.date() + timedelta(days=1)),
        ])
        valid = []
        for doc in docs:
            start_local, _ = doc._get_local_shift_times()
            start_utc = doc._get_utc_time(start_local)
            if self.start_date <= start_utc <= self.end_date:
                valid.append(doc)
        return sorted(valid, key=lambda d: d.date)

    def _notify(self, count):
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Recalculation Queued',
                'message': f"{count} document(s) queued for downtime recalculation.",
                'type': 'success',
                'sticky': False,
            },
        }
