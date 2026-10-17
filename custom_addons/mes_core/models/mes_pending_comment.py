import logging
from datetime import timedelta
from odoo import models, fields, api

_logger = logging.getLogger(__name__)

MATCH_TOLERANCE_SEC = 90
PENDING_MAX_AGE_HOURS = 12
TARGET_MODELS = (
    'mes.performance.alarm',
    'mes.performance.slowing',
    'mes.performance.running',
)


class MesPendingComment(models.Model):
    _name = 'mes.pending.comment'
    _description = 'Kiosk Comment Awaiting Status Sync'
    _order = 'event_time desc'

    machine_id = fields.Many2one('mrp.workcenter', string='Work Center',
                                 required=True, ondelete='cascade', index=True)
    event_time = fields.Datetime(string='Status Start (UTC)', required=True, index=True)
    comment = fields.Text(string='Comment')
    new_event_id = fields.Many2one('mes.event', string='Replacement Stop')
    employee_id = fields.Many2one('hr.employee', string='Recorded By')
    applied = fields.Boolean(string='Applied', default=False, index=True)
    applied_on = fields.Datetime(string='Applied On')

    _sql_constraints = [
        ('machine_event_uniq', 'unique(machine_id, event_time)',
         'Only one queued comment per status start is allowed.'),
    ]

    @api.model
    def queue_for_status(self, workcenter, event_time, comment=None,
                         new_event=None, employee=None):
        existing = self.sudo().search([
            ('machine_id', '=', workcenter.id),
            ('event_time', '=', event_time),
            ('applied', '=', False),
        ], limit=1)

        values = {}
        if comment is not None:
            values['comment'] = (comment or '').strip() or False
        if new_event is not None:
            values['new_event_id'] = new_event.id if new_event else False
        if employee:
            values['employee_id'] = employee.id

        if existing:
            existing.write(values)
            if not existing.comment and not existing.new_event_id:
                existing.unlink()
                return self.browse()
            return existing

        if not values.get('comment') and not values.get('new_event_id'):
            return self.browse()

        values.update({
            'machine_id': workcenter.id,
            'event_time': event_time,
        })
        return self.sudo().create(values)

    @api.model
    def get_for_status(self, workcenter, event_time):
        record = self.sudo().search([
            ('machine_id', '=', workcenter.id),
            ('event_time', '=', event_time),
            ('applied', '=', False),
        ], limit=1)
        if not record:
            return False
        return {
            'id': record.id,
            'event_time': fields.Datetime.to_string(record.event_time),
            'comment': record.comment or '',
            'new_event_id': record.new_event_id.id or False,
            'new_event_name': record.new_event_id.name or '',
        }

    @api.model
    def apply_for_workcenter(self, workcenter):
        pending = self.sudo().search([
            ('machine_id', '=', workcenter.id),
            ('applied', '=', False),
        ])
        if not pending:
            return 0

        cutoff = fields.Datetime.now() - timedelta(hours=PENDING_MAX_AGE_HOURS)
        tolerance = timedelta(seconds=MATCH_TOLERANCE_SEC)
        applied = 0

        for record in pending:
            if record.event_time < cutoff:
                record.write({'applied': True, 'applied_on': fields.Datetime.now()})
                _logger.warning("Queued comment %s expired without a matching status", record.id)
                continue
            target = self._find_target(workcenter, record.event_time, tolerance)
            if not target:
                continue
            values = {}
            if record.comment:
                values['comment'] = record.comment
            if record.new_event_id and target.loss_id.is_manual_stop:
                values['loss_id'] = record.new_event_id.id
            if values:
                target.sudo().write(values)
            record.write({'applied': True, 'applied_on': fields.Datetime.now()})
            applied += 1
        return applied

    @api.model
    def _find_target(self, workcenter, event_time, tolerance):
        lower = event_time - tolerance
        upper = event_time + tolerance
        best = None
        best_delta = None
        for model_name in TARGET_MODELS:
            candidates = self.env[model_name].sudo().search([
                ('performance_id.machine_id', '=', workcenter.id),
                ('start_time', '>=', lower),
                ('start_time', '<=', upper),
            ])
            for candidate in candidates:
                delta = abs((candidate.start_time - event_time).total_seconds())
                if best_delta is None or delta < best_delta:
                    best, best_delta = candidate, delta
        return best
