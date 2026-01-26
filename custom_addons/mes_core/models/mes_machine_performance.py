from odoo import models, fields, api
from typing import Dict, Any

class MesMachinePerformance(models.Model):
    _name = 'mes.machine.performance'
    _description = 'Machine Performance Data (OEE)'
    _order = 'date desc, shift_id'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Doc ID', default='New', readonly=True)
    date = fields.Date(string='Date', required=True, default=fields.Date.context_today)

    shift_id = fields.Many2one('mes.shift', string='Shift', required=True)
    machine_id = fields.Many2one('mrp.workcenter', string='Machine', required=True)

    alarm_ids = fields.One2many('mes.performance.alarm', 'performance_id', string='Alarms')
    rejection_ids = fields.One2many('mes.performance.rejection', 'performance_id', string='Rejections')

    state = fields.Selection([
        ('draft', 'Draft'),
        ('done', 'Locked')
    ], string='Status', default='draft', tracking=True)

    _sql_constraints = [
        ('uniq_report', 'unique(machine_id, date, shift_id)', 'Report for this shift already exists!')
    ]

    @api.model_create_multi
    def create(self, vals_list):
        for vals in vals_list:
            if vals.get('name', 'New') == 'New':
                vals['name'] = f"PERF/{vals.get('date')}/{vals.get('machine_id')}"
        return super().create(vals_list)

class MesPerformanceAlarm(models.Model):
    _name = 'mes.performance.alarm'
    _description = 'Machine Alarms'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade')
    loss_id = fields.Many2one('mrp.workcenter.productivity.loss', string='Alarm Reason')
    start_time = fields.Datetime(string='Start Time')
    end_time = fields.Datetime(string='End Time')
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

class MesPerformanceRejection(models.Model):
    _name = 'mes.performance.rejection'
    _description = 'Machine Rejections'

    performance_id = fields.Many2one('mes.machine.performance', string='Report', ondelete='cascade')
    product_id = fields.Many2one('product.product', string='Product')
    qty = fields.Float(string='Quantity')
    reason_id = fields.Many2one('mes.rejection.reason', string='Reason') 