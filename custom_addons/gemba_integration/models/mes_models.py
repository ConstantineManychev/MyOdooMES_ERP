from odoo import models, fields, api
from typing import Optional, Tuple

class MesWorkcenter(models.Model):
    """
    Extension of Workcenter to support integration with Gemba/VerifySystems.
    """
    _inherit = 'mrp.workcenter'

    machine_number = fields.Integer(string='Machine Number')
    code_imatec = fields.Char(string='Imatec Name', help="Name used in external DB (e.g. IMA3)")
    
    _sql_constraints = [
        ('code_imatec_uniq', 'unique(code_imatec)', 'Imatec Code must be unique!')
    ]

    @api.model
    def get_or_create_from_external(self, machine_name: str) -> 'MesWorkcenter':
        """
        Finds a workcenter by standard name or external Imatec code.
        Creates a new one if not found.
        
        :param machine_name: Raw string from external DB (e.g. 'M1 - IMA3')
        :return: Workcenter recordset (singleton)
        """
        # 1. Try exact match by name
        machine = self.search([('name', '=', machine_name)], limit=1)
        if machine:
            return machine

        # 2. Try parsing ' - ' for Imatec code
        imatec_code = None
        if ' - ' in machine_name:
            parts = machine_name.split(' - ')
            if len(parts) > 1:
                imatec_code = parts[1].strip()
                machine = self.search([('code_imatec', '=', imatec_code)], limit=1)
                if machine:
                    return machine
        
        # 3. Create new if not found
        vals = {'name': machine_name}
        if imatec_code:
            vals['code_imatec'] = imatec_code
            
        return self.create(vals)


class MesAlarmReason(models.Model):
    _inherit = 'mrp.workcenter.productivity.loss'

    alarm_code = fields.Integer(string='PLC Alarm Code', index=True)
    full_name_external = fields.Char(string='External Full Name')
    
    @api.model
    def get_or_create_by_code(self, code: int, description: str) -> 'MesAlarmReason':
        """Idempotent creation of alarm reason."""
        reason = self.search([('alarm_code', '=', code)], limit=1)
        if not reason:
            reason = self.create({
                'name': description,
                'alarm_code': code,
                'category': 'availability',
                'manual': True
            })
        return reason


class MesShiftReport(models.Model):
    _name = 'mes.shift.report'
    _description = 'Machine Shift Production Report'
    _order = 'date desc, shift_type'

    name = fields.Char(string='Report ID', default='New', readonly=True)
    date = fields.Date(string='Shift Date', required=True)
    
    shift_type = fields.Selection([
        ('morning', 'Morning'),
        ('afternoon', 'Afternoon'),
        ('night', 'Night')
    ], string='Shift', required=True)

    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', required=True)
    
    alarm_ids = fields.One2many('mes.shift.alarm', 'report_id', string='Alarms')
    rejection_ids = fields.One2many('mes.shift.rejection', 'report_id', string='Rejections')

    _sql_constraints = [
        ('uniq_report', 'unique(workcenter_id, date, shift_type)', 'Report for this shift already exists!')
    ]

    @api.model
    def create(self, vals):
        if vals.get('name', 'New') == 'New':
            vals['name'] = f"{vals.get('date')} - {vals.get('workcenter_id')}"
        return super().create(vals)


class MesShiftAlarm(models.Model):
    _name = 'mes.shift.alarm'
    _description = 'Shift Alarm Line'

    report_id = fields.Many2one('mes.shift.report', string='Report', ondelete='cascade')
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

class MesShiftRejection(models.Model):
    _name = 'mes.shift.rejection'
    _description = 'Shift Rejection Line'

    report_id = fields.Many2one('mes.shift.report', string='Report', ondelete='cascade')
    product_id = fields.Many2one('product.product', string='Product')
    qty = fields.Float(string='Quantity')
    reason = fields.Char(string='Reason')