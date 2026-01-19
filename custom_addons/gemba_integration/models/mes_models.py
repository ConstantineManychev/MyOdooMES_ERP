from odoo import models, fields, api

# --- 1. Catalog: Machines (Extending standard Workcenter) ---
class MesWorkcenter(models.Model):
    _inherit = 'mrp.workcenter'

    machine_number = fields.Integer(string='Machine Number')
    code_imatec = fields.Char(string='Imatec Name', help="Name used in external DB (e.g. IMA3)")
    
    # Search optimization
    _sql_constraints = [
        ('code_imatec_uniq', 'unique(code_imatec)', 'Imatec Code must be unique!')
    ]

# --- 2. Catalog: Alarms (Extending standard downtime reasons) ---
class MesAlarmReason(models.Model):
    _inherit = 'mrp.workcenter.productivity.loss'

    alarm_code = fields.Integer(string='PLC Alarm Code', index=True)
    full_name_external = fields.Char(string='External Full Name')
    

# --- 3. Docuiment: MachineShiftProductionReport ---
class MesShiftReport(models.Model):
    _name = 'mes.shift.report'
    _description = 'Machine Shift Production Report'
    _order = 'date desc, shift_type'

    # doc header
    name = fields.Char(string='Report ID', default='New', readonly=True)
    date = fields.Date(string='Shift Date', required=True)
    
    # Shifts
    shift_type = fields.Selection([
        ('morning', 'Morning'),
        ('afternoon', 'Afternoon'),
        ('night', 'Night')
    ], string='Shift', required=True)

    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', required=True)
    
    # Alarms table part
    alarm_ids = fields.One2many('mes.shift.alarm', 'report_id', string='Alarms')

    # Rejections table part
    rejection_ids = fields.One2many('mes.shift.rejection', 'report_id', string='Rejections')

    # Unique check
    _sql_constraints = [
        ('uniq_report', 'unique(workcenter_id, date, shift_type)', 'Report for this shift already exists!')
    ]

    @api.model
    def create(self, vals):
        if vals.get('name', 'New') == 'New':
            # Doc number generation
            vals['name'] = f"{vals.get('date')} - {vals.get('workcenter_id')}"
        return super().create(vals)

# --- 4. Alarms Line ---
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

# --- 5 Rejections Line ---
class MesShiftRejection(models.Model):
    _name = 'mes.shift.rejection'
    _description = 'Shift Rejection Line'

    report_id = fields.Many2one('mes.shift.report', string='Report', ondelete='cascade')
    product_id = fields.Many2one('product.product', string='Product')
    qty = fields.Float(string='Quantity')
    reason = fields.Char(string='Reason') 