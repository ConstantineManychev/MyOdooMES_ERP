from odoo import models, fields, api

class MesShifts(models.Model):
    _name = 'mes.shift'
    _description = 'Work Shifts'
    
    name = fields.Char(string='Shift Name', required=True)
    code = fields.Char(string='Code', help="Code for external integration")
    start_hour = fields.Float(string='Start Hour')
    end_hour = fields.Float(string='End Hour')
    duration = fields.Float(
        string='Duration (Hours)', 
        compute='_compute_duration', 
        store=True, 
        readonly=True
    )

    @api.depends('start_hour', 'end_hour')
    def _compute_duration(self):
        for shift in self:
            if shift.end_hour >= shift.start_hour:
                shift.duration = shift.end_hour - shift.start_hour
            else:
                shift.duration = 24.0 - shift.start_hour + shift.end_hour

class MesDefects(models.Model):
    _name = 'mes.defect'
    _description = 'QC Defect Types'
    
    name = fields.Char(string='Defect Name', required=True)
    code = fields.Char(string='Defect Code')
    description = fields.Text(string='Description')

class MesCounts(models.Model):
    _name = 'mes.counts'
    _description = 'Counts'

    name = fields.Char(string='Event', required=True)
    code = fields.Char(string='Code')
    default_OPCTag = fields.Char(string='Default OPC Tag', help="Default tag for OPC integration")
    is_module_count = fields.Boolean(string='Is Module Count', help="Indicates if this count is related to module production")
    wheel = fields.Integer(string='Wheel', help="Number of the wheel associated with this count")
    module = fields.Integer(string='Module', help="Number of the module associated with this count")

class MesEvents(models.Model):
    _name = 'mes.event'
    _description = 'Event'

    name = fields.Char(string='Event Name', required=True)
    code = fields.Char(string='Code')
    default_OPCTag = fields.Char(string='Default OPC Tag', help="Default tag for OPC integration")
    default_PLCValue = fields.Char(string='Default PLC Value', help="Default value for PLC integration")

class MesWorkcenter(models.Model):
    _inherit = 'mrp.workcenter'

    machine_number = fields.Integer(string='Machine Number')
    maintainx_id = fields.Integer(string='MaintainX ID', help="ID used in MaintainX system")
    code_imatec = fields.Char(string='Imatec Name', help="Name used in external DB (e.g. IMA3)")
    
    _sql_constraints = [
        ('code_imatec_uniq', 'unique(code_imatec)', 'Imatec Code must be unique!')
    ]

class MesStreams(models.Model):
    _name = 'mes.stream'
    _description = 'Stream'

    stream_number = fields.Integer(string='Stream Number')
    machine_id = fields.Many2one('mrp.workcenter', string='Machine')
    
    def name_get(self):
        result = []
        for rec in self:
            name = f"Stream {rec.stream_number} ({rec.machine_id.name})"
            result.append((rec.id, name))
        return result

class MesWheels(models.Model):
    _name = 'mes.wheel'
    _description = 'Wheel'

    wheel_number = fields.Integer(string='Wheel Number')
    maintainx_id = fields.Integer(string='MaintainX ID', help="ID used in MaintainX system")
    stream_id = fields.Many2one('mes.stream', string='Parent Stream')
    modules_amount = fields.Integer(string='Number of Modules')

class MesEmployee(models.Model):
    _inherit = 'hr.employee'

    maintainx_id = fields.Char(string='MaintainX ID', help="User ID from MaintainX system")