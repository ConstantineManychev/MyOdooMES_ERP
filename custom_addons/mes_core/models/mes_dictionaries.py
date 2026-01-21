from odoo import models, fields

class MesShift(models.Model):
    _name = 'mes.shift'
    _description = 'Work Shifts'
    
    name = fields.Char(string='Shift Name', required=True)
    code = fields.Char(string='Code', help="Code for external integration")
    start_hour = fields.Float(string='Start Hour')
    duration = fields.Float(string='Duration (Hours)')

class MesDefect(models.Model):
    _name = 'mes.defect'
    _description = 'QC Defect Types'
    
    name = fields.Char(string='Defect Name', required=True)
    code = fields.Char(string='Defect Code')
    description = fields.Text(string='Description')

class MesRejectionReason(models.Model):
    _name = 'mes.rejection.reason'
    _description = 'Rejection Reasons'

    name = fields.Char(string='Reason', required=True)
    code = fields.Char(string='Code')

# Расширяем стандартный Workcenter
class MesWorkcenter(models.Model):
    _inherit = 'mrp.workcenter'

    machine_number = fields.Integer(string='Machine Number')
    code_imatec = fields.Char(string='Imatec Name', help="Name used in external DB (e.g. IMA3)")
    
    _sql_constraints = [
        ('code_imatec_uniq', 'unique(code_imatec)', 'Imatec Code must be unique!')
    ]