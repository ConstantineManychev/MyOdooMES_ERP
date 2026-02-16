from odoo import models, fields, api

class MesPlannedDowntime(models.Model):
    _name = 'mes.planned.downtime'
    _description = 'Planned Machine Downtime'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string="Reason", required=True, tracking=True)
    machine_ids = fields.Many2many('mrp.workcenter', string="Machines", required=True)
    
    start_date = fields.Datetime(string="Start Date", required=True, tracking=True)
    end_date = fields.Datetime(string="End Date", required=True, tracking=True)
    
    downtime_type = fields.Selection([
        ('maintenance', 'Maintenance'),
        ('cleaning', 'Cleaning'),
        ('changeover', 'Changeover'),
        ('break', 'Shift Break')
    ], string="Type", required=True, default='maintenance')
    
    state = fields.Selection([
        ('draft', 'Draft'),
        ('applied', 'Applied')
    ], default='draft', tracking=True)

    def action_apply(self):
        self.ensure_one()
        Leave = self.env['resource.calendar.leaves']
        
        vals_list = []
        for machine in self.machine_ids:
            if not machine.resource_calendar_id:
                continue
                
            vals_list.append({
                'name': f"[{self.downtime_type.upper()}] {self.name}",
                'calendar_id': machine.resource_calendar_id.id,
                'date_from': self.start_date,
                'date_to': self.end_date,
                'resource_id': machine.resource_id.id, 
                'time_type': 'leave'
            })
        
        if vals_list:
            Leave.create(vals_list)
        
        self.state = 'applied'

    def action_reset(self):
        self.state = 'draft'