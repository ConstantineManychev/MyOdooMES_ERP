from odoo import models, fields

class MesMachineSettings(models.Model):
    _name = 'mes.machine.settings'
    _description = 'Machine Telemetry Settings'
    
    name = fields.Char(string='Name', required=True)
    workcenter_id = fields.Many2one('mrp.workcenter', string='Workcenter', required=True)
    ip_address = fields.Char(string='IP Address')
    port = fields.Integer(string='Port', default=502)
    protocol = fields.Selection([
        ('modbus', 'Modbus TCP'),
        ('opc_ua', 'OPC UA'),
        ('mqtt', 'MQTT')
    ], string='Protocol', default='modbus')
    active = fields.Boolean(default=True)
