from odoo import models, fields, api

class MesMachineSettings(models.Model):
    _name = 'mes.machine.settings'
    _description = 'Machine Connection Settings'
    _inherit = ['mail.thread', 'mail.activity.mixin', 'mes.timescale.base']

    name = fields.Char(string='Machine Name', required=True, copy=False, tracking=True)
    ip_connection = fields.Char(string='Connection IP', tracking=True)
    ip_data = fields.Char(string='TwinCAT/Data IP', tracking=True)
    
    signal_ids = fields.One2many('mes.signal.tag', 'machine_settings_id', string='Monitored Signals')

    _sql_constraints = [('name_uniq', 'unique (name)', 'Machine Name must be unique!')]

    def init(self):
        if hasattr(self.env['mes.timescale.db.manager'], '_init_DB'):
            self.env['mes.timescale.db.manager']._init_DB()
            self.env['mes.timescale.db.manager']._init_local_fdw()

    @api.model
    def create(self, vals):
        rec = super().create(vals)
        self._execute_from_file('upsert_machine.sql', (rec.name, rec.ip_connection, rec.ip_data))
        return rec

    def write(self, vals):
        res = super().write(vals)
        for rec in self:
            self._execute_from_file('upsert_machine.sql', (rec.name, rec.ip_connection, rec.ip_data))
        return res

    def unlink(self):
        for rec in self:
            self._execute_from_file('delete_machine.sql', (rec.name,))
        return super().unlink()

class MesSignalTag(models.Model):
    _name = 'mes.signal.tag'
    _description = 'Monitored Signal Configuration'
    _inherit = ['mes.timescale.base']
    _rec_name = 'tag_name'

    machine_settings_id = fields.Many2one('mes.machine.settings', string='Machine', required=True, ondelete='cascade')
    tag_name = fields.Char(string='Signal Tag', required=True)
    
    poll_type = fields.Selection([('cyclic', 'Cyclic'), ('on_change', 'On Change')], default='cyclic', required=True)
    poll_frequency = fields.Integer(string='Freq (ms)', default=1000)
    
    param_type = fields.Selection([
        ('auto', 'Auto'),
        ('bool', 'Boolean'),
        ('int', 'Integer'),
        ('double', 'Double/Real'),
        ('string', 'String')
    ], string='Data Type', default='auto', required=True)
    
    signal_type = fields.Selection([
        ('count', 'Count (Integer)'),
        ('event', 'Event (Integer)'),
        ('process', 'Process (Mixed)')
    ], string='Category', required=True)

    @api.model
    def create(self, vals):
        rec = super().create(vals)
        self._sync(rec)
        return rec

    def write(self, vals):
        res = super().write(vals)
        for rec in self:
            self._sync(rec)
        return res

    def unlink(self):
        for rec in self:
            self._execute_from_file('delete_signal.sql', (rec.machine_settings_id.name, rec.tag_name))
        return super().unlink()

    def _sync(self, rec):
        self._execute_from_file('upsert_signal.sql', (
            rec.machine_settings_id.name, rec.tag_name, 
            rec.poll_type, rec.poll_frequency, rec.param_type, rec.signal_type
        ))