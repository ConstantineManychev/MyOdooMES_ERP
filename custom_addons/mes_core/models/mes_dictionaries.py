import operator
import logging
from datetime import datetime
from odoo import models, fields, api
from odoo.exceptions import ValidationError
from odoo.http import request
from odoo.osv import expression

_logger = logging.getLogger(__name__)

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

class MesHierarchyMixin(models.AbstractModel):
    _name = 'mes.hierarchy.mixin'
    _description = 'Hierarchy Sync Mixin'

    @api.model
    def sync_batch(self, data_list):
        existing = self.search([])
        code_map = {r.code: r for r in existing if r.code}
        name_map = {r.name: r for r in existing}
        parent_cache = {r.name: r.id for r in existing}

        for item in data_list:
            name = item.get('name', '').strip()
            code = item.get('code', '').strip()
            parent_name = item.get('parent_name', '').strip()
            vals = item.get('vals', {})

            if not name:
                continue

            parent_id = False
            if parent_name:
                if parent_name not in parent_cache:
                    new_parent = self.create({'name': parent_name})
                    parent_cache[parent_name] = new_parent.id
                    name_map[parent_name] = new_parent
                parent_id = parent_cache[parent_name]

            vals.update({
                'name': name,
                'parent_id': parent_id
            })
            if code:
                vals['code'] = code

            rec = code_map.get(code) if code else name_map.get(name)
            if rec:
                rec.write(vals)
            else:
                new_rec = self.create(vals)
                if code:
                    code_map[code] = new_rec
                name_map[name] = new_rec
                parent_cache[name] = new_rec.id

        return True

class MesCounts(models.Model):
    _name = 'mes.counts'
    _inherit = ['mes.hierarchy.mixin']
    _description = 'Counts'
    _parent_name = "parent_id" 
    _parent_store = True       
    _rec_name = 'complete_name' 
    _order = 'complete_name'

    name = fields.Char(string='Event', required=True)
    code = fields.Char(string='Code')

    parent_id = fields.Many2one('mes.counts', string='Parent Group', index=True, ondelete='cascade')
    child_ids = fields.One2many('mes.counts', 'parent_id', string='Children')
    parent_path = fields.Char(index=True, unaccent=False)

    complete_name = fields.Char('Complete Name', compute='_compute_complete_name', store=True)

    default_OPCTag = fields.Char(string='Default OPC Tag', help="Default tag for OPC integration")
    is_cumulative = fields.Boolean(string='Cumulative (MAX-MIN)', default=False, 
                                   help="If true, OEE calculation will use MAX-MIN difference for this count instead of summing up values")
    is_module_count = fields.Boolean(string='Is Module Count', help="Indicates if this count is related to module production")
    wheel = fields.Integer(string='Wheel', help="Number of the wheel associated with this count")
    module = fields.Integer(string='Module', help="Number of the module associated with this count")

    @api.depends('name', 'parent_id.complete_name')
    def _compute_complete_name(self):
        for count in self:
            if count.parent_id:
                count.complete_name = '%s / %s' % (count.parent_id.complete_name, count.name)
            else:
                count.complete_name = count.name
                
    @api.constrains('parent_id')
    def _check_hierarchy(self):
        if not self._check_recursion():
            raise ValidationError('Error! You cannot create recursive categories.')

    def get_tag_for_machine(self, machine_id):
        self.ensure_one()
        override = self.env['mes.signal.count'].search([
            ('count_id', '=', self.id),
            ('machine_id', '=', machine_id.id if isinstance(machine_id, models.Model) else machine_id)
        ], limit=1)
        if override:
            return override.tag_name
        return self.default_OPCTag
    
    def get_count_config_for_machine(self, machine_id):
        self.ensure_one()
        override = self.env['mes.signal.count'].search([
            ('count_id', '=', self.id),
            ('machine_id', '=', machine_id.id if isinstance(machine_id, models.Model) else machine_id)
        ], limit=1)
        
        if override:
            return override.tag_name, override.is_cumulative
        return self.default_OPCTag, self.is_cumulative


class MesEvents(models.Model):
    _name = 'mes.event'
    _inherit = ['mes.hierarchy.mixin']
    _description = 'Event'
    _parent_name = "parent_id" 
    _parent_store = True       
    _rec_name = 'complete_name' 
    _order = 'complete_name'

    name = fields.Char(string='Event Name', required=True)
    code = fields.Char(string='Code')
    
    parent_id = fields.Many2one('mes.event', string='Parent Group', index=True, ondelete='cascade')
    child_ids = fields.One2many('mes.event', 'parent_id', string='Children')
    parent_path = fields.Char(index=True, unaccent=False)
    
    complete_name = fields.Char('Complete Name', compute='_compute_complete_name', store=True)

    default_event_tag_type = fields.Selection([
        ('OEE.nMachineState', 'Machine State (OEE.nMachineState)'),
        ('OEE.nStopRootReason', 'Stop Reason (OEE.nStopRootReason)')
    ], string='Default Tag Type', help="Base tag source for this event")
    default_plc_value = fields.Integer(string='Default PLC Value', help="Value that triggers this event")

    @api.depends('name', 'parent_id.complete_name')
    def _compute_complete_name(self):
        for event in self:
            if event.parent_id:
                event.complete_name = '%s / %s' % (event.parent_id.complete_name, event.name)
            else:
                event.complete_name = event.name

    @api.constrains('parent_id')
    def _check_hierarchy(self):
        if not self._check_recursion():
            raise ValidationError('Error! You cannot create recursive categories.')

    def get_mapping_for_machine(self, machine_id):
        self.ensure_one()
        override = self.env['mes.signal.event'].search([
            ('event_id', '=', self.id),
            ('machine_id', '=', machine_id.id if isinstance(machine_id, models.Model) else machine_id)
        ], limit=1)
        if override:
            return override.tag_name, override.plc_value
        return self.default_event_tag_type, self.default_plc_value


class MesProcess(models.Model):
    _name = 'mes.process'
    _inherit = ['mes.hierarchy.mixin']
    _description = 'Process Parameters Dictionary'
    _parent_name = "parent_id" 
    _parent_store = True       
    _rec_name = 'complete_name' 
    _order = 'complete_name'

    name = fields.Char(string='Process Name', required=True)
    code = fields.Char(string='Code')
    
    parent_id = fields.Many2one('mes.process', string='Parent Group', index=True, ondelete='cascade')
    child_ids = fields.One2many('mes.process', 'parent_id', string='Children')
    parent_path = fields.Char(index=True, unaccent=False)
    
    complete_name = fields.Char('Complete Name', compute='_compute_complete_name', store=True)
    default_OPCTag = fields.Char(string='Default OPC Tag', help="Default tag for OPC integration")

    @api.depends('name', 'parent_id.complete_name')
    def _compute_complete_name(self):
        for rec in self:
            if rec.parent_id:
                rec.complete_name = '%s / %s' % (rec.parent_id.complete_name, rec.name)
            else:
                rec.complete_name = rec.name

    def get_tag_for_machine(self, machine_id):
        self.ensure_one()
        override = self.env['mes.signal.process'].search([
            ('process_id', '=', self.id),
            ('machine_id', '=', machine_id.id if isinstance(machine_id, models.Model) else machine_id)
        ], limit=1)
        if override:
            return override.tag_name
        return self.default_OPCTag

class MesWorkcenter(models.Model):
    _inherit = 'mrp.workcenter'

    machine_number = fields.Integer(string='Machine Number')
    maintainx_id = fields.Integer(string='MaintainX ID', help="ID used in MaintainX system")
    code_imatec = fields.Char(string='Imatec Name', help="Name used in external DB (e.g. IMA3)")

    machine_settings_id = fields.Many2one('mes.machine.settings', string='Telemetry Settings')

    runtime_event_id = fields.Many2one('mes.event', string='Runtime Event', help="Runtime event used for OEE calculation")
    production_count_id = fields.Many2one('mes.counts', string='Production Count', help="Good parts count used for OEE calculation")
    refresh_frequency = fields.Integer(string='Refresh Frequency (sec)', default=60, help="Frequency of OEE dashboard refresh in seconds")
    ideal_capacity_per_min = fields.Float(string='Ideal Capacity (Parts/Min)', default=200.0)

    current_availability = fields.Float(string='Availability (%)', compute='_compute_realtime_oee')
    current_performance = fields.Float(string='Performance (%)', compute='_compute_realtime_oee')
    current_quality = fields.Float(string='Quality (%)', compute='_compute_realtime_oee')
    current_produced = fields.Integer(string='Produced Today', compute='_compute_realtime_oee')

    current_oee = fields.Float(string='OEE (%)', compute='_compute_realtime_oee', search='_search_current_oee')
    current_waste_losses = fields.Float(string='Waste Losses (%)', compute='_compute_realtime_oee', search='_search_waste_losses')
    current_downtime_losses = fields.Float(string='Downtime Losses (%)', compute='_compute_realtime_oee', search='_search_downtime_losses')

    current_first_running_time = fields.Datetime(string='Start Time', compute='_compute_realtime_oee')
    current_runtime_formatted = fields.Char(string='Runtime', compute='_compute_realtime_oee')
    current_top_rejection = fields.Char(string='Top Rejection', compute='_compute_realtime_oee')
    current_top_alarm = fields.Char(string='Top Alarm', compute='_compute_realtime_oee')

    allowed_pc_ips = fields.Char(
        string='All Allowed PC IPs',
        help='Specify the IP addresses of allowed computers, separated by commas (e.g., 192.168.1.50, 192.168.1.51)'
    )
    
    _sql_constraints = [
        ('code_imatec_uniq', 'unique(code_imatec)', 'Imatec Code must be unique!')
    ]

    @api.model
    def fields_get(self, allfields=None, attributes=None):
        res = super().fields_get(allfields, attributes)
        custom_sort_fields = [
            'current_oee', 'current_waste_losses', 'current_downtime_losses',
            'current_produced', 'current_first_running_time', 'current_runtime_formatted'
        ]
        for field in custom_sort_fields:
            if field in res:
                res[field]['sortable'] = True
        return res

    @api.model
    def web_search_read(self, domain=None, specification=None, offset=0, limit=None, order=None, count_limit=None, **kw):

        domain = self._apply_operator_ip_filter(domain)
        custom_order_field, reverse, order = self._extract_custom_order(order)
        
        res = super().web_search_read(
            domain=domain, specification=specification, offset=offset, 
            limit=limit, order=order, count_limit=count_limit, **kw
        )
        
        if res.get('records'):
            self._apply_custom_sort(res['records'], custom_order_field, reverse)
            
        return res

    @api.model
    def search_read(self, domain=None, fields=None, offset=0, limit=None, order=None, **kw):

        domain = self._apply_operator_ip_filter(domain)
        custom_order_field, reverse, order = self._extract_custom_order(order)
        
        res = super().search_read(
            domain=domain, fields=fields, offset=offset, 
            limit=limit, order=order, **kw
        )
        
        if isinstance(res, list):
            self._apply_custom_sort(res, custom_order_field, reverse)
            
        return res

    @api.model
    def _apply_operator_ip_filter(self, domain):
        if request and self.env.user.has_group('mes_core.group_mes_operator') and not self.env.user.has_group('mes_core.group_mes_manager'):
            client_ip = request.httprequest.headers.get('X-Forwarded-For', request.httprequest.remote_addr)
            if client_ip:
                client_ip = client_ip.split(',')[0].strip()
            else:
                client_ip = 'UNKNOWN'
                
            _logger.info(f"MES Security: Operator opened the dashboard from IP address: {client_ip}")
            
            all_restricted_wcs = self.env['mrp.workcenter'].sudo().search([('allowed_pc_ips', '!=', False)])
            allowed_wc_ids = []
            
            for wc in all_restricted_wcs:
                valid_ips = [ip.strip() for ip in wc.allowed_pc_ips.split(',')]
                if client_ip in valid_ips:
                    allowed_wc_ids.append(wc.id)
            
            ip_domain = [('id', 'in', allowed_wc_ids)]
            return expression.AND([domain or [], ip_domain])
        return domain

    @api.model
    def _extract_custom_order(self, order):
        custom_sort_fields = [
            'current_oee', 'current_waste_losses', 'current_downtime_losses',
            'current_produced', 'current_first_running_time', 'current_runtime_formatted'
        ]
        custom_order_field = None
        reverse = False
        
        if order:
            for part in order.split(','):
                part = part.strip().lower()
                for cf in custom_sort_fields:
                    if part.startswith(cf):
                        custom_order_field = cf
                        reverse = 'desc' in part
                        break
                if custom_order_field:
                    break
            
            if custom_order_field:
                order = 'name asc' 
                
        return custom_order_field, reverse, order

    @api.model
    def _apply_custom_sort(self, records, custom_order_field, reverse):
        if not custom_order_field or not records:
            return

        def sort_key(item):
            val = item.get(custom_order_field)
            if val is False or val is None:
                if custom_order_field == 'current_first_running_time':
                    return datetime.min 
                elif custom_order_field == 'current_runtime_formatted':
                    return ""
                return -float('inf')
            return val

        records.sort(key=sort_key, reverse=reverse)

    @api.constrains('refresh_frequency')
    def _check_refresh_frequency(self):
        for wc in self:
            if wc.refresh_frequency < 10:
                raise ValidationError('Configuration error: Refresh frequency cannot be less than 10 seconds.')

    def _compute_realtime_oee(self):
        for wc in self:
            if not wc.machine_settings_id or not wc.runtime_event_id or not wc.production_count_id:
                wc._reset_oee()
                continue
            
            oee_data = wc.machine_settings_id.get_realtime_oee(
                runtime_event=wc.runtime_event_id,
                production_count=wc.production_count_id,
                workcenter=wc
            )
            
            if 'error' in oee_data:
                wc._reset_oee()
            else:
                wc.current_oee = oee_data.get('oee', 0.0)
                wc.current_availability = oee_data.get('availability', 0.0)
                wc.current_performance = oee_data.get('performance', 0.0)
                wc.current_quality = oee_data.get('quality', 0.0)
                wc.current_produced = oee_data.get('total_produced', 0)

                wc.current_waste_losses = oee_data.get('waste_losses', 0.0)
                wc.current_downtime_losses = oee_data.get('downtime_losses', 0.0)

                wc.current_first_running_time = oee_data.get('first_running_time', False)
                wc.current_runtime_formatted = oee_data.get('runtime_formatted', '00:00:00')
                wc.current_top_rejection = oee_data.get('top_rejection', 'None')
                wc.current_top_alarm = oee_data.get('top_alarm', 'None')

    def _reset_oee(self):
        self.current_oee = 0.0
        self.current_availability = 0.0
        self.current_performance = 0.0
        self.current_quality = 0.0
        self.current_produced = 0

        self.current_waste_losses = 0.0
        self.current_downtime_losses = 0.0

        self.current_first_running_time = False
        self.current_runtime_formatted = '00:00:00'
        self.current_top_rejection = 'None'
        self.current_top_alarm = 'None'

    def _get_op_func(self, operator_str):
        ops = {
            '=': operator.eq, '!=': operator.ne,
            '<': operator.lt, '<=': operator.le,
            '>': operator.gt, '>=': operator.ge
        }
        return ops.get(operator_str, operator.eq)

    def _search_current_oee(self, operator_str, value):
        op_func = self._get_op_func(operator_str)
        wcs = self.search([('machine_settings_id', '!=', False)])
        match_ids = wcs.filtered(lambda wc: op_func(wc.current_oee, value)).ids
        return [('id', 'in', match_ids)] if match_ids else [('id', '=', 0)]

    def _search_waste_losses(self, operator_str, value):
        op_func = self._get_op_func(operator_str)
        wcs = self.search([('machine_settings_id', '!=', False)])
        match_ids = wcs.filtered(lambda wc: op_func(wc.current_waste_losses, value)).ids
        return [('id', 'in', match_ids)] if match_ids else [('id', '=', 0)]

    def _search_downtime_losses(self, operator_str, value):
        op_func = self._get_op_func(operator_str)
        wcs = self.search([('machine_settings_id', '!=', False)])
        match_ids = wcs.filtered(lambda wc: op_func(wc.current_downtime_losses, value)).ids
        return [('id', 'in', match_ids)] if match_ids else [('id', '=', 0)]

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