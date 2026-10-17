import pytz
import logging
from datetime import datetime, timedelta, time
import math
import time as time_module
from odoo import models, fields, api, _
from odoo.exceptions import ValidationError, UserError
from odoo.exceptions import ValidationError

_logger = logging.getLogger(__name__)

FSM_SYNC_JOB_ETA_SEC = 18
MACHINE_STATE_STOPPED = 1
WORKCENTER_STATUS_CHANNEL = 'mes.workcenter.status'

class MesShifts(models.Model):
    _name = 'mes.shift'
    _description = 'Work Shifts'
    _order = 'sequence, start_hour'
    
    sequence = fields.Integer(string="Sequence", default=10)
    name = fields.Char(string='Shift Name', required=True)
    code = fields.Char(string='Code')
    start_hour = fields.Float(string='Start Hour')
    end_hour = fields.Float(string='End Hour')
    duration = fields.Float(string='Duration (Hours)', compute='_compute_duration', store=True, readonly=True)
    company_id = fields.Many2one('res.company', string='Company', required=True, default=lambda self: self.env.company)
    workcenter_ids = fields.Many2many('mrp.workcenter', string='Machines', domain="[('company_id', '=', company_id)]")

    @api.depends('start_hour', 'end_hour')
    def _compute_duration(self):
        for s in self:
            if s.end_hour >= s.start_hour:
                s.duration = s.end_hour - s.start_hour
            else:
                s.duration = 24.0 - s.start_hour + s.end_hour

    @api.model
    def get_current_shift_window(self, wc=None):
        now_utc = fields.Datetime.now()
        tz_name = wc.company_id.tz if wc and wc.company_id.tz else 'UTC'
        mac_tz = pytz.timezone(tz_name)
        now_mac = pytz.utc.localize(now_utc).astimezone(mac_tz).replace(tzinfo=None)
        
        curr_h = now_mac.hour + now_mac.minute / 60.0 + now_mac.second / 3600.0
        
        shifts = self.search([('company_id', '=', wc.company_id.id)])
        valid_shifts = [s for s in shifts if not (wc and s.workcenter_ids and wc.id not in s.workcenter_ids.ids)]
        
        curr_s = None
        for s in valid_shifts:
            if s.start_hour < s.end_hour:
                if s.start_hour <= curr_h < s.end_hour:
                    curr_s = s
                    break
            else:
                if curr_h >= s.start_hour or curr_h < s.end_hour:
                    curr_s = s
                    break
        
        if not curr_s:
            return None, None

        start_d = now_mac.date()
        if curr_s.start_hour > curr_s.end_hour and curr_h < curr_s.end_hour:
            start_d -= timedelta(days=1)
            
        s_time = datetime.combine(
            start_d,
            time(hour=int(curr_s.start_hour), minute=int((curr_s.start_hour % 1) * 60))
        )
        
        e_time = s_time + timedelta(hours=curr_s.duration)
        
        return s_time, e_time

    def get_previous_shift_window(self, wc=None):
        s_loc, _e_loc = self.get_current_shift_window(wc)
        return self._shift_window_before(s_loc, wc)

    def _shift_window_before(self, boundary_loc, wc=None):
        if not boundary_loc:
            return None, None
        prev_start = boundary_loc - timedelta(hours=1)
        prev_h = prev_start.hour + prev_start.minute / 60.0

        shifts = self.search([('company_id', '=', wc.company_id.id)]) if wc else self.search([])
        valid_shifts = [s for s in shifts if not (wc and s.workcenter_ids and wc.id not in s.workcenter_ids.ids)]

        for s in valid_shifts:
            in_window = (s.start_hour < s.end_hour and s.start_hour <= prev_h < s.end_hour) or \
                        (s.start_hour > s.end_hour and (prev_h >= s.start_hour or prev_h < s.end_hour))
            if in_window:
                start_d = prev_start.date()
                if s.start_hour > s.end_hour and prev_h < s.end_hour:
                    start_d -= timedelta(days=1)
                p_start = datetime.combine(start_d, time(hour=int(s.start_hour), minute=int((s.start_hour % 1) * 60)))
                return p_start, p_start + timedelta(hours=s.duration)
        return None, None

    MAX_SHIFT_LOOKBACK = 21
    MIN_PLANNED_SECONDS = 60.0

    def _planned_seconds_for(self, wc, s_loc, e_loc):
        mac_tz = pytz.timezone(wc.company_id.tz or 'UTC')
        s_utc = mac_tz.localize(s_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
        e_utc = mac_tz.localize(e_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
        total_sec = (e_utc - s_utc).total_seconds()
        if total_sec <= 0:
            return 0.0

        slots = self.env['mes.flat.downtime'].sudo().search([
            ('machine_id', '=', wc.id),
            ('start_time', '<', e_utc),
            ('end_time', '>', s_utc),
        ], order='start_time')

        covered = 0.0
        cursor = s_utc
        for slot in slots:
            slot_start = max(slot.start_time, s_utc)
            slot_end = min(slot.end_time, e_utc)
            if slot_end <= cursor:
                continue
            covered += (slot_end - max(slot_start, cursor)).total_seconds()
            cursor = slot_end
        return max(0.0, total_sec - covered)

    def get_previous_worked_shift_window(self, wc=None):
        boundary_loc, _e = self.get_current_shift_window(wc)
        if not boundary_loc or not wc:
            return None, None

        for _step in range(self.MAX_SHIFT_LOOKBACK):
            ps_loc, pe_loc = self._shift_window_before(boundary_loc, wc)
            if not ps_loc:
                return None, None
            planned_sec = self._planned_seconds_for(wc, ps_loc, pe_loc)
            if planned_sec > self.MIN_PLANNED_SECONDS:
                return ps_loc, pe_loc
            _logger.info("Shift %s - %s skipped for %s: no planned working time",
                         ps_loc, pe_loc, wc.name)
            boundary_loc = ps_loc
        _logger.info("No worked shift found for %s within the last %s shifts",
                     wc.name if wc else '-', self.MAX_SHIFT_LOOKBACK)
        return None, None

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
        name_map = {r.name: r for r in existing if r.name}
        parent_cache = {r.name: r.id for r in existing if r.name}

        for item in data_list:
            name = item.get('name', '').strip()
            code = item.get('code', '').strip()
            parent_name = item.get('parent_name', '').strip()
            vals = item.get('vals', {})

            if not name:
                continue

            if name == parent_name:
                parent_name = ''

            parent_id = False
            if parent_name:
                if parent_name not in parent_cache:
                    new_parent = self.create({'name': parent_name})
                    parent_cache[parent_name] = new_parent.id
                    name_map[parent_name] = new_parent
                parent_id = parent_cache[parent_name]

            vals.update({'name': name})
            if parent_name:
                vals['parent_id'] = parent_id
            if code:
                vals['code'] = code

            rec = code_map.get(code) if code else None
            if not rec:
                rec = name_map.get(name)

            if rec:
                if parent_id and parent_id == rec.id:
                    vals.pop('parent_id', None)
                
                try:
                    with self.env.cr.savepoint():
                        rec.write(vals)
                except Exception as e:
                    vals.pop('parent_id', None)
                    rec.write(vals)

                if code and code not in code_map:
                    code_map[code] = rec
                name_map[name] = rec
                parent_cache[name] = rec.id
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
    complete_name = fields.Char('Complete Name', compute='_compute_complete_name', store=True,
                                recursive=True)

    default_OPCTag = fields.Char(string='Default OPC Tag')
    stream_id = fields.Many2one('mes.stream', string='Stream', index=True, ondelete='restrict')
    part_id = fields.Many2one('mes.part', string='Part', index=True, ondelete='restrict',
                              domain="stream_id and ['|', ('stream_id', '=', False), ('stream_id', '=', stream_id)] or []")
    piece_id = fields.Many2one('mes.piece', string='Piece', index=True, ondelete='restrict',
                               domain="[('part_id', '=', part_id)]")
    part_has_pieces = fields.Boolean(string='Part Has Pieces',
                                     compute='_compute_part_has_pieces')
    calculable = fields.Boolean(string='Calculable', default=False, index=True)
    hide_in_kiosk = fields.Boolean(
        string='Hide In Kiosk', default=False, index=True,
        help="Keep this record out of the kiosk selection lists. Use it for parent "
             "groups and any entry an operator should never pick directly.")

    @api.depends('part_id', 'part_id.piece_ids')
    def _compute_part_has_pieces(self):
        for rec in self:
            rec.part_has_pieces = bool(rec.part_id.piece_ids)

    @api.onchange('part_id')
    def _onchange_part_id(self):
        for rec in self:
            if rec.part_id:
                if rec.part_id.stream_id:
                    rec.stream_id = rec.part_id.stream_id
                if rec.piece_id and rec.piece_id.part_id != rec.part_id:
                    rec.piece_id = False
            else:
                rec.piece_id = False

    @api.onchange('stream_id')
    def _onchange_stream_id(self):
        for rec in self:
            if rec.stream_id and rec.part_id and rec.part_id.stream_id \
                    and rec.part_id.stream_id != rec.stream_id:
                rec.part_id = False
                rec.piece_id = False

    @api.constrains('part_id', 'piece_id', 'stream_id')
    def _check_part_stream_consistency(self):
        for rec in self:
            if rec.part_id and rec.part_id.stream_id and rec.stream_id \
                    and rec.part_id.stream_id != rec.stream_id:
                raise ValidationError(_(
                    "Stream mismatch on '%(record)s': the record is on stream "
                    "'%(record_stream)s' while part '%(part)s' belongs to stream "
                    "'%(part_stream)s'.\n\n"
                    "Clear the stream, pick a part from the same stream or a part "
                    "without a stream, or move the part to this stream.",
                    record=rec.display_name,
                    record_stream=rec.stream_id.name,
                    part=rec.part_id.name,
                    part_stream=rec.part_id.stream_id.name))
            if rec.piece_id and rec.piece_id.part_id != rec.part_id:
                raise ValidationError(_(
                    "Piece '%(piece)s' does not belong to part '%(part)s'.",
                    piece=rec.piece_id.name,
                    part=rec.part_id.name or '-'))
    is_cumulative = fields.Boolean(string='Cumulative (MAX-MIN)', default=False)
    is_module_count = fields.Boolean(string='Is Module Count')
    wheel = fields.Integer(string='Wheel')
    module = fields.Integer(string='Module')
    related_count_ids = fields.Many2many(
        'mes.counts', 'mes_count_related_rel', 'count_id', 'related_id',
        string='Related Counts',
    )

    @api.depends('name', 'parent_id.complete_name')
    def _compute_complete_name(self):
        for count in self:
            if count.parent_id:
                count.complete_name = '%s / %s' % (count.parent_id.complete_name, count.name)
            else:
                count.complete_name = count.name

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
    complete_name = fields.Char('Complete Name', compute='_compute_complete_name', store=True,
                                recursive=True)

    color = fields.Char(string='Color', default='#808080')
    stream_id = fields.Many2one('mes.stream', string='Stream', index=True, ondelete='restrict')
    part_id = fields.Many2one('mes.part', string='Part', index=True, ondelete='restrict',
                              domain="stream_id and ['|', ('stream_id', '=', False), ('stream_id', '=', stream_id)] or []")
    piece_id = fields.Many2one('mes.piece', string='Piece', index=True, ondelete='restrict',
                               domain="[('part_id', '=', part_id)]")
    part_has_pieces = fields.Boolean(string='Part Has Pieces',
                                     compute='_compute_part_has_pieces')
    calculable = fields.Boolean(string='Calculable', default=False, index=True)
    hide_in_kiosk = fields.Boolean(
        string='Hide In Kiosk', default=False, index=True,
        help="Keep this record out of the kiosk selection lists. Use it for parent "
             "groups and any entry an operator should never pick directly.")

    @api.depends('part_id', 'part_id.piece_ids')
    def _compute_part_has_pieces(self):
        for rec in self:
            rec.part_has_pieces = bool(rec.part_id.piece_ids)

    @api.onchange('part_id')
    def _onchange_part_id(self):
        for rec in self:
            if rec.part_id:
                if rec.part_id.stream_id:
                    rec.stream_id = rec.part_id.stream_id
                if rec.piece_id and rec.piece_id.part_id != rec.part_id:
                    rec.piece_id = False
            else:
                rec.piece_id = False

    @api.onchange('stream_id')
    def _onchange_stream_id(self):
        for rec in self:
            if rec.stream_id and rec.part_id and rec.part_id.stream_id \
                    and rec.part_id.stream_id != rec.stream_id:
                rec.part_id = False
                rec.piece_id = False

    @api.constrains('part_id', 'piece_id', 'stream_id')
    def _check_part_stream_consistency(self):
        for rec in self:
            if rec.part_id and rec.part_id.stream_id and rec.stream_id \
                    and rec.part_id.stream_id != rec.stream_id:
                raise ValidationError(_(
                    "Stream mismatch on '%(record)s': the record is on stream "
                    "'%(record_stream)s' while part '%(part)s' belongs to stream "
                    "'%(part_stream)s'.\n\n"
                    "Clear the stream, pick a part from the same stream or a part "
                    "without a stream, or move the part to this stream.",
                    record=rec.display_name,
                    record_stream=rec.stream_id.name,
                    part=rec.part_id.name,
                    part_stream=rec.part_id.stream_id.name))
            if rec.piece_id and rec.piece_id.part_id != rec.part_id:
                raise ValidationError(_(
                    "Piece '%(piece)s' does not belong to part '%(part)s'.",
                    piece=rec.piece_id.name,
                    part=rec.part_id.name or '-'))
    exclude_from_anomalies = fields.Boolean(
        string='Exclude From Anomaly Report',
        default=False,
        help="Stops with this reason, such as technician work or sampling, are not judged in the "
             "anomaly report.")
    is_manual_stop = fields.Boolean(
        string='Is Manual Stop',
        default=False,
        help="Marks a stoppage reason that an operator can pick manually from "
             "the kiosk instead of it arriving from the PLC.")
    default_event_tag_type = fields.Selection([
        ('OEE.nMachineState', 'Machine State (OEE.nMachineState)'),
        ('OEE.nStopRootReason', 'Stop Reason (OEE.nStopRootReason)')
    ], string='Default Tag Type')
    default_plc_value = fields.Integer(string='Default PLC Value')
    priority = fields.Selection([
        ('0', 'Low'),
        ('1', 'Medium'),
        ('2', 'High'),
        ('3', 'Blocker'),
    ], string='Priority', default='0', required=True,
        help="Used to set the priority of a Call-for-Help task created with "
             "problem area = Alarm and this event as the stoppage reason.")

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
    complete_name = fields.Char('Complete Name', compute='_compute_complete_name', store=True,
                                recursive=True)
    default_OPCTag = fields.Char(string='Default OPC Tag')
    stream_id = fields.Many2one('mes.stream', string='Stream', index=True, ondelete='restrict')
    part_id = fields.Many2one('mes.part', string='Part', index=True, ondelete='restrict',
                              domain="stream_id and ['|', ('stream_id', '=', False), ('stream_id', '=', stream_id)] or []")
    piece_id = fields.Many2one('mes.piece', string='Piece', index=True, ondelete='restrict',
                               domain="[('part_id', '=', part_id)]")
    part_has_pieces = fields.Boolean(string='Part Has Pieces',
                                     compute='_compute_part_has_pieces')
    calculable = fields.Boolean(string='Calculable', default=False, index=True)
    hide_in_kiosk = fields.Boolean(
        string='Hide In Kiosk', default=False, index=True,
        help="Keep this record out of the kiosk selection lists. Use it for parent "
             "groups and any entry an operator should never pick directly.")

    @api.depends('part_id', 'part_id.piece_ids')
    def _compute_part_has_pieces(self):
        for rec in self:
            rec.part_has_pieces = bool(rec.part_id.piece_ids)

    @api.onchange('part_id')
    def _onchange_part_id(self):
        for rec in self:
            if rec.part_id:
                if rec.part_id.stream_id:
                    rec.stream_id = rec.part_id.stream_id
                if rec.piece_id and rec.piece_id.part_id != rec.part_id:
                    rec.piece_id = False
            else:
                rec.piece_id = False

    @api.onchange('stream_id')
    def _onchange_stream_id(self):
        for rec in self:
            if rec.stream_id and rec.part_id and rec.part_id.stream_id \
                    and rec.part_id.stream_id != rec.stream_id:
                rec.part_id = False
                rec.piece_id = False

    @api.constrains('part_id', 'piece_id', 'stream_id')
    def _check_part_stream_consistency(self):
        for rec in self:
            if rec.part_id and rec.part_id.stream_id and rec.stream_id \
                    and rec.part_id.stream_id != rec.stream_id:
                raise ValidationError(_(
                    "Stream mismatch on '%(record)s': the record is on stream "
                    "'%(record_stream)s' while part '%(part)s' belongs to stream "
                    "'%(part_stream)s'.\n\n"
                    "Clear the stream, pick a part from the same stream or a part "
                    "without a stream, or move the part to this stream.",
                    record=rec.display_name,
                    record_stream=rec.stream_id.name,
                    part=rec.part_id.name,
                    part_stream=rec.part_id.stream_id.name))
            if rec.piece_id and rec.piece_id.part_id != rec.part_id:
                raise ValidationError(_(
                    "Piece '%(piece)s' does not belong to part '%(part)s'.",
                    piece=rec.piece_id.name,
                    part=rec.part_id.name or '-'))
    related_process_ids = fields.Many2many('mes.process', 'mes_process_related_rel', 'process_id', 'related_id', string='Related Processes')

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

    is_hist_syncing = fields.Boolean(string='Historical Sync in Progress', default=False)

    machine_number = fields.Integer(string='Machine Number')
    maintainx_id = fields.Integer(string='MaintainX ID')
    code_imatec = fields.Char(string='Imatec Name')
    machine_settings_id = fields.Many2one('mes.machine.settings', string='Telemetry Settings')
    stream_ids = fields.Many2many(
        'mes.stream',
        'mes_stream_workcenter_rel', 'workcenter_id', 'stream_id',
        string='Available Streams',
    )
    runtime_event_id = fields.Many2one('mes.event', string='Runtime Event')
    production_count_id = fields.Many2one('mes.counts', string='Production Count')
    refresh_frequency = fields.Integer(string='Refresh Frequency (sec)', default=60)
    ideal_capacity_per_min = fields.Float(string='Ideal Capacity (Parts/Min)', default=200.0)
    operator_feedback_delay = fields.Integer(
        string='Operator Feedback Delay (min)',
        default=0,
        help="After a supervisor marks a Call-for-Help request as done, the operator "
             "confirmation prompt (Confirm / Not Done) waits this many minutes before "
             "appearing on the kiosk. 0 shows it immediately.")

    current_oee = fields.Float(string="OEE (%)", readonly=True, default=0.0)
    current_availability = fields.Float(string="Availability (%)", readonly=True, default=0.0)
    current_performance = fields.Float(string="Performance (%)", readonly=True, default=0.0)
    current_quality = fields.Float(string="Quality (%)", readonly=True, default=0.0)
    current_produced = fields.Float(string="Produced", digits=(16, 0), readonly=True, default=0)
    current_waste_losses = fields.Float(string="Waste Losses", readonly=True, default=0.0)
    current_downtime_losses = fields.Float(string="Downtime Losses", readonly=True, default=0.0)
    current_first_running_time = fields.Datetime(string="First Running Time", readonly=True)
    current_runtime_formatted = fields.Char(string="Runtime", readonly=True, default='00:00:00')
    current_produced_per_hour = fields.Float(string="Produced/h Runtime", readonly=True, default=0.0)
    current_produced_per_planned_hour = fields.Float(string="Produced/h Planned", readonly=True, default=0.0)
    current_top_rejection = fields.Char(string="Top Rejection", readonly=True, default='None')
    current_top_alarm = fields.Char(string="Top Alarm", readonly=True, default='None')
    chart_bucket_minutes = fields.Integer(string='Chart Bucket (Min)', default=15)
    allowed_pc_ips = fields.Char(string='All Allowed PC IPs')
    company_id = fields.Many2one('res.company', string='Company', required=True, default=lambda self: self.env.company)
    
    current_first_running_time_disp = fields.Char(
        compute='_compute_current_first_running_time_disp',
        store=False
    )

    telemetry_state_logic = fields.Selection([
        ('events', 'Through Events'),
        ('states', 'By State Flags')
    ], string='Telemetry FSM Logic', default='events', required=True)

    part_line_ids = fields.One2many('mes.workcenter.part', 'workcenter_id',
                                    string='Parts')

    stream_canvas_width = fields.Integer(
        string='Stream Canvas Width (px)', default=0,
        help="Width of the kiosk stream button area. Leave 0 to size it automatically "
             "from the default stream positions.")
    stream_canvas_height = fields.Integer(
        string='Stream Canvas Height (px)', default=0,
        help="Height of the kiosk stream button area. Leave 0 to size it automatically "
             "from the default stream positions.")

    maintainx_procedure_template_id = fields.Char(
        string='MaintainX Procedure Template ID', copy=False,
        help="Numeric ID of the MaintainX procedure template attached to work orders "
             "created from Call for Help on this work center. Leave empty to create "
             "work orders without a procedure.")

    operator_stop_event_id = fields.Many2one('mes.event', string='Operator Stop Event',
                                             help='Event treated as "Operator Stop" for kiosk help-request logic.')

    kiosk_session_ids = fields.One2many('mes.kiosk.session', 'workcenter_id')
    active_operator_session_id = fields.Many2one('mes.kiosk.session', compute='_compute_active_sessions', store=False)
    active_supervisor_session_id = fields.Many2one('mes.kiosk.session', compute='_compute_active_sessions', store=False)
    active_technician_session_id = fields.Many2one('mes.kiosk.session', compute='_compute_active_sessions', store=False)
    open_help_request_id = fields.Many2one('mes.task', compute='_compute_open_help_request', store=False)
    open_help_request_count = fields.Integer(compute='_compute_open_help_request', store=False)
    has_open_help_request = fields.Boolean(compute='_compute_open_help_request', store=False)

    @api.depends('kiosk_session_ids.is_active', 'kiosk_session_ids.role')
    def _compute_active_sessions(self):
        for wc in self:
            active = wc.kiosk_session_ids.filtered('is_active')
            wc.active_operator_session_id = active.filtered(lambda s: s.role == 'operator')[:1]
            wc.active_supervisor_session_id = active.filtered(lambda s: s.role == 'supervisor')[:1]
            wc.active_technician_session_id = active.filtered(lambda s: s.role == 'technician')[:1]

    def _compute_open_help_request(self):
        for wc in self:
            tasks = wc._get_open_help_requests()
            wc.open_help_request_id = tasks[:1]
            wc.open_help_request_count = len(tasks)
            wc.has_open_help_request = bool(tasks)

    def _get_open_help_requests(self):
        self.ensure_one()
        return self.env['mes.task'].sudo().search([
            ('machine_id', '=', self.id),
            ('task_type', '=', 'kiosk_help'),
            ('parent_id', '=', False),
            ('state', 'not in', ['help_completed', 'help_rejected', 'cancel']),
        ], order='priority desc, id desc')

    mes_status_label = fields.Char(
        string='Current Status', compute='_compute_mes_current_status', store=False)
    mes_status_color = fields.Char(
        string='Current Status Color', compute='_compute_mes_current_status', store=False)
    mes_status_since = fields.Datetime(
        string='Current Status Since', compute='_compute_mes_current_status', store=False)
    mes_status_provisional = fields.Boolean(
        string='Current Stop Reason Pending', compute='_compute_mes_current_status', store=False)

    _STATUS_FALLBACK_COLORS = {
        'mes.performance.alarm': '#dc3545',
        'mes.performance.slowing': '#6c757d',
        'mes.performance.running': '#28a745',
    }

    _STATUS_SOURCE_TABLES = (
        ('mes_performance_alarm', 'mes.performance.alarm', 'Alarm'),
        ('mes_performance_slowing', 'mes.performance.slowing', 'Slowing'),
        ('mes_performance_running', 'mes.performance.running', 'Running'),
    )

    def _compute_mes_current_status(self):
        statuses = self._fetch_current_status_map()
        for wc in self:
            status = statuses.get(wc.id)
            wc.mes_status_label = status['label'] if status else ''
            wc.mes_status_color = status['color'] if status else ''
            wc.mes_status_since = status['since'] if status else False
            wc.mes_status_provisional = status['provisional'] if status else False

    def _fetch_current_status_map(self):
        machine_ids = tuple(self.ids)
        if not machine_ids:
            return {}

        union_parts = []
        for table, model_name, _default_label in self._STATUS_SOURCE_TABLES:
            provisional = 'COALESCE(reason_provisional, FALSE)' if model_name == 'mes.performance.alarm' else 'FALSE'
            union_parts.append(
                f"SELECT performance_id, loss_id, start_time, '{model_name}' AS source, "
                f"{provisional} AS provisional "
                f"FROM {table} WHERE end_time IS NULL"
            )
        query = f"""
            SELECT DISTINCT ON (perf.machine_id)
                   perf.machine_id, logs.source, evt.name AS loss_name, evt.color AS loss_color,
                   logs.start_time, logs.provisional
            FROM ({' UNION ALL '.join(union_parts)}) AS logs
            JOIN mes_machine_performance perf ON perf.id = logs.performance_id
            LEFT JOIN mes_event evt ON evt.id = logs.loss_id
            WHERE perf.machine_id IN %s
            ORDER BY perf.machine_id, logs.start_time DESC
        """
        self.env.cr.execute(query, (machine_ids,))

        default_labels = {model: label for _t, model, label in self._STATUS_SOURCE_TABLES}
        statuses = {}
        for machine_id, source, loss_name, loss_color, since, provisional in self.env.cr.fetchall():
            statuses[machine_id] = {
                'label': loss_name or default_labels.get(source, ''),
                'color': loss_color or self._STATUS_FALLBACK_COLORS.get(source, '#6c757d'),
                'since': since,
                'provisional': bool(provisional),
            }
        return statuses

    def _get_current_open_alarm(self):
        self.ensure_one()
        return self.env['mes.performance.alarm'].sudo().search([
            ('performance_id.machine_id', '=', self.id),
            ('end_time', '=', False),
        ], order='start_time desc', limit=1)

    def _get_available_streams(self):
        self.ensure_one()
        return self.stream_ids.filtered('active').sorted(lambda s: (s.sequence, s.name))

    def _get_stream_layout_map(self):
        self.ensure_one()
        layouts = self.env['mes.stream.layout'].sudo().search([('workcenter_id', '=', self.id)])
        return {layout.stream_id.id: layout for layout in layouts}

    def _kiosk_header_badges(self):
        self.ensure_one()
        outage = self.env['mes.link.outage'].sudo().search([
            ('workcenter_id', '=', self.id),
            ('state', '=', 'ongoing'),
        ], limit=1)
        if not outage:
            return []
        tz = pytz.timezone(self.company_id.tz or 'UTC')
        since = pytz.utc.localize(outage.date_start).astimezone(tz)
        today = pytz.utc.localize(fields.Datetime.now()).astimezone(tz).date()
        stamp = since.strftime('%H:%M') if since.date() == today else since.strftime('%d/%m %H:%M')
        if outage.cause == 'ads':
            name = _("PLC not readable since %s", stamp)
        else:
            name = _("Logger offline since %s", stamp)
        return [{
            'key': 'link',
            'title': _("Connection"),
            'name': name,
            'details': [],
            'tone': 'warning',
            'note': _("Stops and counts of this machine are not being recorded."),
        }]

    def get_stream_layout(self):
        self.ensure_one()
        streams = self._get_available_streams()
        layout_map = self._get_stream_layout_map()
        items = []
        for stream in streams:
            layout = layout_map.get(stream.id)
            items.append({
                'stream_id': stream.id,
                'name': stream.name,
                'code': stream.code or '',
                'image_url': f'/web/image/mes.stream/{stream.id}/image' if stream.image else '',
                'width': stream.button_width or 120,
                'height': stream.button_height or 120,
                'pos_x': layout.pos_x if layout else stream.default_pos_x,
                'pos_y': layout.pos_y if layout else stream.default_pos_y,
            })
        configured = bool(self.stream_canvas_width and self.stream_canvas_height)
        if configured:
            canvas_width = self.stream_canvas_width
            canvas_height = self.stream_canvas_height
        else:
            canvas_width, canvas_height = self._compute_stream_canvas_size(streams)
        return {
            'workcenter_id': self.id,
            'canvas_width': canvas_width,
            'canvas_height': canvas_height,
            'configured': configured,
            'items': items,
        }

    def _compute_stream_canvas_size(self, streams):
        total_width = sum(stream.button_width or 120 for stream in streams)
        total_height = sum(stream.button_height or 120 for stream in streams)
        return max(total_width, 1), max(total_height, 1)

    def save_stream_canvas(self, width, height):
        self.ensure_one()
        self.sudo().write({
            'stream_canvas_width': max(0, min(4000, int(width or 0))),
            'stream_canvas_height': max(0, min(4000, int(height or 0))),
        })
        return True

    def save_stream_layout(self, items):
        self.ensure_one()
        Layout = self.env['mes.stream.layout'].sudo()
        allowed_ids = set(self._get_available_streams().ids)
        for item in items or []:
            stream_id = int(item.get('stream_id') or 0)
            if stream_id not in allowed_ids:
                continue
            vals = {
                'pos_x': min(100.0, max(0.0, float(item.get('pos_x') or 0.0))),
                'pos_y': min(100.0, max(0.0, float(item.get('pos_y') or 0.0))),
            }
            layout = Layout.search([
                ('workcenter_id', '=', self.id),
                ('stream_id', '=', stream_id),
            ], limit=1)
            if layout:
                layout.write(vals)
            else:
                Layout.create(dict(vals, workcenter_id=self.id, stream_id=stream_id))
        Layout.search([
            ('workcenter_id', '=', self.id),
            ('stream_id', 'not in', list(allowed_ids)),
        ]).unlink()
        return True

    def _get_manual_stop_events_for_stream(self, stream):
        self.ensure_one()
        if stream not in self._get_available_streams():
            return self.env['mes.event']
        return self.env['mes.event'].sudo().search([
            '|', ('stream_id', '=', stream.id), ('stream_id', '=', False),
            ('is_manual_stop', '=', True),
            ('hide_in_kiosk', '=', False),
        ], order='complete_name')

    def _active_kiosk_sessions(self):
        self.ensure_one()
        order = {'operator': 0, 'supervisor': 1, 'technician': 2}
        return self.sudo().kiosk_session_ids.filtered('is_active').sorted(
            lambda session: (order.get(session.role, 9), session.login_at))

    def _kiosk_permissions(self):
        self.ensure_one()
        company = self.company_id or self.env.company
        return {feature: company._kiosk_role_codes(feature) for feature in ('planned_downtime', 'help')}

    def _kiosk_allowed_session(self, feature, session_id):
        self.ensure_one()
        session = self.env['mes.kiosk.session'].sudo().browse(int(session_id or 0)).exists()
        if not session or session not in self._active_kiosk_sessions():
            return session.browse()
        if session.role not in self._kiosk_permissions()[feature]:
            return session.browse()
        return session

    def _is_manual_stop_available(self, event):
        self.ensure_one()
        if not event or not event.is_manual_stop or event.hide_in_kiosk:
            return False
        if not event.stream_id:
            return True
        return event in self._get_manual_stop_events_for_stream(event.stream_id)

    def _get_top_recent_non_production_count(self, minutes=5):
        self.ensure_one()
        machine = self.machine_settings_id
        if not machine:
            return self.env['mes.counts']
        production_tag_ids = machine.count_tag_ids.filtered(
            lambda c: c.count_id == self.production_count_id
        ).ids
        candidate_tags = machine.count_tag_ids.filtered(lambda c: c.id not in production_tag_ids)
        if not candidate_tags:
            return self.env['mes.counts']
        from datetime import timedelta
        import pytz
        now_utc = fields.Datetime.now()
        start_utc = now_utc - timedelta(minutes=minutes)
        tz = pytz.timezone(self.company_id.tz or 'UTC')
        start_local = pytz.utc.localize(start_utc).astimezone(tz).replace(tzinfo=None)
        end_local = pytz.utc.localize(now_utc).astimezone(tz).replace(tzinfo=None)
        try:
            with self.env['mes.timescale.base']._connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT tag_name, MAX(value) - MIN(value) AS delta
                        FROM telemetry_count
                        WHERE machine_name = %s
                          AND tag_name = ANY(%s)
                          AND time >= %s AND time <= %s
                        GROUP BY tag_name
                        ORDER BY delta DESC
                        LIMIT 1
                    """, (machine.name, candidate_tags.mapped('tag_name'), start_local, end_local))
                    row = cur.fetchone()
        except Exception:
            return self.env['mes.counts']
        if not row or not row[1]:
            return self.env['mes.counts']
        winning_tag = candidate_tags.filtered(lambda c: c.tag_name == row[0])[:1]
        return winning_tag.count_id

    def _get_alarm_frequency_options(self, days=7, limit=15):
        self.ensure_one()
        from datetime import timedelta
        Alarm = self.env['mes.performance.alarm'].sudo()
        since = fields.Datetime.now() - timedelta(days=days)
        groups = Alarm.read_group(
            domain=[
                ('performance_id.machine_id', '=', self.id),
                ('start_time', '>=', since),
            ],
            fields=['loss_id'],
            groupby=['loss_id'],
            orderby='loss_id_count desc',
            limit=limit,
        )
        options = []
        seen = set()
        hidden_ids = set(self.env['mes.event'].sudo().search([
            ('hide_in_kiosk', '=', True)]).ids)
        for g in groups:
            if not g.get('loss_id'):
                continue
            loss_id, loss_name = g['loss_id']
            if loss_id in hidden_ids:
                continue
            options.append({'id': loss_id, 'label': loss_name, 'count': g.get('loss_id_count', 0)})
            seen.add(loss_id)
        if self.machine_settings_id:
            mapped = self.env['mes.signal.event'].sudo().search([
                ('machine_id', '=', self.machine_settings_id.id),
            ]).mapped('event_id').filtered(lambda e: not e.hide_in_kiosk)
            for evt in mapped:
                if evt.id not in seen and len(options) < 30:
                    options.append({'id': evt.id, 'label': evt.name, 'count': 0})
                    seen.add(evt.id)
        return options

    def _get_most_frequent_alarm(self, hours=24):
        self.ensure_one()
        from datetime import timedelta
        Alarm = self.env['mes.performance.alarm'].sudo()
        since = fields.Datetime.now() - timedelta(hours=hours)
        groups = Alarm.read_group(
            domain=[
                ('performance_id.machine_id', '=', self.id),
                ('start_time', '>=', since),
            ],
            fields=['loss_id'],
            groupby=['loss_id'],
            orderby='loss_id_count desc',
            limit=1,
        )
        if groups and groups[0].get('loss_id'):
            return self.env['mes.event'].sudo().browse(groups[0]['loss_id'][0])
        return self.env['mes.event']

    def _get_runtime_hours(self, start_utc, end_utc):
        self.ensure_one()
        Running = self.env['mes.performance.running'].sudo()
        records = Running.search([
            ('performance_id.machine_id', '=', self.id),
            ('start_time', '<=', end_utc),
            '|', ('end_time', '>=', start_utc), ('end_time', '=', False),
        ])
        total_sec = 0.0
        for rec in records:
            s = max(rec.start_time, start_utc)
            e = min(rec.end_time or end_utc, end_utc)
            if e > s:
                total_sec += (e - s).total_seconds()
        return total_sec / 3600.0

    def _get_reject_rate_candidates(self, hours=8):
        self.ensure_one()
        machine = self.machine_settings_id
        if not machine:
            return []
        candidate_tags = machine.count_tag_ids.filtered(lambda c: c.count_id and c.count_id.module)
        if not candidate_tags:
            return []
        from datetime import timedelta
        import pytz
        now_utc = fields.Datetime.now()
        start_utc = now_utc - timedelta(hours=hours)
        tz = pytz.timezone(self.company_id.tz or 'UTC')
        start_local = pytz.utc.localize(start_utc).astimezone(tz).replace(tzinfo=None)
        end_local = pytz.utc.localize(now_utc).astimezone(tz).replace(tzinfo=None)
        deltas = {}
        try:
            with self.env['mes.timescale.base']._connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT tag_name,
                               SUM(value) AS sum_val,
                               MAX(value) - MIN(value) AS cum_val
                        FROM telemetry_count
                        WHERE machine_name = %s
                          AND tag_name = ANY(%s)
                          AND time >= %s AND time <= %s
                        GROUP BY tag_name
                    """, (machine.name, candidate_tags.mapped('tag_name'), start_local, end_local))
                    for tag, sum_val, cum_val in cur.fetchall():
                        deltas[tag] = (float(sum_val or 0), float(cum_val or 0))
        except Exception:
            return []
        runtime_h = self._get_runtime_hours(start_utc, now_utc)
        divisor = runtime_h if runtime_h > 0.05 else float(hours)
        per_count = {}
        for sig in candidate_tags:
            sum_val, cum_val = deltas.get(sig.tag_name, (0.0, 0.0))
            qty = cum_val if (sig.is_cumulative or sig.count_id.is_cumulative) else sum_val
            node = per_count.setdefault(sig.count_id.id, {'count': sig.count_id, 'qty': 0.0})
            node['qty'] += qty
        result = []
        for node in per_count.values():
            rate = node['qty'] / divisor
            result.append({
                'id': node['count'].id,
                'label': node['count'].display_name,
                'qty': round(node['qty'], 1),
                'rate': round(rate, 2),
            })
        result.sort(key=lambda r: r['rate'], reverse=True)
        return result

    @api.depends('current_first_running_time')
    def _compute_current_first_running_time_disp(self):
        for rec in self:
            if rec.current_first_running_time:
                tz_name = rec.company_id.tz or 'UTC'
                mac_tz = pytz.timezone(tz_name)
                utc_dt = pytz.utc.localize(rec.current_first_running_time)
                loc_dt = utc_dt.astimezone(mac_tz)
                rec.current_first_running_time_disp = loc_dt.strftime('%d.%m.%Y %H:%M:%S')
            else:
                rec.current_first_running_time_disp = False

    _sql_constraints = [
        ('code_imatec_uniq', 'unique(code_imatec)', 'Imatec Code must be unique!')
    ]

    def _get_pending_operator_stops(self, limit=200):
        self.ensure_one()
        operator_event = self.operator_stop_event_id
        if not operator_event:
            return []

        start_loc, end_loc = self.env['mes.shift'].sudo().get_current_shift_window(self)
        if not start_loc:
            return []

        machine_tz = pytz.timezone(self.company_id.tz or 'UTC')
        start_utc = machine_tz.localize(start_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
        end_utc = machine_tz.localize(end_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
        now_utc = fields.Datetime.now()
        end_utc = min(end_utc, now_utc)
        if end_utc <= start_utc:
            return []

        alarms = self.env['mes.performance.alarm'].sudo().search([
            ('performance_id.machine_id', '=', self.id),
            ('loss_id', '=', operator_event.id),
            ('start_time', '>=', start_utc),
            ('start_time', '<', end_utc),
            '|', ('comment', '=', False), ('comment', '=', ''),
        ], order='start_time desc', limit=limit)
        if not alarms:
            return []

        excluded = self._collect_explained_intervals(start_utc, end_utc)
        results = []
        for alarm in alarms:
            alarm_end = alarm.end_time or now_utc
            if any(alarm.start_time < window_end and alarm_end > window_start
                   for window_start, window_end in excluded):
                continue
            results.append({
                'model': 'mes.performance.alarm',
                'id': alarm.id,
                'name': operator_event.name,
                'start': fields.Datetime.to_string(alarm.start_time),
                'end': fields.Datetime.to_string(alarm.end_time) if alarm.end_time else False,
                'is_open': not alarm.end_time,
                'duration': (alarm_end - alarm.start_time).total_seconds(),
                'comment': '',
                'is_manual_stop': True,
            })
        return results

    def _collect_explained_intervals(self, start_utc, end_utc):
        self.ensure_one()
        now_utc = fields.Datetime.now()
        windows = []

        planned = self.env['mes.flat.downtime'].sudo().search([
            ('machine_id', '=', self.id),
            ('start_time', '<', end_utc),
            ('end_time', '>', start_utc),
        ])
        windows.extend((slot.start_time, slot.end_time) for slot in planned)

        intervals = self.env['mes.task.work.interval'].sudo().search([
            ('machine_id', '=', self.id),
            ('start_time', '<', end_utc),
            '|', ('end_time', '=', False), ('end_time', '>', start_utc),
        ])
        windows.extend((interval.start_time, interval.end_time or now_utc)
                       for interval in intervals)
        return windows

    def _lookup_event_readonly(self, tag_name, plc_value):
        self.ensure_one()
        machine = self.machine_settings_id
        if machine:
            signal = self.env['mes.signal.event'].sudo().search([
                ('machine_id', '=', machine.id),
                ('tag_name', '=', tag_name),
                ('plc_value', '=', plc_value),
            ], limit=1)
            if signal:
                return signal.event_id
        return self.env['mes.event'].sudo().search([
            ('default_event_tag_type', '=', tag_name),
            ('default_plc_value', '=', plc_value),
        ], limit=1)

    def build_live_status(self, machine_state, state_since, reason_value=None):
        self.ensure_one()
        machine_state = int(machine_state)
        event = self.env['mes.event']
        status = 'slowing'

        if machine_state == MACHINE_STATE_STOPPED:
            status = 'alarm'
            if reason_value:
                event = self._lookup_event_readonly('OEE.nStopRootReason', int(reason_value))
        else:
            event = self._lookup_event_readonly('OEE.nMachineState', machine_state)
            if event and self.runtime_event_id and event.id == self.runtime_event_id.id:
                status = 'running'

        defaults = {
            'running': ('Running', '#28a745'),
            'alarm': ('Stopped', '#dc3545'),
            'slowing': ('Slowing', '#6c757d'),
        }
        default_name, default_color = defaults[status]
        return {
            'workcenter_id': self.id,
            'machine_state': machine_state,
            'state_since': state_since,
            'status': status,
            'name': event.name if event else default_name,
            'color': (event.color if event and event.color else default_color),
            'has_reason': bool(event),
            'event_id': event.id if event else False,
        }

    def _notify_kiosk_live_status(self, machine_state, state_since, reason_value=None):
        self.ensure_one()
        payload = self.build_live_status(machine_state, state_since, reason_value)
        bus = self.env['bus.bus']
        bus._sendone(
            f'mes.kiosk.workcenter.{self.id}',
            'machine_live_state',
            payload,
        )
        bus._sendone(
            WORKCENTER_STATUS_CHANNEL,
            'workcenter_status_color',
            {
                'workcenter_id': self.id,
                'color': payload['color'],
                'label': payload['name'],
                'state_since': payload['state_since'],
                'has_reason': payload['has_reason'],
            },
        )

    def _notify_kiosk_state_refresh(self):
        bus = self.env['bus.bus']
        for model in ('mes.performance.alarm', 'mes.performance.slowing', 'mes.performance.running'):
            self.env[model].flush_model()
        statuses = self._fetch_current_status_map()
        for workcenter in self:
            bus._sendone(
                f'mes.kiosk.workcenter.{workcenter.id}',
                'machine_state_changed',
                {'workcenter_id': workcenter.id},
            )
            status = statuses.get(workcenter.id)
            if not status:
                continue
            bus._sendone(
                WORKCENTER_STATUS_CHANNEL,
                'workcenter_status_saved',
                {
                    'workcenter_id': workcenter.id,
                    'label': status['label'],
                    'color': status['color'],
                    'since': fields.Datetime.to_string(status['since']),
                    'provisional': status['provisional'],
                },
            )

    def action_sync_parts_with_maintainx(self):
        self.ensure_one()
        wizard = self.env['mes.part.sync.wizard'].open_for_workcenter(self)
        return {
            'type': 'ir.actions.act_window',
            'name': _("Sync Parts With MaintainX"),
            'res_model': 'mes.part.sync.wizard',
            'res_id': wizard.id,
            'view_mode': 'form',
            'target': 'new',
        }

    def _enqueue_fsm_sync(self):
        self.ensure_one()
        self.env['mes.machine.performance'].sudo().with_delay(
            channel='root.mes_fsm',
            priority=5,
            eta=FSM_SYNC_JOB_ETA_SEC,
            description=f"FSM sync WC={self.id} ({self.name})",
            identity_key=f"fsm_sync_{self.id}",
        )._sync_machine_fsm_job(self.id)

    def cron_update_realtime_metrics(self):
        workcenters = self.search([
            ('machine_settings_id', '!=', False),
            ('runtime_event_id', '!=', False),
            ('production_count_id', '!=', False)
        ])
        if not workcenters:
            return

        settings_model = self.env['mes.machine.settings']
        oee_results = settings_model.get_realtime_oee_batch(workcenters)

        for wc in workcenters:
            data = oee_results.get(wc.id, {})
            if not data or 'error' in data:
                continue

            vals = {}
            if wc.current_oee != data.get('oee', 0.0): vals['current_oee'] = data.get('oee', 0.0)
            if wc.current_availability != data.get('availability', 0.0): vals['current_availability'] = data.get('availability', 0.0)
            if wc.current_performance != data.get('performance', 0.0): vals['current_performance'] = data.get('performance', 0.0)
            if wc.current_quality != data.get('quality', 0.0): vals['current_quality'] = data.get('quality', 0.0)
            if wc.current_produced != data.get('total_produced', 0): vals['current_produced'] = data.get('total_produced', 0)
            if wc.current_waste_losses != data.get('waste_losses', 0.0): vals['current_waste_losses'] = data.get('waste_losses', 0.0)
            if wc.current_downtime_losses != data.get('downtime_losses', 0.0): vals['current_downtime_losses'] = data.get('downtime_losses', 0.0)
            if wc.current_first_running_time != data.get('first_running_time'): vals['current_first_running_time'] = data.get('first_running_time')
            if wc.current_runtime_formatted != data.get('runtime_formatted', '00:00:00'): vals['current_runtime_formatted'] = data.get('runtime_formatted', '00:00:00')
            if wc.current_produced_per_hour != data.get('produced_per_hour', 0.0): vals['current_produced_per_hour'] = data.get('produced_per_hour', 0.0)
            if wc.current_produced_per_planned_hour != data.get('produced_per_planned_hour', 0.0): vals['current_produced_per_planned_hour'] = data.get('produced_per_planned_hour', 0.0)
            if wc.current_top_rejection != data.get('top_rejection', 'None'): vals['current_top_rejection'] = data.get('top_rejection', 'None')
            if wc.current_top_alarm != data.get('top_alarm', 'None'): vals['current_top_alarm'] = data.get('top_alarm', 'None')
            if vals:
                wc.write(vals)

    def action_force_metrics_update(self):
        self.ensure_one()
        oee_results = self.env['mes.machine.settings'].sudo().get_realtime_oee_batch(self)
        
        data = oee_results.get(self.id, {})
        if not data or 'error' in data:
            return

        self.sudo().write({
            'current_oee': data.get('oee', 0.0),
            'current_availability': data.get('availability', 0.0),
            'current_performance': data.get('performance', 0.0),
            'current_quality': data.get('quality', 0.0),
            'current_produced': data.get('total_produced', 0),
            'current_waste_losses': data.get('waste_losses', 0.0),
            'current_downtime_losses': data.get('downtime_losses', 0.0),
            'current_first_running_time': data.get('first_running_time'),
            'current_runtime_formatted': data.get('runtime_formatted', '00:00:00'),
            'current_produced_per_hour': data.get('produced_per_hour', 0.0),
            'current_produced_per_planned_hour': data.get('produced_per_planned_hour', 0.0),
            'current_top_rejection': data.get('top_rejection', 'None'),
            'current_top_alarm': data.get('top_alarm', 'None')
        })

    def action_open_waste_losses(self):
        self.ensure_one()
        if not self.machine_settings_id:
            return
        return self.machine_settings_id.action_open_waste_losses()

    def action_open_downtime_losses(self):
        self.ensure_one()
        if not self.machine_settings_id:
            return
        return self.machine_settings_id.action_open_downtime_losses()
    
    @api.model
    def _apply_operator_ip_filter(self, domain):
        return domain

    @api.constrains('refresh_frequency')
    def _check_refresh_frequency(self):
        for wc in self:
            if wc.refresh_frequency < 10:
                raise ValidationError('Configuration error: Refresh frequency cannot be less than 10 seconds.')

    @api.model
    def _split_processes_by_type(self, mac):
        numeric, textual, undecided = [], [], {}
        for process in self.env['mes.process'].search([]):
            tag = process.get_tag_for_machine(mac)
            if not tag:
                continue
            signal = mac.process_tag_ids.filtered(lambda item: item.process_id == process)[:1]
            kind = signal.param_type if signal else 'auto'
            if kind == 'string':
                textual.append(process)
            elif kind == 'auto':
                undecided[process] = tag
            else:
                numeric.append(process)
        if undecided:
            with self.env['mes.timescale.base']._connection() as conn:
                with conn.cursor() as cur:
                    for process, tag in undecided.items():
                        cur.execute("""
                            SELECT value_str IS NOT NULL FROM telemetry_process
                            WHERE machine_name = %s AND tag_name = %s AND time > NOW() - INTERVAL '30 days'
                            ORDER BY time DESC LIMIT 1
                        """, (mac.name, tag))
                        row = cur.fetchone()
                        (textual if row and row[0] else numeric).append(process)
        return numeric, textual

    @api.model
    def _text_process_points(self, cur, mac, tag, s_loc, calc_e_loc, to_iso):
        cur.execute("""
            SELECT time, value_str FROM (
                (SELECT time, value_str FROM telemetry_process
                 WHERE machine_name = %s AND tag_name = %s AND time < %s AND value_str IS NOT NULL
                 ORDER BY time DESC LIMIT 1)
                UNION ALL
                (SELECT time, value_str FROM telemetry_process
                 WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time <= %s
                   AND value_str IS NOT NULL
                 ORDER BY time ASC)
            ) sub ORDER BY time ASC
        """, (mac.name, tag, s_loc, mac.name, tag, s_loc, calc_e_loc))
        points, previous = [], None
        for moment, text in cur.fetchall():
            label = (text or '').strip()
            if not label or label == previous:
                continue
            points.append({'x': to_iso(moment.replace(tzinfo=None)), 'label': label})
            previous = label
        return points

    @api.model
    def _build_chart_payload(self, wc, s_loc, calc_e_loc, s_utc, calc_e_utc, b_min, count_id=False, proc_id=False,
                             text_proc_id=False):
        mac = wc.machine_settings_id

        def to_iso(dt):
            if not dt: return None
            if isinstance(dt, str): return dt
            return dt.replace(tzinfo=None).strftime('%Y-%m-%dT%H:%M:%S')

        available_counts = []
        seen_c_ids = set()
        for ct in mac.count_tag_ids:
            if ct.count_id and ct.count_id.id not in seen_c_ids:
                available_counts.append({'id': ct.count_id.id, 'name': ct.count_id.name})
                seen_c_ids.add(ct.count_id.id)

        tgt_c = self.env['mes.counts'].browse(int(count_id)) if count_id else wc.production_count_id
        is_ideal_shown = bool(wc.production_count_id and tgt_c and tgt_c.id == wc.production_count_id.id)

        counts_to_fetch = tgt_c | tgt_c.related_count_ids if tgt_c else tgt_c
        c_tags_by_count = {}
        for ct in mac.count_tag_ids:
            if ct.count_id and ct.count_id in counts_to_fetch:
                c_tags_by_count.setdefault(ct.count_id, []).append(ct)
        c_sigs = mac.count_tag_ids.filtered(lambda t: t.count_id == tgt_c)
        c_tags = list(set(c_sigs.mapped('tag_name')))
        is_cum_map = {s.tag_name: s.is_cumulative for s in mac.count_tag_ids.filtered(lambda t: t.count_id in counts_to_fetch)}

        related_counts_info = []
        if tgt_c:
            for rc in tgt_c.related_count_ids:
                rc_sigs = mac.count_tag_ids.filtered(lambda t: t.count_id == rc)
                if rc_sigs:
                    related_counts_info.append({'id': rc.id, 'name': rc.complete_name or rc.name})

        numeric_procs, text_procs = self._split_processes_by_type(mac)
        available_procs = [{'id': p.id, 'name': p.complete_name or p.name} for p in numeric_procs]
        available_text_procs = [{'id': p.id, 'name': p.complete_name or p.name} for p in text_procs]
        tgt_text = self.env['mes.process'].browse(int(text_proc_id)) if text_proc_id else self.env['mes.process']
        if tgt_text and tgt_text not in text_procs:
            tgt_text = self.env['mes.process']

        tgt_p = self.env['mes.process'].browse(int(proc_id)) if proc_id else self.env['mes.process']
        if tgt_p and tgt_p not in numeric_procs:
            tgt_p = self.env['mes.process']
        p_to_fetch = (tgt_p | tgt_p.related_process_ids).filtered(lambda process: process in numeric_procs)

        b_sec = b_min * 60
        tot_s = (calc_e_loc - s_loc).total_seconds()
        n_ints = math.ceil(tot_s / b_sec) if tot_s > 0 else 1

        labels = []
        prod_data = []
        ideal_data = []
        b_idx_map = {}
        ideal_per_b = (wc.ideal_capacity_per_min or 0.0) * b_min

        for i in range(n_ints + 1):
            b_dt = s_loc + timedelta(seconds=i * b_sec)
            labels.append(to_iso(b_dt))
            prod_data.append(0.0)
            ideal_data.append(ideal_per_b if i > 0 else 0.0)
            b_idx_map[b_dt.strftime('%Y-%m-%dT%H:%M')] = i

        tl_data = mac._fetch_timeline_raw(s_utc, calc_e_utc, wc.id)
        
        all_p_data = []
        main_p_data = []

        with self.env['mes.timescale.base']._connection() as conn:
            with conn.cursor() as cur:
                if c_tags:
                    raw_prod = mac._fetch_production_chart_raw(cur, c_tags, s_loc, calc_e_loc, b_min)
                    for row in raw_prod:
                        t_name = row[0]
                        b_time = row[1].replace(tzinfo=None)
                        qty = float(row[3]) if is_cum_map.get(t_name) else float(row[2])
                        key = b_time.strftime('%Y-%m-%dT%H:%M')
                        if key in b_idx_map:
                            idx = b_idx_map[key] + 1
                            if idx < len(prod_data): prod_data[idx] += qty

                related_counts_chart = []
                for rc in (tgt_c.related_count_ids if tgt_c else self.env['mes.counts']):
                    rc_sigs = mac.count_tag_ids.filtered(lambda t: t.count_id == rc)
                    rc_tags = list(set(rc_sigs.mapped('tag_name')))
                    if not rc_tags:
                        continue
                    rc_prod = [0.0] * len(prod_data)
                    raw_rc = mac._fetch_production_chart_raw(cur, rc_tags, s_loc, calc_e_loc, b_min)
                    for row in raw_rc:
                        t_name = row[0]
                        b_time = row[1].replace(tzinfo=None)
                        qty = float(row[3]) if is_cum_map.get(t_name) else float(row[2])
                        key = b_time.strftime('%Y-%m-%dT%H:%M')
                        if key in b_idx_map:
                            idx = b_idx_map[key] + 1
                            if idx < len(rc_prod): rc_prod[idx] += qty
                    related_counts_chart.append({
                        'id': rc.id,
                        'name': rc.complete_name or rc.name,
                        'data': rc_prod,
                    })

                for p in p_to_fetch:
                    p_tag = p.get_tag_for_machine(mac)
                    if not p_tag: continue
                    cur.execute("""
                        SELECT time, value FROM (
                            (SELECT time, value FROM telemetry_process
                            WHERE machine_name = %s AND tag_name = %s AND time < %s
                            ORDER BY time DESC LIMIT 1)
                            UNION ALL
                            (SELECT time, value FROM telemetry_process
                            WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time <= %s
                            ORDER BY time ASC)
                        ) sub ORDER BY time ASC
                    """, (mac.name, p_tag, s_loc, mac.name, p_tag, s_loc, calc_e_loc))
                    
                    p_series = []
                    for row in cur.fetchall():
                        if row[1] is None:
                            continue
                        p_series.append({'x': to_iso(row[0].replace(tzinfo=None)), 'y': float(row[1])})
                    if tgt_p and p.id == tgt_p.id: main_p_data = p_series
                    all_p_data.append({'name': p.complete_name or p.name, 'data': p_series})

                text_process = False
                if tgt_text:
                    text_tag = tgt_text.get_tag_for_machine(mac)
                    if text_tag:
                        text_process = {
                            'name': tgt_text.complete_name or tgt_text.name,
                            'points': self._text_process_points(cur, mac, text_tag, s_loc, calc_e_loc, to_iso),
                        }

        return {
            'timeline': tl_data,
            'chart_duration_sec': n_ints * b_sec,
            'shift_start': labels[0],
            'available_counts': available_counts,
            'selected_count_id': tgt_c.id if tgt_c else False,
            'selected_count_name': tgt_c.name if tgt_c else 'Data',
            'related_counts': related_counts_info,
            'available_processes': available_procs,
            'selected_process_id': tgt_p.id if tgt_p else False,
            'selected_process_name': tgt_p.complete_name if tgt_p else '',
            'available_text_processes': available_text_procs,
            'selected_text_process_id': tgt_text.id if tgt_text else False,
            'chart': {
                'labels': labels,
                'production': prod_data,
                'ideal': ideal_data,
                'process': main_p_data,
                'processes': all_p_data,
                'text_process': text_process,
                'related_counts': related_counts_chart,
                'bucket_sec': b_sec,
                'show_ideal': is_ideal_shown
            }
        }

    MAX_CHART_BUCKETS = 240

    @api.model
    def _align_to_bucket(self, moment, bucket_minutes, round_up=False):
        bucket_seconds = max(1, bucket_minutes) * 60
        epoch = datetime(1970, 1, 1)
        elapsed = int((moment - epoch).total_seconds())
        aligned = (elapsed // bucket_seconds) * bucket_seconds
        if round_up and aligned != elapsed:
            aligned += bucket_seconds
        return epoch + timedelta(seconds=aligned)

    @api.model
    def _fit_bucket_to_period(self, period_hours, bucket_minutes):
        minimum_bucket = math.ceil((period_hours * 60) / self.MAX_CHART_BUCKETS)
        if bucket_minutes >= minimum_bucket:
            return bucket_minutes
        adjusted = max(5, int(math.ceil(minimum_bucket / 5.0) * 5))
        _logger.info("Custom chart bucket raised from %s to %s minutes for a %sh window",
                     bucket_minutes, adjusted, period_hours)
        return adjusted

    @api.model
    def get_custom_chart_data(self, workcenter_id, period_hours=2, bucket_minutes=5,
                              selected_count_id=False, selected_process_id=False,
                              selected_text_process_id=False):
        try:
            wc_id = workcenter_id[0] if isinstance(workcenter_id, (list, tuple)) else workcenter_id
            wc = self.sudo().browse(wc_id)
            if not wc_id or not wc.exists():
                return {'error': 'Work center not found'}
            if not wc.machine_settings_id:
                return {'error': 'Telemetry Settings are not linked to this work center'}

            period_hours = min(24, max(2, int(period_hours or 2)))
            bucket_minutes = min(120, max(5, int(bucket_minutes or 5)))
            bucket_minutes = self._fit_bucket_to_period(period_hours, bucket_minutes)

            now_utc = fields.Datetime.now()
            mac_tz = pytz.timezone(wc.company_id.tz or 'UTC')
            now_loc = pytz.utc.localize(now_utc).astimezone(mac_tz).replace(tzinfo=None)
            end_loc = self._align_to_bucket(now_loc, bucket_minutes, round_up=True)
            start_loc = self._align_to_bucket(end_loc - timedelta(hours=period_hours), bucket_minutes)
            start_utc = mac_tz.localize(start_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
            end_utc = mac_tz.localize(end_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)

            started_at = time_module.monotonic()
            payload = self.sudo()._build_chart_payload(
                wc, start_loc, end_loc, start_utc, end_utc, bucket_minutes,
                selected_count_id, selected_process_id, selected_text_process_id)
            _logger.info("Custom chart built for wc=%s period=%sh bucket=%smin in %.2fs",
                         wc.id, period_hours, bucket_minutes, time_module.monotonic() - started_at)
            payload['period_hours'] = period_hours
            payload['bucket_minutes'] = bucket_minutes
            payload['window_start'] = start_loc.strftime('%Y-%m-%d %H:%M')
            payload['window_end'] = end_loc.strftime('%Y-%m-%d %H:%M')
            return payload
        except Exception:
            _logger.exception("get_custom_chart_data failed wc=%s", workcenter_id)
            return {'error': 'Custom chart data unavailable'}

    @api.model
    def get_live_chart_data(self, workcenter_id, selected_count_id=False, selected_process_id=False,
                            selected_text_process_id=False):
        try:
            wc_id = workcenter_id[0] if isinstance(workcenter_id, (list, tuple)) else workcenter_id
            wc = self.sudo().browse(wc_id)
            if not wc_id or not wc.exists():
                _logger.warning("get_live_chart_data: workcenter %s not found", workcenter_id)
                return {'error': 'Work center not found'}
            if not wc.machine_settings_id:
                _logger.warning("get_live_chart_data: workcenter %s (%s) has no telemetry settings linked",
                                wc.id, wc.name)
                return {'error': 'Telemetry Settings are not linked to this work center'}

            s_loc, e_loc = self.env['mes.shift'].sudo().get_current_shift_window(wc)
            if not s_loc:
                return {'error': 'No active shift'}

            now_utc = fields.Datetime.now()
            mac_tz = pytz.timezone(wc.company_id.tz or 'UTC')
            now_loc = pytz.utc.localize(now_utc).astimezone(mac_tz).replace(tzinfo=None)
            calc_e_loc = min(now_loc, e_loc)
            s_utc = mac_tz.localize(s_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
            calc_e_utc = mac_tz.localize(calc_e_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
            b_min = max(1, wc.chart_bucket_minutes)

            return self.sudo()._build_chart_payload(wc, s_loc, calc_e_loc, s_utc, calc_e_utc, b_min, selected_count_id,
                                                    selected_process_id, selected_text_process_id)
        except Exception as e:
            _logger.exception("get_live_chart_data failed wc=%s count=%s proc=%s", workcenter_id, selected_count_id, selected_process_id)
            return {'error': f'{type(e).__name__}: {e}'}

    def _process_timeline_colors(self, machine, raw_timeline, state_configs):
        name_map = {}
        color_map = {}
        
        for ev in machine.event_tag_ids:
            key = (ev.tag_name, ev.plc_value)
            if key in name_map:
                name_map[key] += " / " + ev.event_id.name
            else:
                name_map[key] = ev.event_id.name
                color_map[key] = ev.event_id.color or '#808080'

        global_evts = self.env['mes.event'].search([])
        fallback_name_map = {ge.default_plc_value: ge.name for ge in global_evts}
        fallback_color_map = {ge.default_plc_value: (ge.color or '#808080') for ge in global_evts}

        result = []
        for row in raw_timeline:
            val = int(row[2]) if row[2] is not None else 0
            t_name = row[3]
            key = (t_name, val)
            
            evt_name = name_map.get(key) or fallback_name_map.get(val)
            evt_color = color_map.get(key) or fallback_color_map.get(val)
            
            if not evt_name:
                is_running = any(c['tag'] == t_name and c['val'] == val for c in state_configs)
                is_state_tag = any(c['tag'] == t_name for c in state_configs)
                
                if is_running:
                    evt_name, evt_color = 'Running', '#28a745'
                elif val == 0:
                    evt_name, evt_color = 'Ready / Cleared', '#cccccc'
                else:
                    evt_name = f'Code {val}'
                    evt_color = '#dc3545' if is_state_tag else '#ffc107'

            result.append({
                'start': row[0].strftime('%Y-%m-%dT%H:%M:%S'),
                'end': row[1].strftime('%Y-%m-%dT%H:%M:%S'),
                'duration': (row[1] - row[0]).total_seconds(),
                'color': evt_color,
                'name': evt_name
            })
        return result

class MesHistDashboardWiz(models.TransientModel):
    _name = 'mes.hist.dashboard.wiz'
    _description = 'Historical Chart Wizard'

    wc_id = fields.Many2one('mrp.workcenter', string='Machine', required=True)
    s_time = fields.Datetime(string='Start Time', required=True)
    e_time = fields.Datetime(string='End Time', required=True)
    b_min = fields.Integer(string='Bucket (Min)', default=15, required=True)
    
    count_id = fields.Many2one('mes.counts', string='Production Count')
    proc_id = fields.Many2one('mes.process', string='Process Variable')
    text_proc_id = fields.Many2one('mes.process', string='Text Process')

    available_count_ids = fields.Many2many('mes.counts', compute='_compute_available_tags')
    available_proc_ids = fields.Many2many('mes.process', compute='_compute_available_tags')
    available_text_proc_ids = fields.Many2many('mes.process', compute='_compute_available_tags')

    @api.depends('wc_id')
    def _compute_available_tags(self):
        for wiz in self:
            if wiz.wc_id and wiz.wc_id.machine_settings_id:
                mac = wiz.wc_id.machine_settings_id
                c_ids = mac.count_tag_ids.mapped('count_id').ids
                wiz.available_count_ids = [(6, 0, c_ids)]
                
                numeric, textual = self.env['mrp.workcenter']._split_processes_by_type(mac)
                wiz.available_proc_ids = [(6, 0, [process.id for process in numeric])]
                wiz.available_text_proc_ids = [(6, 0, [process.id for process in textual])]
            else:
                wiz.available_count_ids = [(6, 0, [])]
                wiz.available_proc_ids = [(6, 0, [])]
                wiz.available_text_proc_ids = [(6, 0, [])]

    def action_update_chart(self):
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'mes.hist.dashboard.wiz',
            'res_id': self.id,
            'view_mode': 'form',
            'target': 'current',
        }

    @api.model
    def get_chart_data(self, wiz_id):
        r_id = wiz_id[0] if isinstance(wiz_id, list) else wiz_id
        wiz = self.browse(r_id)
        
        if not wiz.exists() or not wiz.wc_id or not wiz.s_time or not wiz.e_time:
            return {'error': 'Please provide Machine, Start Time, and End Time.'}

        tz_name = wiz.wc_id.company_id.tz or 'Europe/Dublin'
        local_tz = pytz.timezone(tz_name)

        s_utc = pytz.utc.localize(wiz.s_time)
        e_utc = pytz.utc.localize(wiz.e_time)

        s_time_wall = s_utc.astimezone(local_tz).replace(tzinfo=None)
        e_time_wall = e_utc.astimezone(local_tz).replace(tzinfo=None)

        res = self.env['mrp.workcenter']._build_chart_payload(
            wc=wiz.wc_id, 
            s_loc=s_time_wall, 
            calc_e_loc=e_time_wall, 
            s_utc=wiz.s_time, 
            calc_e_utc=wiz.e_time, 
            b_min=max(1, wiz.b_min), 
            count_id=wiz.count_id.id if wiz.count_id else False, 
            proc_id=wiz.proc_id.id if wiz.proc_id else False,
            text_proc_id=wiz.text_proc_id.id if wiz.text_proc_id else False
        )
        
        res['shift_start'] = s_time_wall.strftime('%Y-%m-%d %H:%M:%S')
        
        return res

class MesStreams(models.Model):
    _name = 'mes.stream'
    _description = 'Machine Stream'
    _order = 'sequence, name'

    name = fields.Char(string='Stream Name', required=True)
    code = fields.Char(string='Stream ID')
    sequence = fields.Integer(string='Sequence', default=10)
    active = fields.Boolean(string='Active', default=True)
    image = fields.Binary(string='Button Image', attachment=True)
    button_width = fields.Integer(string='Button Width (px)', default=120)
    button_height = fields.Integer(string='Button Height (px)', default=120)
    default_pos_x = fields.Float(string='Default X (%)', default=0.0)
    default_pos_y = fields.Float(string='Default Y (%)', default=0.0)
    workcenter_ids = fields.Many2many(
        'mrp.workcenter',
        'mes_stream_workcenter_rel', 'stream_id', 'workcenter_id',
        string='Machines',
    )
    layout_ids = fields.One2many('mes.stream.layout', 'stream_id', string='Machine Layouts')
    count_ids = fields.One2many('mes.counts', 'stream_id', string='Counts')
    event_ids = fields.One2many('mes.event', 'stream_id', string='Events')
    process_ids = fields.One2many('mes.process', 'stream_id', string='Processes')
    manual_stop_event_ids = fields.One2many(
        'mes.event', 'stream_id',
        string='Manual Stop Events',
        domain=[('is_manual_stop', '=', True)],
    )

    _sql_constraints = [
        ('mes_stream_code_unique', 'unique(code)', 'Stream ID must be unique.'),
    ]

    @api.depends('name', 'code')
    def _compute_display_name(self):
        for rec in self:
            rec.display_name = f"[{rec.code}] {rec.name}" if rec.code else rec.name

class MesStreamLayout(models.Model):
    _name = 'mes.stream.layout'
    _description = 'Machine Stream Button Layout'
    _order = 'workcenter_id, stream_id'

    workcenter_id = fields.Many2one('mrp.workcenter', string='Machine', required=True, ondelete='cascade', index=True)
    stream_id = fields.Many2one('mes.stream', string='Stream', required=True, ondelete='cascade', index=True)
    pos_x = fields.Float(string='X (%)', default=0.0)
    pos_y = fields.Float(string='Y (%)', default=0.0)

    _sql_constraints = [
        ('mes_stream_layout_unique', 'unique(workcenter_id, stream_id)',
         'A stream can only be positioned once per machine.'),
    ]

    @api.constrains('pos_x', 'pos_y')
    def _check_relative_position(self):
        for rec in self:
            if not (0.0 <= rec.pos_x <= 100.0) or not (0.0 <= rec.pos_y <= 100.0):
                raise ValidationError('Stream position must be expressed as a percentage between 0 and 100.')

class MesPart(models.Model):
    _name = 'mes.part'
    _description = 'Machine Part'
    _order = 'name'

    name = fields.Char(string='Part', required=True)
    code = fields.Char(string='Code')
    active = fields.Boolean(string='Active', default=True)
    stream_id = fields.Many2one('mes.stream', string='Parent Stream')
    wheel_number = fields.Integer(string='Wheel Number')
    modules_amount = fields.Integer(string='Number of Modules')
    piece_ids = fields.One2many('mes.piece', 'part_id', string='Pieces')
    piece_count = fields.Integer(string='Pieces', compute='_compute_piece_count')

    _sql_constraints = [
        ('name_uniq', 'unique(name)', 'A part with this name already exists.'),
    ]

    @api.depends('piece_ids')
    def _compute_piece_count(self):
        for part in self:
            part.piece_count = len(part.piece_ids)

    stream_removal_policy = fields.Selection([
        ('keep', 'Keep the previous stream on catalogue records'),
        ('clear', 'Remove the stream from catalogue records'),
    ], string='When Clearing The Stream', copy=False)
    stream_removal_pending = fields.Boolean(
        string='Stream Removal Needs A Decision', compute='_compute_stream_removal_pending')

    @api.depends('stream_id')
    def _compute_stream_removal_pending(self):
        for part in self:
            part.stream_removal_pending = bool(
                not part.stream_id and part._linked_records_with_stream())

    def _linked_records_with_stream(self):
        self.ensure_one()
        if not self.id:
            return []
        found = []
        for model_name in ('mes.counts', 'mes.event', 'mes.process'):
            found += self.env[model_name].sudo().search([
                ('part_id', '=', self.id),
                ('stream_id', '!=', False),
            ])
        return found

    @api.onchange('stream_id')
    def _onchange_stream_removal(self):
        for part in self:
            if part.stream_id:
                part.stream_removal_policy = False
                continue
            linked = part._linked_records_with_stream()
            if not linked:
                part.stream_removal_policy = False
                continue
            return {'warning': {
                'title': _("Stream removed from the part"),
                'message': _(
                    "You removed the stream from part '%(part)s'.\n\n"
                    "%(count)s catalogue record(s) use this part and have a stream of "
                    "their own. Choose below what should happen to them, then save. "
                    "Discard the form to put the stream back.",
                    part=part.name or '-', count=len(linked)),
            }}

    def write(self, vals):
        if 'stream_id' not in vals:
            return super().write(vals)

        clearing = self.filtered(
            lambda p: p.stream_id and not vals['stream_id'] and p._linked_records_with_stream())
        policy = vals.get('stream_removal_policy') or self[:1].stream_removal_policy
        if clearing and not policy:
            raise UserError(_(
                "You removed the stream from part '%(part)s', but catalogue records "
                "using this part still carry a stream.\n\n"
                "Set 'When Clearing The Stream' to either keep their current stream "
                "or remove it, then save again. Discarding the form restores the "
                "previous stream.",
                part=clearing[:1].name or '-'))

        moving = self.filtered(lambda p: p.stream_id.id != vals['stream_id'])
        result = super().write(vals)

        if clearing and policy == 'clear':
            clearing._clear_stream_on_linked_records()
        if moving:
            moving._cascade_stream_to_linked_records()
        if 'stream_removal_policy' not in vals:
            super(MesPart, self).write({'stream_removal_policy': False})
        return result

    def _clear_stream_on_linked_records(self):
        for part in self:
            for model_name in ('mes.counts', 'mes.event', 'mes.process'):
                linked = self.env[model_name].sudo().search([
                    ('part_id', '=', part.id),
                    ('stream_id', '!=', False),
                ])
                if linked:
                    linked.write({'stream_id': False})
                    _logger.info("Part %s lost its stream: cleared stream on %s %s record(s)",
                                 part.name, len(linked), model_name)

    def _cascade_stream_to_linked_records(self):
        for part in self:
            if not part.stream_id:
                continue
            for model_name in ('mes.counts', 'mes.event', 'mes.process'):
                linked = self.env[model_name].sudo().search([('part_id', 'in', part.ids)])
                stale = linked.filtered(lambda r: r.stream_id != part.stream_id)
                if stale:
                    stale.write({'stream_id': part.stream_id.id})
                    _logger.info("Part %s moved to stream %s: realigned %s %s record(s)",
                                 part.name, part.stream_id.name,
                                 len(stale), model_name)


class MesPiece(models.Model):
    _name = 'mes.piece'
    _description = 'Part Piece'
    _order = 'part_id, sequence, name'

    name = fields.Char(string='Piece', required=True)
    code = fields.Char(string='Code')
    sequence = fields.Integer(string='Sequence', default=10)
    active = fields.Boolean(string='Active', default=True)
    part_id = fields.Many2one('mes.part', string='Part', required=True,
                              ondelete='cascade', index=True)


class MesWorkcenterPart(models.Model):
    _name = 'mes.workcenter.part'
    _description = 'Part Fitted To Work Center'
    _order = 'workcenter_id, part_id'

    workcenter_id = fields.Many2one('mrp.workcenter', string='Work Center',
                                    required=True, ondelete='cascade', index=True)
    part_id = fields.Many2one('mes.part', string='Part', required=True,
                              ondelete='restrict')
    maintainx_id = fields.Char(string='MaintainX Asset ID', required=True,
                               help="Asset ID of this part in MaintainX. Work orders "
                                    "raised against this part are attached to it "
                                    "instead of the machine.")
    note = fields.Char(string='Note')
    active = fields.Boolean(string='Active', default=True)
    piece_ids = fields.One2many(related='part_id.piece_ids', string='Pieces')

    _sql_constraints = [
        ('workcenter_part_uniq', 'unique(workcenter_id, part_id)',
         'This part is already listed on the work center.'),
        ('maintainx_id_uniq', 'unique(maintainx_id)',
         'This MaintainX asset ID is already used by another part.'),
    ]

    def name_get(self):
        return [(rec.id, f"{rec.part_id.name} ({rec.workcenter_id.name})") for rec in self]

class MesEmployee(models.Model):
    _inherit = 'hr.employee'

    maintainx_id = fields.Char(string='MaintainX ID')
    maintainx_api_token = fields.Char(
        string='MaintainX API Token',
        groups='hr.group_hr_user',
        help="Personal REST token of this employee. When set, work order comments and "
             "status changes triggered by this employee are sent under their own "
             "MaintainX account instead of the company service account.")

class ResCompany(models.Model):
    _inherit = 'res.company'
    
    tz = fields.Selection(related='partner_id.tz', readonly=False, string='Timezone', required=True)
