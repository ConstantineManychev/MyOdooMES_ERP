import requests
import logging
from datetime import datetime
from odoo import models, fields, api
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

class MesTaskStatusHistory(models.Model):
    _name = 'mes.task.status.history'
    _description = 'Task Status History'
    _order = 'change_date desc'

    task_id = fields.Many2one('mes.task', string='Task', ondelete='cascade')
    status = fields.Char(string='Status')
    change_date = fields.Datetime(string='Date', default=fields.Datetime.now)
    source = fields.Selection([('odoo', 'Odoo'), ('maintainx', 'MaintainX')], string='Source', default='maintainx')

class MesTask(models.Model):
    _name = 'mes.task'
    _description = 'MES Task'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Task Title', required=True)
    description = fields.Html(string='Description')
    
    # === MaintainX Data ===
    maintainx_id = fields.Char(string="MaintainX ID", readonly=True, copy=False, index=True)
    maintainx_created_at = fields.Datetime(string="MX Created At", readonly=True)
    
    # Assignees
    maintainx_current_assignee = fields.Char(string="Current Assignee (MX)")
    maintainx_assignees_history = fields.Text(string="Assignees History (MX)", readonly=True)

    # === Odoo Fields ===
    author_id = fields.Many2one('res.users', string='Author', default=lambda self: self.env.user, readonly=True)
    user_id = fields.Many2one('res.users', string='Assigned To (Odoo)', tracking=True)
    
    machine_id = fields.Many2one('mrp.workcenter', string='Machine', tracking=True)
    
    # Update State
    state = fields.Selection([
        ('new', 'Open'),
        ('on_hold', 'On Hold'),
        ('assigned', 'In Progress'),
        ('done', 'Done'),
        ('cancel', 'Cancelled')
    ], string='Status', default='new', tracking=True)

    # Priority
    priority = fields.Selection([
        ('low', 'Low'),
        ('medium', 'Medium'),
        ('high', 'High')
    ], string='Priority', default='low', tracking=True)

    # Status History Table
    status_history_ids = fields.One2many('mes.task.status.history', 'task_id', string="Status History")

    # ---------------------------------------------------------
    # API CONFIG
    # ---------------------------------------------------------
    def _get_maintainx_config(self):
        params = self.env['ir.config_parameter'].sudo()
        token = params.get_param('gemba.maintainx_token')
        if not token:
            raise UserError("MaintainX API Token is not configured!")
        
        base_url = "https://api.getmaintainx.com/v1"
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        return base_url, headers

    # ---------------------------------------------------------
    # SYNC LOGIC
    # ---------------------------------------------------------
    @api.model
    def sync_tasks_from_maintainx(self):
        _logger.info(">>> STARTING MaintainX Sync (Advanced)...")
        
        try:
            base_url, headers = self._get_maintainx_config()
        except UserError:
            return

        endpoint = f"{base_url}/workorders"
        params = {'limit': 50}

        try:
            response = requests.get(endpoint, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            workorders = data.get('workOrders') or data.get('items') or []
            
            _logger.info(f"Processing {len(workorders)} work orders...")

            for wo in workorders:
                self._process_single_wo(wo)

        except Exception as e:
            _logger.error(f"Sync Error: {e}")

    def _process_single_wo(self, wo):
        # 1. Data Extraction
        wo_id = str(wo.get('id'))
        title = wo.get('title', 'No Title')
        desc = wo.get('description', '') or ''
        
        # Status
        mx_status = str(wo.get('status', '')).upper() # OPEN, DONE, IN_PROGRESS, ON_HOLD
        odoo_state = self._map_status(mx_status)
        
        # Created At
        created_at_str = wo.get('createdAt')
        created_at = None
        if created_at_str:
            try:
                created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            except:
                pass

        # Updated At
        updated_at_str = wo.get('updatedAt')

        # Priority
        mx_priority = str(wo.get('priority', 'LOW')).upper()
        odoo_priority = self._map_priority(mx_priority)

        # MAchine (Asset)
        asset_data = wo.get('asset') or {}
        machine_name = asset_data.get('name')
        machine_id = False
        if machine_name:
            machine = self.env['mrp.workcenter'].search([('name', '=', machine_name)], limit=1)
            if machine:
                machine_id = machine.id

        # Assignees
        assignees_data = wo.get('assignees', [])
        current_assignees_list = [str(a.get('id', 'Unknown')) for a in assignees_data if a.get('type') == 'USER']
        current_assignees_str = ", ".join(current_assignees_list)

        # 2. Search or Create/Update Task
        task = self.search([('maintainx_id', '=', wo_id)], limit=1)

        vals = {
            'priority': odoo_priority,
            'maintainx_current_assignee': current_assignees_str,
        }
        
        # if machine found, link it
        if machine_id:
            vals['machine_id'] = machine_id

        if not task:
            # === Create ===
            _logger.info(f"Creating Task {wo_id}")
            vals.update({
                'name': title,
                'description': desc,
                'maintainx_id': wo_id,
                'maintainx_created_at': created_at,
                'state': odoo_state,
                'maintainx_assignees_history': f"{fields.Datetime.now()}: {current_assignees_str}\n"
            })
            new_task = self.create(vals)
            # Record initial status in history
            self.env['mes.task.status.history'].create({
                'task_id': new_task.id,
                'status': mx_status,
                'change_date': created_at or fields.Datetime.now()
            })

        else:
            # === Update ===
            
            # If status changed, update and log history
            if task.state != odoo_state:
                _logger.info(f"Updating status for {wo_id}: {task.state} -> {odoo_state}")
                vals['state'] = odoo_state
                
                # Add to status history
                self.env['mes.task.status.history'].create({
                    'task_id': task.id,
                    'status': mx_status,
                    'change_date': datetime.now()
                })

            # Update description and title if changed
            if task.maintainx_current_assignee != current_assignees_str:
                history_line = f"{fields.Datetime.now()}: {current_assignees_str}\n"
                vals['maintainx_assignees_history'] = (task.maintainx_assignees_history or "") + history_line

            task.write(vals)

    def _map_status(self, status):
        """Map MaintainX status to Odoo state"""
        if status == 'OPEN':
            return 'new'
        elif status == 'ON_HOLD':
            return 'on_hold'
        elif status == 'IN_PROGRESS':
            return 'assigned'
        elif status == 'DONE':
            return 'done'
        return 'new'

    def _map_priority(self, priority):
        """Map MaintainX priority to Odoo"""
        if priority == 'HIGH':
            return 'high'
        elif priority == 'MEDIUM':
            return 'medium'
        return 'low'

    # Sending logic to MaintainX (not implemented)
    def action_send_to_maintainx(self):
        self.ensure_one()
        # ... TODO: Sending logic here ...
        pass