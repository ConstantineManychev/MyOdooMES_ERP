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
    
    maintainx_id = fields.Char(string="MaintainX ID", readonly=True, copy=False, index=True)
    maintainx_created_at = fields.Datetime(string="MX Created At", readonly=True)
    maintainx_assignees_history = fields.Text(string="Assignees History (MX)", readonly=True)

    author_id = fields.Many2one('res.users', string='Author', default=lambda self: self.env.user, readonly=True)
    assigned_id = fields.Many2one('hr.employee', string='Assigned To', tracking=True)
    
    machine_id = fields.Many2one('mrp.workcenter', string='Machine', tracking=True)
    
    state = fields.Selection([
        ('new', 'Open'),
        ('on_hold', 'On Hold'),
        ('assigned', 'In Progress'),
        ('done', 'Done'),
        ('cancel', 'Cancelled')
    ], string='Status', default='new', tracking=True)

    priority = fields.Selection([
        ('low', 'Low'),
        ('medium', 'Medium'),
        ('high', 'High')
    ], string='Priority', default='low', tracking=True)

    parent_id = fields.Many2one('mes.task', string="Parent Task", ondelete='cascade', index=True)
    child_ids = fields.One2many('mes.task', 'parent_id', string="Subtasks")

    def action_open_task(self):
        self.ensure_one()
        return {
            'type': 'ir.actions.act_window',
            'res_model': 'mes.task',
            'view_mode': 'form',
            'res_id': self.id,
            'target': 'current',
        }

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

    @api.model
    def load_tasks_from_maintainx(self):
        _logger.info(">>> STARTING MaintainX Sync (Advanced)...")
        
        try:
            base_url, headers = self._get_maintainx_config()
        except UserError:
            return

        endpoint = f"{base_url}/workorders"
        params = {'limit': 200}

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
        wo_id_raw = wo.get('id')
        wo_id = str(wo_id_raw).strip() if wo_id_raw is not None else ''
        title = wo.get('title', 'No Title')
        desc = wo.get('description', '') or ''
        
        mx_status = str(wo.get('status', '')).upper() # OPEN, DONE, IN_PROGRESS, ON_HOLD
        odoo_state = self._map_status(mx_status)
        
        created_at_str = wo.get('createdAt')
        created_at = None
        if created_at_str:
            try:
                created_at = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
            except:
                pass

        updated_at_str = wo.get('updatedAt')

        mx_priority = str(wo.get('priority', 'LOW')).upper()
        odoo_priority = self._map_priority(mx_priority)

        asset_data = wo.get('asset') or {}
        machine_name = asset_data.get('name')
        machine_id = False
        if machine_name:
            machine = self.env['mrp.workcenter'].search([('name', '=', machine_name)], limit=1)
            if machine:
                machine_id = machine.id

        assignees_data = wo.get('assignees', [])
        current_assignees_list = []
        employee_id = False 

        for a in assignees_data:
            if a.get('type') == 'USER':
                mx_user_id = str(a.get('id', 'Unknown'))
                current_assignees_list.append(mx_user_id)
                
                if not employee_id and mx_user_id != 'Unknown':
                    found_employee = self.env['hr.employee'].search([('maintainx_id', '=', mx_user_id)], limit=1)
                    if found_employee:
                        employee_id = found_employee.id

        current_assignees_str = ", ".join(current_assignees_list)

        task = self.search([('maintainx_id', '=', wo_id)], limit=1) if wo_id else self.search([('maintainx_id', '=', False)], limit=1)
        if not task and wo_id:
            task = self.search([('maintainx_id', 'ilike', wo_id)], limit=1)

        if task:
            _logger.debug(f"Found existing task for MaintainX id={wo_id}: {task.id}")
        else:
            _logger.debug(f"No existing task for MaintainX id={wo_id}; will create new.")

        vals = {
            'priority': odoo_priority
        }

        if employee_id:
            vals['assigned_id'] = employee_id
        
        if machine_id:
            vals['machine_id'] = machine_id

        if not task:
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

            self.env['mes.task.status.history'].create({
                'task_id': new_task.id,
                'status': mx_status,
                'change_date': created_at or fields.Datetime.now()
            })

        else:
            if task.state != odoo_state:
                _logger.info(f"Updating status for {wo_id}: {task.state} -> {odoo_state}")
                vals['state'] = odoo_state
                
                self.env['mes.task.status.history'].create({
                    'task_id': task.id,
                    'status': mx_status,
                    'change_date': datetime.now()
                })

            if task.assigned_id != employee_id:
                history_line = f"{fields.Datetime.now()}: {current_assignees_str}\n"
                vals['maintainx_assignees_history'] = (task.maintainx_assignees_history or "") + history_line

            task.write(vals)

    def _map_status(self, status):
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
        if priority == 'HIGH':
            return 'high'
        elif priority == 'MEDIUM':
            return 'medium'
        return 'low'

    def action_send_to_maintainx(self):
        self.ensure_one()
        # ... TODO: Sending logic here ...
        pass