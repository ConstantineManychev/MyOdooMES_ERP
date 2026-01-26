import requests
import logging
from datetime import datetime
from odoo import models, fields, api
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

_STATUS_MAPPING = {
    'OPEN': 'new',
    'IN_PROGRESS': 'assigned',
    'ON_HOLD': 'on_hold',
    'DONE': 'done',
    'CANCELLED': 'cancel',
    'COMPLETED': 'done',
}

_PRIORITY_MAPPING = {
    'HIGH': '2',
    'MEDIUM': '1',
    'LOW': '0',
}

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

    status_history_ids = fields.One2many('mes.task.status.history', 'task_id', string="Status History")
    author_id = fields.Many2one('res.users', string='Author', default=lambda self: self.env.user, readonly=True)
    assigned_id = fields.Many2one('hr.employee', string='Assigned To', tracking=True)
    
    machine_id = fields.Many2one('mrp.workcenter', string='Machine', tracking=True)
    
    state = fields.Selection([
        ('new', 'Open'),
        ('assigned', 'In Progress'),
        ('on_hold', 'On Hold'),
        ('done', 'Done'),
        ('cancel', 'Cancelled')
    ], string='Status', default='new', tracking=True, group_expand='_expand_states')

    priority = fields.Selection([
        ('0', 'Low'),
        ('1', 'Medium'),
        ('2', 'High')
    ], string='Priority', default='0', tracking=True)

    parent_id = fields.Many2one('mes.task', string="Parent Task", ondelete='cascade', index=True)
    child_ids = fields.One2many('mes.task', 'parent_id', string="Subtasks")

    @api.model
    def _expand_states(self, states, domain, order):
        return ['new', 'assigned', 'on_hold', 'done']

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
        _logger.info(">>> Starting load_tasks_from_maintainx...")
        
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
        except Exception as e:
            _logger.error(f"Sync Error: {e}")

        for wo in workorders:
            try:
                with self.env.cr.savepoint():
                    wo_id = wo.get('id', 'Unknown')
                    self._process_single_wo(self._get_by_id('workOrders', wo_id))
            except Exception as e:
                wo_id = wo.get('id', 'Unknown')
                _logger.error(f"SKIP WorkOrder {wo_id} due to error: {e}")

    def _process_single_wo(self, wo):
        _logger.info("wo: %s", wo)
        wo_id_raw = wo.get('id')
        wo_id = str(wo_id_raw).strip() if wo_id_raw is not None else ''
        if not wo_id:
            _logger.warning("WorkOrder has no ID")
            return
        
        title = wo.get('title', 'No Title')
        desc = wo.get('description', '') or ''
        
        mx_status = str(wo.get('status', '')).upper().strip()
        odoo_state = self._map_status(mx_status)

        mx_priority = str(wo.get('priority', 'LOW')).upper().strip()
        odoo_priority = self._map_priority(mx_priority)
        
        created_at_str = wo.get('createdAt')
        created_at = None
        if created_at_str:
            try:
                dt_aware = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                created_at = dt_aware.replace(tzinfo=None)
            except Exception as e:
                _logger.warning(f"Date parsing error for {wo_id}: {e}")
                pass

        updated_at_str = wo.get('updatedAt')

        machine_id = self._get_machine_from_asset(wo.get('asset') or {})

        assignee_ids = wo.get('assigneeIds', [])
        current_assignees_list = []
        employee_id = False 

        _logger.info("assignee_ids: %s", assignee_ids)

        for mx_user_id_raw in assignee_ids:
            mx_user_id = str(mx_user_id_raw) 
            current_assignees_list.append(mx_user_id)
            _logger.info("mx_user_id_raw: %s", mx_user_id_raw)
            if not employee_id:
                found_employee = self._get_or_create_employee(mx_user_id)
                if found_employee:
                    employee_id = found_employee.id

        current_assignees_str = ", ".join(current_assignees_list)

        task = self.search([('maintainx_id', '=', wo_id)], limit=1) if wo_id else self.search([('maintainx_id', '=', False)], limit=1)
        if not task and wo_id:
            task = self.search([('maintainx_id', 'ilike', wo_id)], limit=1)

        if task:
            _logger.debug(f"Existing task for MaintainX id={wo_id}: {task.id}")
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

    def _get_or_create_employee(self, mx_user_id):
        if not mx_user_id or str(mx_user_id) == 'Unknown':
            return False
        
        mx_user_id = str(mx_user_id)

        employee = self._find_employee_by_mx_id(mx_user_id)
        if employee:
            return employee

        _logger.info(f"Employee {mx_user_id} not found by ID. Fetching info from API...")
        user_data = self._get_by_id('users', mx_user_id)
        
        if not user_data:
            _logger.warning(f"Could not fetch data for User ID {mx_user_id}")
            return False

        employee = self._find_employee_by_name(user_data)
        if employee:
            employee.write({'maintainx_id': mx_user_id})
            _logger.info(f"Linked existing employee '{employee.name}' to MaintainX ID {mx_user_id}")
            return employee

        return self._create_mx_employee(user_data, mx_user_id)

    def _find_employee_by_mx_id(self, mx_user_id):
        return self.env['hr.employee'].search([('maintainx_id', '=', mx_user_id)], limit=1)

    def _find_employee_by_name(self, assignee_data):
        first_name = assignee_data.get('firstName', '').strip()
        last_name = assignee_data.get('lastName', '').strip()
        
        full_name = f"{first_name} {last_name}".strip()
        
        if not full_name:
            return False

        return self.env['hr.employee'].search([('name', 'ilike', full_name)], limit=1)

    def _create_mx_employee(self, assignee_data, mx_user_id):
        first_name = assignee_data.get('firstName', '').strip()
        last_name = assignee_data.get('lastName', '').strip()
        full_name = f"{first_name} {last_name}".strip()
        
        name_to_create = full_name if full_name else f"MX User {mx_user_id}"
        
        try:
            new_emp = self.env['hr.employee'].create({
                'name': name_to_create,
                'maintainx_id': mx_user_id,
                'work_email': assignee_data.get('email')
            })
            _logger.info(f"Created new employee: {name_to_create} (MX ID: {mx_user_id})")
            return new_emp
        except Exception as e:
            _logger.error(f"Failed to create employee {name_to_create}: {e}")
            return False

    def _get_by_id(self, area, id):
        try:
            base_url, headers = self._get_maintainx_config()
        except UserError:
            return None

        endpoint = f"{base_url}/{area}/{id}"
        params = {}

        try:
            response = requests.get(endpoint, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if isinstance(data, list):
                return data
            
            w = data.get(area[:-1]) or []
            return w
            
        except Exception as e:
            _logger.error(f"Sync Error: {e}")
            return None

        return None

    def _get_machine_from_asset(self, asset_data):
        if not asset_data:
            return False
        if asset_data:
            if not asset_data.get('parentId'):
                asset_id = asset_data.get('id')
                machine = self.env['mrp.workcenter'].search([('maintainx_id', '=', asset_id)], limit=1)
                if machine:
                    return machine.id
            else:
                parent_id = asset_data.get('parentId') or {}
                return self._get_machine_from_asset(self._get_by_id('assets', parent_id))
        return False

    def _map_status(self, status):
        return _STATUS_MAPPING.get(status, 'new')

    def _map_priority(self, priority):
        return _PRIORITY_MAPPING.get(priority, '0')

    def action_send_to_maintainx(self):
        self.ensure_one()
        # ... TODO: Sending logic here ...
        pass