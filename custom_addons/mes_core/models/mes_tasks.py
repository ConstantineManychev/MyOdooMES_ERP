import logging
import pytz
import json
import hashlib
from dateutil import parser
from datetime import datetime
from odoo import models, fields, api
from odoo.exceptions import UserError
from odoo.addons.mes_core.tools.maintainx_api import MaintainXClient

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

    name = fields.Char(string='Task Title', required=True)
    description = fields.Html(string='Description')
    
    maintainx_id = fields.Char(string="MaintainX ID", readonly=True, copy=False, index=True)
    maintainx_created_at = fields.Datetime(string="MX Created At", readonly=True)
    maintainx_assignees_history = fields.Text(string="Assignees History (MX)", readonly=True)
    maintainx_updated_at = fields.Datetime(string="MX Updated At", readonly=True)
    maintainx_data_hash = fields.Char(string="Data Hash", index=True, copy=False)

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

    def _get_api_client(self):
        token = self.env['ir.config_parameter'].sudo().get_param('gemba.maintainx_token')
        if not token:
            raise UserError("MaintainX Token not found in System Parameters.")
        return MaintainXClient(token)

    @api.model
    def _parse_date(self, date_str):
        if not date_str:
            return False
        try:
            dt = parser.parse(date_str)
            return dt.astimezone(pytz.UTC).replace(tzinfo=None)
        except (ValueError, TypeError):
            _logger.warning(f"Failed to parse date: {date_str}")
            return False

    def _calculate_hash(self, data):
        significant_data = {
            'title': data.get('title'),
            'desc': data.get('description'),
            'status': data.get('status'),
            'priority': data.get('priority'),
            #'assignees': sorted(data.get('assigneeIds', [])),
            'assetId': data.get('assetId'),
            'updatedAt': data.get('updatedAt'),
        }
        return hashlib.sha256(json.dumps(significant_data, sort_keys=True).encode('utf-8')).hexdigest()

    @api.model
    def load_tasks_from_maintainx(self):
        _logger.info(">>> Starting load_tasks_from_maintainx...")
        try:
            client = self._get_api_client()
            response = client.get_workorders(limit=200)
            workorders_list = response.get('workOrders') or response.get('items') or []
        except Exception as e:
            _logger.exception("Sync Failed during API call")
            return

        mx_ids = [str(wo.get('id')) for wo in workorders_list if wo.get('id')]
        existing_tasks = self.search([('maintainx_id', 'in', mx_ids)])
        existing_map = {task.maintainx_id: task for task in existing_tasks}

        jobs_count = 0
        for wo_data in workorders_list:
            wo_id = str(wo_data.get('id'))
            if not wo_id:
                continue

            new_hash = self._calculate_hash(wo_data)
            existing_task = existing_map.get(wo_id)

            if existing_task and existing_task.maintainx_data_hash == new_hash:
                continue

            self.with_delay(
                channel='root.maintainx',
                description=f"Sync MX Task {wo_id}",
                priority=10,
                identity_key=f"mx_sync_{wo_id}"
            ).action_sync_single_wo_job(wo_id)
            jobs_count += 1

        _logger.info(f"Sync finished. Created {jobs_count} update jobs.")

    def action_sync_single_wo_job(self, workorder_id):
        client = self._get_api_client()
        full_wo_data = client.get_entity('workorders', workorder_id)
        if not full_wo_data:
            return
        
        cache = {'employees': {}, 'machines': {}} 
        self._process_single_wo(full_wo_data, client, cache)

    def _process_single_wo(self, wo, client, cache):
        wo_id = str(wo.get('id'))
        vals = self._prepare_task_values(wo, client, cache)
        
        task = self.search([('maintainx_id', '=', wo_id)], limit=1)

        if not task:
            _logger.info(f"Creating Task {wo_id}")
            task = self.create(vals)
            self._create_status_history(task, wo.get('status'))
        else:
            old_state = task.state
            
            new_history = vals.pop('maintainx_assignees_history', '')
            if new_history:
                vals['maintainx_assignees_history'] = (task.maintainx_assignees_history or "") + new_history

            task.write(vals)
            if old_state != vals.get('state'):
                self._create_status_history(task, wo.get('status'))

    def _prepare_task_values(self, wo, client, cache):
        mx_status = str(wo.get('status', '')).upper().strip()
        mx_priority = str(wo.get('priority', 'LOW')).upper().strip()
        
        assignee_ids = wo.get('assigneeIds', [])
        assignee_names = []
        employee_id = False
        
        for mx_user_id in assignee_ids:
            emp = self._get_or_create_employee(str(mx_user_id), client, cache)
            if emp:
                assignee_names.append(emp.name)
                if not employee_id:
                    employee_id = emp.id
        
        current_assignees_str = ", ".join(assignee_names)
        assignee_history_line = f"{fields.Datetime.now()}: {current_assignees_str}\n"

        machine_id = False
        if wo.get('assetId'):
            machine_id = self._get_machine_recursive(wo.get('assetId'), client, cache)

        return {
            'name': wo.get('title', 'No Title'),
            'description': wo.get('description', ''),
            'maintainx_id': str(wo.get('id')),
            'maintainx_created_at': self._parse_date(wo.get('createdAt')),
            'maintainx_updated_at': self._parse_date(wo.get('updatedAt')),
            'maintainx_data_hash': self._calculate_hash(wo),
            'state': self._STATUS_MAPPING.get(mx_status, 'new'),
            'priority': self._PRIORITY_MAPPING.get(mx_priority, '0'),
            'assigned_id': employee_id,
            'machine_id': machine_id,
            'maintainx_assignees_history': assignee_history_line
        }

    def _create_status_history(self, task, raw_status):
        self.env['mes.task.status.history'].create({
            'task_id': task.id,
            'status': str(raw_status).upper(),
            'change_date': fields.Datetime.now()
        })

    def _get_or_create_employee(self, mx_user_id, client, cache):
        if mx_user_id in cache['employees']:
            return cache['employees'][mx_user_id]

        _logger.info(f"Looking for employee with MaintainX ID {mx_user_id}")
        employee = self.env['hr.employee'].search([('maintainx_id', '=', mx_user_id)], limit=1)
        if employee:
            cache['employees'][mx_user_id] = employee
            return employee

        user_data = client.get_entity('users', mx_user_id)
        if not user_data:
            return False

        first = user_data.get('firstName', '').strip()
        last = user_data.get('lastName', '').strip()
        full_name = f"{first} {last}".strip()
        
        employee = self.env['hr.employee'].search([('name', 'ilike', full_name)], limit=1)
        if employee:
            employee.write({'maintainx_id': mx_user_id})
        else:
            try:
                _logger.info(f"Creating employee for MX user {mx_user_id} with name '{full_name}'")
                employee = self.env['hr.employee'].create({
                    'name': full_name or f"MX User {mx_user_id}",
                    'maintainx_id': mx_user_id,
                    'work_email': user_data.get('email')
                })
            except Exception as e:
                _logger.error(f"Cannot create employee: {e}")
                return False

        cache['employees'][mx_user_id] = employee
        return employee

    def _get_machine_recursive(self, asset_id, client, cache, depth=0):
        if not asset_id or depth > 5:
            return False

        if asset_id in cache['machines']:
            return cache['machines'][asset_id]

        machine = self.env['mrp.workcenter'].search([('maintainx_id', '=', asset_id)], limit=1)
        if machine:
            cache['machines'][asset_id] = machine.id
            return machine.id

        asset_data = client.get_entity('assets', asset_id)
        if not asset_data:
            return False
            
        parent_id = asset_data.get('parentId')
        if parent_id:
            res = self._get_machine_recursive(parent_id, client, cache, depth + 1)
            if res:
                cache['machines'][asset_id] = res
            return res
            
        return False