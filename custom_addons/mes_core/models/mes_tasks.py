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
    
    company_id = fields.Many2one('res.company', string='Company', required=True, default=lambda self: self.env.company)

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
        # We only use fields that are guaranteed to be identical in both 
        # list view (get_workorders) and detail view (get_workorder).
        # Description is often truncated or missing in list views.
        significant_data = {
            'id': data.get('id'),
            'updatedAt': data.get('updatedAt'),
            'status': data.get('status'),
        }
        return hashlib.sha256(json.dumps(significant_data, sort_keys=True).encode('utf-8')).hexdigest()

    @api.model
    def load_tasks_from_maintainx(self):
        _logger.info(">>> Starting load_tasks_from_maintainx...")
        try:
            client = self._get_api_client()
            workorders_list = client.get_workorders(limit=200)
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

            # MaintainX doesn't provide assignees in the main list endpoint, 
            # so we need to sync all tasks with different hash to get that info and update assignee history
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
        full_wo_data = client.get_workorder(workorder_id)
        if not full_wo_data:
            return
        
        cache = {'employees': {}, 'machines': {}} 
        self._process_single_wo(full_wo_data, client, cache)

    def _process_single_wo(self, wo, client, cache):
        if not wo.get('id'):
            _logger.error(f"Received WorkOrder data without ID. Data keys: {list(wo.keys())}")
            return

        wo_id = str(wo.get('id'))
        vals = self._prepare_task_values(wo, client, cache)
        
        task = self.search([('maintainx_id', '=', wo_id)], limit=1)
        
        sync_result = {
            'action': 'none',
            'task_id': False,
            'changes': {}
        }

        if not task:
            _logger.info(f"Creating Task {wo_id}")
            task = self.create(vals)
            self._create_status_history(task, wo.get('status'))
            
            task.message_post(body="Task created from MaintainX sync.")
            
            sync_result.update({'action': 'created', 'task_id': task.id, 'changes': vals})
            return sync_result

        if 'maintainx_assignees_history' in vals:
            new_history_entry = vals.pop('maintainx_assignees_history', '')
            if new_history_entry:
                current_hist = task.maintainx_assignees_history or ""
                vals['maintainx_assignees_history'] = current_hist + new_history_entry

        changes = self._compute_task_delta(task, vals)

        old_state = task.state
        task.write(vals)

        if changes:
            html_message = self._format_load_message(changes) 
            if html_message:
                task.message_post(body=html_message)
            
            sync_result.update({'action': 'updated', 'task_id': task.id, 'changes': changes})
        else:
            sync_result.update({'action': 'silent_update', 'task_id': task.id})

        if old_state != vals.get('state'):
            self._create_status_history(task, wo.get('status'))
        
        return sync_result

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

        user_data = client.get_user(mx_user_id) 
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

        asset_data = client.get_asset(asset_id)
        if not asset_data:
            return False
            
        parent_id = asset_data.get('parentId')
        if parent_id:
            res = self._get_machine_recursive(parent_id, client, cache, depth + 1)
            if res:
                cache['machines'][asset_id] = res
            return res
            
        return False
    
    def _compute_task_delta(self, task, vals):
        changes = {}
        IGNORED_FIELDS = {
            'maintainx_data_hash', 
            'maintainx_updated_at', 
            'maintainx_assignees_history',
            'maintainx_created_at',
            'maintainx_id'
        }

        for field, new_value in vals.items():
            if field in IGNORED_FIELDS:
                continue
            
            try:
                current_value = task[field]
                
                if hasattr(current_value, 'id'): 
                    current_id = current_value.id or False
                    new_id = new_value or False
                    
                    if current_id != new_id:
                        changes[field] = {
                            'old': current_value.display_name if current_value else 'Empty', 
                            'new': self._get_name_from_id(field, new_value) # См. ниже про этот метод
                        }
                
                else:
                    c_val = current_value or False
                    n_val = new_value or False
                    
                    if c_val != n_val:
                        changes[field] = {'old': c_val, 'new': n_val}
                        
            except KeyError:
                continue
                
        return changes

    def _get_name_from_id(self, field_name, res_id):
        if not res_id:
            return 'Empty'
            
        model_map = {
            'machine_id': 'mrp.workcenter',
            'assigned_id': 'hr.employee',
            'author_id': 'res.users'
        }
        
        model = model_map.get(field_name)
        if model:
            rec = self.env[model].browse(res_id)
            return rec.name if rec.exists() else f"ID {res_id}"
        return str(res_id)

    def _format_load_message(self, changes):
        if not changes:
            return None
            
        msg_body = "<b>Updated from MaintainX:</b><ul>"
        for f, diff in changes.items():
            field_label = f.replace('_', ' ').capitalize()
            msg_body += f"<li>{field_label}: {diff['old']} &rarr; {diff['new']}</li>"
        msg_body += "</ul>"
        return msg_body