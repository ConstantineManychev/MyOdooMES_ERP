from odoo import models, fields, api

class MesTask(models.Model):
    _name = 'mes.task'
    _description = 'MES Task'
    _inherit = ['mail.thread', 'mail.activity.mixin'] # История изменений и ответственных тут "из коробки"

    name = fields.Char(string='Task Title', required=True)
    description = fields.Html(string='Description')
    
    # Автор и Ответственный
    author_id = fields.Many2one('res.users', string='Author', default=lambda self: self.env.user, readonly=True)
    user_id = fields.Many2one('res.users', string='Assigned To', tracking=True) # tracking=True пишет историю в лог
    
    machine_id = fields.Many2one('mrp.workcenter', string='Machine')
    
    state = fields.Selection([
        ('new', 'New'),
        ('assigned', 'In Progress'),
        ('done', 'Done'),
        ('cancel', 'Cancelled')
    ], string='Status', default='new', tracking=True)

    @api.model_create_multi
    def create(self, vals_list):
        tasks = super().create(vals_list)
        for task in tasks:
            if task.user_id:
                task._notify_assignee()
        return tasks

    def write(self, vals):
        res = super().write(vals)
        if 'user_id' in vals:
            self._notify_assignee()
        if vals.get('state') == 'done':
            self._notify_author_confirmation()
        return res

    def _notify_assignee(self):
        """Creates an activity (To Do) for the assigned user."""
        for task in self:
            if task.user_id:
                task.activity_schedule(
                    'mail.mail_activity_data_todo',
                    user_id=task.user_id.id,
                    summary=f'New Task Assigned: {task.name}',
                    note='Please check this task.'
                )

    def _notify_author_confirmation(self):
        """Returns task to author for confirmation."""
        for task in self:
            task.activity_schedule(
                'mail.mail_activity_data_todo',
                user_id=task.author_id.id,
                summary=f'Task Done: {task.name}',
                note='Please confirm the resolution.'
            )

    def action_done(self):
        self.write({'state': 'done'})