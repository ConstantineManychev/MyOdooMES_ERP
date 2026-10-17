from odoo import models, fields, api
from odoo.http import request
from .mes_count_dashboard import (
    DASHBOARD_INTERVAL_SELECTION,
    DASHBOARD_PERIOD_MODES,
    DASHBOARD_VIEW_MODES,
    DEFAULT_INTERVAL_MINUTES,
    DEFAULT_PERIOD_HOURS,
)

MES_DASHBOARD_PREF_FIELDS = [
    'mes_dashboard_count_ids',
    'mes_dashboard_last_count_id',
    'mes_dashboard_interval',
    'mes_dashboard_period_mode',
    'mes_dashboard_period_hours',
    'mes_dashboard_view_mode',
]


class ResUsers(models.Model):
    _inherit = 'res.users'

    fob_uid = fields.Char(string='FOB UID', copy=False, index=True, groups='mes_core.group_mes_supervisor,base.group_system')

    mes_dashboard_count_ids = fields.Many2many(
        'mes.counts', 'mes_user_dashboard_count_rel', 'user_id', 'count_id',
        string='Dashboard Counts',
        help="Counts offered as tabs on the Counts Dashboard. Empty means no tabs "
             "are shown, so pick at least one count here.")
    mes_dashboard_last_count_id = fields.Many2one(
        'mes.counts', string='Last Dashboard Count', ondelete='set null')
    mes_dashboard_interval = fields.Selection(
        DASHBOARD_INTERVAL_SELECTION, string='Dashboard Interval',
        default=str(DEFAULT_INTERVAL_MINUTES))
    mes_dashboard_period_mode = fields.Selection(
        DASHBOARD_PERIOD_MODES, string='Dashboard Period', default='shift')
    mes_dashboard_period_hours = fields.Integer(
        string='Dashboard Period Hours', default=DEFAULT_PERIOD_HOURS)
    mes_dashboard_view_mode = fields.Selection(
        DASHBOARD_VIEW_MODES, string='Dashboard View', default='table')

    _sql_constraints = [
        ('fob_uid_uniq', 'unique(fob_uid)', 'FOB UID must be unique across users.'),
    ]

    @property
    def SELF_READABLE_FIELDS(self):
        return super().SELF_READABLE_FIELDS + ['fob_uid'] + MES_DASHBOARD_PREF_FIELDS

    @property
    def SELF_WRITEABLE_FIELDS(self):
        return super().SELF_WRITEABLE_FIELDS + ['fob_uid'] + MES_DASHBOARD_PREF_FIELDS

    @api.model
    def _get_real_client_ip(self):
        if not request:
            return None
        ip = (
            request.httprequest.headers.get('X-Real-IP') or
            (request.httprequest.headers.get('X-Forwarded-For') or '').split(',')[0].strip() or
            request.httprequest.remote_addr
        )
        return ip.strip() if ip else None

    @api.model
    def _get_mes_accessible_workcenter_ids(self):
        Workcenter = self.env['mrp.workcenter'].sudo()
        user = self.env.user
        if user.has_group('mes_core.group_mes_manager'):
            return Workcenter.search([]).ids
        if not request:
            return Workcenter.search([]).ids
        client_ip = self._get_real_client_ip()
        if not client_ip:
            return []
        candidates = Workcenter.search([('allowed_pc_ips', '!=', False)])
        return candidates.filtered(
            lambda wc: client_ip in {ip.strip() for ip in (wc.allowed_pc_ips or '').split(',') if ip.strip()}
        ).ids

    @api.model
    def _resolve_workcenter_by_ip(self, client_ip):
        if not client_ip:
            return self.env['mrp.workcenter']
        candidates = self.env['mrp.workcenter'].sudo().search([('allowed_pc_ips', '!=', False)])
        return candidates.filtered(
            lambda wc: client_ip in {ip.strip() for ip in (wc.allowed_pc_ips or '').split(',') if ip.strip()}
        )[:1]
