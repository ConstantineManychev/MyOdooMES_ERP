import base64
import hashlib
import logging
import os

import psycopg2

from odoo import models, fields, api, _
from odoo.exceptions import UserError
from odoo.tools import mute_logger

_logger = logging.getLogger(__name__)

SOURCE_PATH_PARAM = 'mes_core.logger_source_path'
DEFAULT_SOURCE_PATH = '/mnt/externals/logger_automate.py'
ADMIN_GROUP = 'mes_core.group_mes_administrator'


class MesLoggerRelease(models.Model):
    _name = 'mes.logger.release'
    _description = 'Logger Release'
    _order = 'detected_at desc, id desc'

    name = fields.Char(string='Version', required=True, readonly=True)
    sha256 = fields.Char(string='SHA-256', required=True, readonly=True, index=True)
    content = fields.Binary(string='File', attachment=False, readonly=True)
    size = fields.Integer(string='Size (bytes)', readonly=True)
    state = fields.Selection([
        ('draft', 'Draft'),
        ('pilot', 'Pilot'),
        ('released', 'Released'),
        ('archived', 'Archived'),
    ], string='State', default='draft', required=True, readonly=True)
    pilot_machine_ids = fields.Many2many('mes.machine.settings', string='Pilot Machines')
    detected_at = fields.Datetime(string='Detected', readonly=True, default=fields.Datetime.now)
    released_at = fields.Datetime(string='Released', readonly=True)
    released_by_id = fields.Many2one('res.users', string='Released By', readonly=True)
    note = fields.Text(string='Release Notes')
    installed_count = fields.Integer(string='Machines Running', compute='_compute_installed_count')

    _sql_constraints = [
        ('sha256_uniq', 'unique(sha256)', 'This logger version is already registered.'),
    ]

    def _compute_installed_count(self):
        Machine = self.env['mes.machine.settings'].sudo()
        for release in self:
            release.installed_count = Machine.search_count([('logger_sha256', '=', release.sha256)])

    @api.model
    def _source_path(self):
        return self.env['ir.config_parameter'].sudo().get_param(SOURCE_PATH_PARAM, DEFAULT_SOURCE_PATH)

    @api.model
    def _read_source(self):
        path = self._source_path()
        if not os.path.isfile(path):
            return None, path
        with open(path, 'rb') as handle:
            return handle.read(), path

    @api.model
    def _register_source(self):
        data, path = self._read_source()
        if data is None:
            return None, path, False
        digest = hashlib.sha256(data).hexdigest()
        existing = self.sudo().search([('sha256', '=', digest)], limit=1)
        if existing:
            if existing._content_ok():
                return existing, path, False
            existing.write({'content': base64.b64encode(data), 'size': len(data)})
            _logger.warning("Logger release %s had no intact file; restored it from %s", existing.name, path)
            return existing, path, 'restored'
        try:
            with self.env.cr.savepoint(), mute_logger('odoo.sql_db'):
                release = self.sudo().create({
                    'name': f"{fields.Datetime.now():%Y-%m-%d %H:%M} · {digest[:8]}",
                    'sha256': digest,
                    'content': base64.b64encode(data),
                    'size': len(data),
                })
        except psycopg2.IntegrityError:
            _logger.info("Logger release %s was registered at the same moment by another process", digest[:8])
            return self.browse(), path, False
        _logger.info("Logger release detected: %s (%s bytes) from %s", release.name, len(data), path)
        return release, path, True

    @api.model
    def _cron_detect_new_version(self):
        try:
            self._register_source()
        except Exception:
            _logger.exception("Logger release detection failed")

    def action_detect_new_version(self):
        self._check_admin()
        release, path, created = self._register_source()
        if release is None:
            raise UserError(_("Logger source file not found: %(path)s", path=path))
        if not release:
            message = _("This version was registered a moment ago by the automatic check.")
        elif created == 'restored':
            message = _("The stored file of version %(name)s was empty or damaged; it has been restored.",
                        name=release.name)
        elif created:
            message = _("New logger version registered: %(name)s", name=release.name)
        else:
            message = _("No changes: the file matches version %(name)s", name=release.name)
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'type': 'success' if created else 'info',
                'title': _("Logger Releases"),
                'message': message,
                'next': {'type': 'ir.actions.client', 'tag': 'reload'},
            },
        }

    def _check_admin(self):
        if not self.env.user.has_group(ADMIN_GROUP):
            raise UserError(_("Only MES administrators can manage logger releases."))

    def action_release_pilot(self):
        self._check_admin()
        self.ensure_one()
        if not self.pilot_machine_ids:
            raise UserError(_("Select at least one pilot machine first."))
        if self.state not in ('draft', 'pilot'):
            raise UserError(_("Only a draft release can go to pilot."))
        self.search([('state', '=', 'pilot'), ('id', '!=', self.id)]).write({'state': 'draft'})
        self.write({'state': 'pilot'})

    def action_release_all(self):
        self._check_admin()
        self.ensure_one()
        if self.state not in ('draft', 'pilot'):
            raise UserError(_("Only a draft or pilot release can be released."))
        self.search([('state', '=', 'released'), ('id', '!=', self.id)]).write({'state': 'archived'})
        self.write({
            'state': 'released',
            'released_at': fields.Datetime.now(),
            'released_by_id': self.env.user.id,
        })

    def action_archive_release(self):
        self._check_admin()
        self.write({'state': 'archived'})

    def action_back_to_draft(self):
        self._check_admin()
        self.write({'state': 'draft'})

    @api.model
    def _target_for_machine(self, machine):
        if not machine:
            return self.browse()
        pilot = self.sudo().search([
            ('state', '=', 'pilot'),
            ('pilot_machine_ids', 'in', machine.ids),
        ], limit=1)
        if pilot:
            return pilot
        return self.sudo().search([('state', '=', 'released')], limit=1)

    def _content_b64(self):
        self.ensure_one()
        raw = self.sudo().content
        if isinstance(raw, bytes):
            return raw.decode('ascii')
        return raw or ''

    def _content_ok(self):
        self.ensure_one()
        try:
            data = base64.b64decode(self._content_b64())
        except (ValueError, TypeError):
            return False
        return bool(data) and hashlib.sha256(data).hexdigest() == self.sha256


class MesMachineSettings(models.Model):
    _inherit = 'mes.machine.settings'

    logger_sha256 = fields.Char(string='Logger SHA-256', readonly=True)
    logger_checked_at = fields.Datetime(string='Logger Last Update Check', readonly=True)
    logger_release_id = fields.Many2one('mes.logger.release', string='Running Logger Version',
                                        compute='_compute_logger_release')
    logger_target_release_id = fields.Many2one('mes.logger.release', string='Target Logger Version',
                                               compute='_compute_logger_release')
    logger_update_state = fields.Selection([
        ('up_to_date', 'Up to date'),
        ('pending', 'Update pending'),
        ('unknown', 'Unknown version'),
        ('no_release', 'No release published'),
    ], string='Logger Update State', compute='_compute_logger_release')

    def _compute_logger_release(self):
        Release = self.env['mes.logger.release'].sudo()
        for machine in self:
            running = Release.search([('sha256', '=', machine.logger_sha256)], limit=1) \
                if machine.logger_sha256 else Release.browse()
            target = Release._target_for_machine(machine)
            machine.logger_release_id = running
            machine.logger_target_release_id = target
            if not target:
                machine.logger_update_state = 'no_release'
            elif not machine.logger_sha256 or not running:
                machine.logger_update_state = 'unknown'
            elif running == target:
                machine.logger_update_state = 'up_to_date'
            else:
                machine.logger_update_state = 'pending'


class MrpWorkcenter(models.Model):
    _inherit = 'mrp.workcenter'

    logger_checked_at = fields.Datetime(related='machine_settings_id.logger_checked_at')
    logger_release_id = fields.Many2one(related='machine_settings_id.logger_release_id')
    logger_target_release_id = fields.Many2one(related='machine_settings_id.logger_target_release_id')
    logger_update_state = fields.Selection(related='machine_settings_id.logger_update_state')
