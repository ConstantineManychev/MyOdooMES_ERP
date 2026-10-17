import base64
import hashlib
import logging

from odoo import api, SUPERUSER_ID

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    env = api.Environment(cr, SUPERUSER_ID, {})
    Release = env['mes.logger.release']
    attachments = env['ir.attachment'].search([
        ('res_model', '=', 'mes.logger.release'),
        ('res_field', '=', 'content'),
    ])
    for attachment in attachments:
        release = Release.browse(attachment.res_id).exists()
        if not release or release._content_ok():
            continue
        data = attachment.raw
        if data and hashlib.sha256(data).hexdigest() == release.sha256:
            release.write({'content': base64.b64encode(data)})
        else:
            _logger.warning("Logger release %s: stored file missing or damaged in the filestore", release.name)
    attachments.unlink()
    Release._register_source()
