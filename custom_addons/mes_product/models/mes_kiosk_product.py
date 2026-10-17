import logging
from datetime import datetime, timedelta

from odoo import models, fields, _
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

KIOSK_BACKDATE_LIMIT = timedelta(hours=24)
KIOSK_FUTURE_TOLERANCE = timedelta(minutes=1)
WALL_FORMAT = '%Y-%m-%dT%H:%M'


def parse_wall_moment(text):
    cleaned = (text or '').strip().replace(' ', 'T')
    for pattern in ('%Y-%m-%dT%H:%M:%S', WALL_FORMAT):
        try:
            return datetime.strptime(cleaned, pattern)
        except ValueError:
            continue
    return None


def check_moment(moment, now):
    if moment > now + KIOSK_FUTURE_TOLERANCE:
        return 'future'
    if moment < now - KIOSK_BACKDATE_LIMIT:
        return 'too_old'
    return None


class MrpWorkcenter(models.Model):
    _inherit = 'mrp.workcenter'

    def _kiosk_product_templates(self):
        self.ensure_one()
        configs = self.env['mes.workcenter.product.config'].sudo().search(
            [('workcenter_id', '=', self.id)], order='date_from desc')
        templates = configs.mapped('product_tmpl_id')
        current = self.sudo().current_product_id
        if current:
            templates |= current.product_tmpl_id
        return templates.filtered('active')

    def _kiosk_variant_lines(self, template):
        return template.attribute_line_ids.filtered(
            lambda line: line.attribute_id.create_variant != 'no_variant')

    def _kiosk_default_values(self, template):
        current = self.sudo().current_product_id
        if current and current.product_tmpl_id == template:
            return current.product_template_attribute_value_ids.ids
        config = self.env['mes.workcenter.product.config'].sudo().search([
            ('workcenter_id', '=', self.id),
            ('product_tmpl_id', '=', template.id),
        ], order='date_from desc', limit=1)
        fixed = config.attribute_value_ids
        lines = self._kiosk_variant_lines(template)
        chosen = []
        for line in lines:
            values = line.product_template_value_ids.filtered('ptav_active')
            preferred = values.filtered(lambda value: value.product_attribute_value_id in fixed)[:1]
            if preferred:
                chosen.append(preferred.id)
        if len(chosen) == len(lines):
            return chosen
        first = template.product_variant_ids[:1]
        return first.product_template_attribute_value_ids.ids

    def _kiosk_product_options(self):
        self.ensure_one()
        engine = self.env['mes.product.engine'].sudo()
        now = fields.Datetime.now()
        now_wall = engine._to_wall(self, now)
        shift_start, _shift_end = self.env['mes.shift'].sudo().get_current_shift_window(self)
        templates = self._kiosk_product_templates()
        if not templates:
            raise UserError(_("No output product is configured for this machine."))
        current = self.sudo().current_product_id
        payload = []
        for template in templates:
            lines = self._kiosk_variant_lines(template)
            payload.append({
                'id': template.id,
                'name': template.name,
                'lines': [{
                    'id': line.id,
                    'name': line.attribute_id.name,
                    'values': [{'id': value.id, 'name': value.name}
                               for value in line.product_template_value_ids.filtered('ptav_active')],
                } for line in lines],
                'variants': [{
                    'id': variant.id,
                    'name': variant.display_name,
                    'value_ids': variant.product_template_attribute_value_ids.ids,
                } for variant in template.product_variant_ids],
                'default_value_ids': self._kiosk_default_values(template),
            })
        return {
            'templates': payload,
            'current_template_id': current.product_tmpl_id.id if current else templates[0].id,
            'current_product_id': current.id or False,
            'now': now_wall.strftime(WALL_FORMAT),
            'shift_start': (shift_start or now_wall).strftime(WALL_FORMAT),
            'earliest': (now_wall - KIOSK_BACKDATE_LIMIT).strftime(WALL_FORMAT),
        }

    def _kiosk_set_product(self, session_id, template_id, value_ids, moment):
        self.ensure_one()
        session = self.env['mes.kiosk.session'].sudo().browse(int(session_id or 0)).exists()
        if not session or session not in self._active_kiosk_sessions():
            raise UserError(_("Log in to change the product."))
        template = self.env['product.template'].sudo().browse(int(template_id or 0)).exists()
        if not template or template not in self._kiosk_product_templates():
            raise UserError(_("This product is not configured for this machine."))
        values = self.env['product.template.attribute.value'].sudo().browse(
            [int(value) for value in value_ids or []]).exists()
        lines = self._kiosk_variant_lines(template)
        if len(values) != len(lines) or values.mapped('attribute_line_id') != lines:
            raise UserError(_("Choose one value for every characteristic."))
        variant = template._get_variant_for_combination(values)
        if not variant or not variant.active:
            raise UserError(_("There is no product with this combination of characteristics."))
        wall = parse_wall_moment(moment)
        if not wall:
            raise UserError(_("The time is not valid."))
        engine = self.env['mes.product.engine'].sudo()
        start = engine._from_wall(self, wall)
        problem = check_moment(start, fields.Datetime.now())
        if problem == 'future':
            raise UserError(_("The time cannot be in the future."))
        if problem == 'too_old':
            raise UserError(_("The time cannot be more than 24 hours ago."))
        values_to_write = {
            'workcenter_id': self.id,
            'product_id': variant.id,
            'date_start': start.replace(second=0, microsecond=0),
            'date_end': False,
            'kind': 'correction',
            'origin': 'kiosk',
            'employee_id': session.employee_id.id,
            'user_id': session.user_id.id or self.env.uid,
            'changed_at': fields.Datetime.now(),
        }
        Override = self.env['mes.product.override'].sudo()
        existing = Override.search([
            ('workcenter_id', '=', self.id),
            ('date_start', '=', values_to_write['date_start']),
        ], limit=1)
        if existing:
            existing.write(values_to_write)
        else:
            Override.create(values_to_write)
        applied = True
        try:
            with self.env.cr.savepoint():
                self.env.cr.execute("SET LOCAL lock_timeout = '3s'")
                engine._rebuild_dirty(self)
        except Exception:
            _logger.warning("Kiosk product change on %s saved; history will be rebuilt by the cron",
                            self.name, exc_info=True)
            applied = False
        return {'product_name': variant.display_name, 'applied': applied}
