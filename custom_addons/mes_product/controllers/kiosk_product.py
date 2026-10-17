from odoo import http
from odoo.http import request
from odoo.exceptions import UserError

from odoo.addons.mes_core.controllers.kiosk import MesKioskController


class MesKioskProductController(MesKioskController):

    @http.route('/mes/kiosk/product/options', type='json', auth='user')
    def kiosk_product_options(self, workcenter_id):
        wc = self._validate_workcenter_access(workcenter_id)
        try:
            return dict(wc._kiosk_product_options(), ok=True)
        except UserError as exc:
            return {'ok': False, 'error': str(exc)}

    @http.route('/mes/kiosk/product/set', type='json', auth='user')
    def kiosk_product_set(self, workcenter_id, session_id, template_id, value_ids, moment):
        wc = self._validate_workcenter_access(workcenter_id)
        try:
            return dict(wc._kiosk_set_product(session_id, template_id, value_ids, moment), ok=True)
        except UserError as exc:
            request.env.cr.rollback()
            return {'ok': False, 'error': str(exc)}
