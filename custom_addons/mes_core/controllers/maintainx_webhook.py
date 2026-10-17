import hashlib
import hmac
import json
import logging
import time

from odoo import http
from odoo.http import request

log = logging.getLogger(__name__)

SIGNATURE_HEADER = 'x-maintainx-webhook-body-signature'
SIGNATURE_TOLERANCE_SEC = 300


class MaintainXWebhook(http.Controller):

    @http.route('/mes/maintainx/webhook', type='http', auth='public',
                methods=['POST'], csrf=False, save_session=False)
    def maintainx_webhook(self, **kwargs):
        raw_body = request.httprequest.get_data(as_text=True)
        signature_header = request.httprequest.headers.get(SIGNATURE_HEADER, '')

        company = self._resolve_signing_company(raw_body, signature_header)
        if not company:
            return self._reply(401, {'error': 'Invalid signature'})

        try:
            payload = json.loads(raw_body or '{}')
        except ValueError:
            return self._reply(400, {'error': 'Malformed payload'})

        handled = self._apply_status_change(payload, company)
        return self._reply(200, {'ok': True, 'handled': handled})

    def _reply(self, status, body):
        return request.make_response(
            json.dumps(body),
            headers=[('Content-Type', 'application/json')],
            status=status,
        )

    def _resolve_signing_company(self, raw_body, signature_header):
        timestamp, signature = self._parse_signature_header(signature_header)
        if not timestamp or not signature:
            return None
        if abs(time.time() - timestamp) > SIGNATURE_TOLERANCE_SEC:
            log.warning("MaintainX webhook rejected: timestamp outside tolerance")
            return None

        signed_payload = f"{int(timestamp)}.{raw_body}"
        companies = request.env['res.company'].sudo().search([
            ('maintainx_enabled', '=', True),
            ('maintainx_webhook_secret', '!=', False),
        ])
        for company in companies:
            expected = hmac.new(
                company.maintainx_webhook_secret.encode('utf-8'),
                signed_payload.encode('utf-8'),
                hashlib.sha256,
            ).hexdigest()
            if hmac.compare_digest(expected, signature):
                return company
        log.warning("MaintainX webhook rejected: no company secret matched")
        return None

    def _parse_signature_header(self, header):
        timestamp = None
        signature = None
        for element in (header or '').split(','):
            prefix, separator, value = element.strip().partition('=')
            if not separator:
                continue
            if prefix == 't':
                try:
                    timestamp = float(value)
                except ValueError:
                    return None, None
            elif prefix == 'v1':
                signature = value
        return timestamp, signature

    def _apply_status_change(self, payload, company):
        workorder_id = payload.get('workOrderId')
        new_status = str(payload.get('newStatus') or '').upper().strip()
        if not workorder_id or not new_status:
            return False

        Task = request.env['mes.task'].sudo()
        task = Task.search([
            ('maintainx_id', '=', str(workorder_id)),
            ('company_id', '=', company.id),
        ], limit=1)
        if not task:
            return False

        sub_status = str(payload.get('newSubStatus') or '').upper().strip()
        target_state = Task._STATUS_MAPPING.get(sub_status or new_status)
        if not target_state:
            return False

        client = Task._get_api_client(company)
        cache = {'employees': {}, 'machines': {}}
        actor_id = payload.get('userId')
        employee = Task._get_or_create_employee(str(actor_id), client, cache, company) if actor_id else False
        changed_at = Task._parse_date(payload.get('occurredAt'))

        if task.state != target_state:
            task.with_context(skip_auto_status_history=True,
                              skip_maintainx_push=True).write({'state': target_state})
        Task._create_status_history(task, sub_status or new_status,
                                    employee=employee, changed_at=changed_at)
        return True
