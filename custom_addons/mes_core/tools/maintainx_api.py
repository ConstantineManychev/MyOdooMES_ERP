import requests
import logging
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

class MaintainXClient:
    def __init__(self, token):
        self.base_url = "https://api.getmaintainx.com/v1"
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

    def get_workorders(self, limit=200):
        return self._request("GET", "/workorders", params={'limit': limit})

    def get_entity(self, area, entity_id):
        return self._request("GET", f"/{area}/{entity_id}")

    def _request(self, method, endpoint, **kwargs):
        try:
            response = requests.request(method, f"{self.base_url}{endpoint}", headers=self.headers, timeout=10, **kwargs)
            if response.status_code == 429:
                return {"error": "rate_limit", "retry_after": 30}
            response.raise_for_status()
            return response.json()
        except Exception as e:
            _logger.error(f"MaintainX API Error: {e}")
            return None