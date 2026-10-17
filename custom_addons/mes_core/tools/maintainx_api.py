import json
import logging
import requests
from typing import List, Optional, TypedDict, Any, Union

_logger = logging.getLogger(__name__)

class MaintainXUser(TypedDict):
    id: int
    email: str
    firstName: str
    lastName: str

class MaintainXAsset(TypedDict):
    id: int
    name: str
    parentId: Optional[int]

class MaintainXWorkOrder(TypedDict):
    id: int
    title: str
    description: str
    status: str
    priority: str
    assetId: Optional[int]
    assigneeIds: List[int]
    createdAt: str
    updatedAt: str

class MaintainXClient:
    LOG_BODY_LIMIT = 1000

    def __init__(self, token):
        self.base_url = "https://api.getmaintainx.com/v1"
        self.headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }

    def get_workorders(self, limit=200) -> List[MaintainXWorkOrder]:
        data = self._request("GET", "/workorders", params={'limit': limit})
        if not data:
            return []
        return data.get('workOrders') or data.get('items') or []

    def get_user(self, user_id: str) -> Optional[MaintainXUser]:
        data = self._request("GET", f"/users/{user_id}")
        if not data:
            return None
        return data.get('user', data)

    def get_asset(self, asset_id: str) -> Optional[MaintainXAsset]:
        data = self._request("GET", f"/assets/{asset_id}")
        if not data:
            return None
        return data.get('asset', data)

    def get_workorder(self, workorder_id: str) -> Optional[MaintainXWorkOrder]:
        data = self._request("GET", f"/workorders/{workorder_id}")
        if not data:
            return None
        return data.get('workOrder', data)

    def create_workorder(self, payload: dict) -> Optional[MaintainXWorkOrder]:
        data = self._request("POST", "/workorders", json=payload)
        if not data:
            return None
        return data.get('workOrder', data)

    def update_workorder_status(self, workorder_id: str, status: str) -> Optional[MaintainXWorkOrder]:
        data = self._request("PATCH", f"/workorders/{workorder_id}/status", json={'status': status})
        if not data:
            return None
        return data.get('workOrder', data)

    def update_workorder_description(self, workorder_id: str, description: str) -> Optional[dict]:
        data = self._request("PATCH", f"/workorders/{workorder_id}",
                             json={'description': description})
        if not data:
            return None
        return data.get('workOrder', data)

    def update_workorder_assignees(self, workorder_id: str, assignees: List[dict]) -> Optional[dict]:
        data = self._request("PATCH", f"/workorders/{workorder_id}", json={'assignees': assignees})
        if not data:
            return None
        return data.get('workOrder', data)

    def get_workorder_expanded(self, workorder_id: str) -> Optional[dict]:
        params = [('expand', 'times'), ('expand', 'time_items'), ('expand', 'assignees')]
        data = self._request("GET", f"/workorders/{workorder_id}", params=params)
        if not data:
            return None
        return data.get('workOrder', data)

    def create_workorder_comment(self, workorder_id: str, content: str) -> Optional[dict]:
        return self._request("POST", f"/workorders/{workorder_id}/comments",
                             json={'content': content})

    def get_workorder_comments(self, workorder_id: str, limit: int = 200) -> List[dict]:
        comments = []
        cursor = None
        while True:
            params = {'limit': limit}
            if cursor:
                params['cursor'] = cursor
            data = self._request("GET", f"/workorders/{workorder_id}/comments", params=params)
            if not data:
                break
            page = data.get('comments') or []
            comments.extend(page)
            cursor = data.get('nextCursor')
            if not cursor or not page:
                break
        return comments

    def get_assets(self, limit: int = 200, max_pages: int = 100) -> List[dict]:
        assets = []
        cursor = None
        pages = 0
        while pages < max_pages:
            params = {'limit': limit}
            if cursor:
                params['cursor'] = cursor
            data = self._request("GET", "/assets", params=params)
            if not data:
                break
            page = data.get('assets') if isinstance(data, dict) else data
            page = page or []
            assets.extend(page)
            pages += 1
            cursor = data.get('nextCursor') if isinstance(data, dict) else None
            if not cursor or not page:
                break
        _logger.info("MaintainX assets fetched: %s in %s page(s)", len(assets), pages)
        return assets

    @staticmethod
    def asset_parent_id(asset: dict) -> str:
        parent = asset.get('parentId')
        if parent is None:
            nested = asset.get('parent')
            if isinstance(nested, dict):
                parent = nested.get('id')
        return str(parent) if parent not in (None, '') else ''

    def get_child_assets(self, parent_asset_id: str, limit: int = 200) -> List[dict]:
        target = str(parent_asset_id)
        assets = self.get_assets(limit=limit)
        children = [a for a in assets if self.asset_parent_id(a) == target]
        _logger.info("MaintainX children of asset %s: %s of %s total",
                     target, len(children), len(assets))
        return children

    def _request(self, method, endpoint, **kwargs) -> Optional[Union[dict, list]]:
        url = f"{self.base_url}{endpoint}"
        payload = kwargs.get('json')
        try:
            response = requests.request(method, url, headers=self.headers, timeout=10, **kwargs)
        except Exception as exc:
            _logger.error("MaintainX %s %s failed to send: %s", method, endpoint, exc)
            return None

        body_preview = (response.text or '')[:self.LOG_BODY_LIMIT]
        _logger.info("MaintainX %s %s -> HTTP %s | request=%s | response=%s",
                     method, endpoint, response.status_code,
                     self._safe_payload(payload), body_preview)

        if response.status_code == 429:
            _logger.warning("MaintainX rate limit on %s, limit=%s remaining=%s reset=%ss",
                            endpoint,
                            response.headers.get('X-Rate-Limit-Limit'),
                            response.headers.get('X-Rate-Limit-Remaining'),
                            response.headers.get('X-Rate-Limit-Reset'))
            return None

        if response.status_code == 204:
            return {}

        if response.status_code >= 400:
            _logger.error("MaintainX %s %s rejected with HTTP %s: %s",
                          method, endpoint, response.status_code, body_preview)
            return None

        try:
            return response.json()
        except ValueError:
            _logger.error("MaintainX %s %s returned non-JSON body: %s",
                          method, endpoint, body_preview)
            return None

    def _safe_payload(self, payload):
        if payload is None:
            return '-'
        try:
            return json.dumps(payload)[:self.LOG_BODY_LIMIT]
        except Exception:
            return str(payload)[:self.LOG_BODY_LIMIT]