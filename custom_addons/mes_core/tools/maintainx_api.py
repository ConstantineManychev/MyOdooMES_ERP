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

    def _request(self, method, endpoint, **kwargs) -> Optional[Union[dict, list]]:
        try:
            url = f"{self.base_url}{endpoint}"
            response = requests.request(method, url, headers=self.headers, timeout=10, **kwargs)
            
            if response.status_code == 429:
                _logger.warning("MaintainX API Rate Limit Hit")
                return None
                
            response.raise_for_status()
            return response.json()
        except Exception as e:
            _logger.error(f"MaintainX API Error on {endpoint}: {e}")
            return None