import json
import logging
import logging.handlers
import os
import sys
import time

import requests
from smartcard.CardMonitoring import CardMonitor, CardObserver
from smartcard.CardConnection import CardConnection
from smartcard.util import toHexString

app_dir = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(app_dir, 'config.json')
GET_UID_APDU = [0xFF, 0xCA, 0x00, 0x00, 0x00]
DECIMAL_UID_LENGTH = 10
RESCAN_GUARD_SEC = 1.5

log = logging.getLogger('fob_bridge')
log.setLevel(logging.INFO)
_handler = logging.handlers.RotatingFileHandler(
    os.path.join(app_dir, 'fob_bridge.log'),
    maxBytes=2 * 1024 * 1024, backupCount=3, encoding='utf-8')
_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
log.addHandler(_handler)
log.addHandler(logging.StreamHandler(sys.stdout))


def build_uid_candidates(uid_bytes):
    if not uid_bytes:
        return []
    hex_forward = ''.join(f'{b:02X}' for b in uid_bytes)
    hex_reversed = ''.join(f'{b:02X}' for b in reversed(uid_bytes))
    candidates = []
    for hex_value in (hex_reversed, hex_forward):
        try:
            candidates.append(str(int(hex_value, 16)).zfill(DECIMAL_UID_LENGTH))
        except ValueError:
            pass
    candidates.append(hex_forward)
    seen = []
    for value in candidates:
        if value and value not in seen:
            seen.append(value)
    return seen


class OdooGateway:

    def __init__(self, cfg):
        self.cfg = cfg
        self.http = requests.Session()
        self.http.verify = False
        self.authenticated = False

    def authenticate(self):
        payload = {'jsonrpc': '2.0', 'params': {
            'db': self.cfg['api_db'],
            'login': self.cfg['api_usr'],
            'password': self.cfg['api_pwd'],
        }}
        url = f"{self.cfg['api_url'].rstrip('/')}/web/session/authenticate"
        response = self.http.post(url, json=payload, timeout=15)
        response.raise_for_status()
        body = response.json()
        if body.get('result', {}).get('uid'):
            self.authenticated = True
            log.info("AUTH_OK")
            return True
        log.error(f"AUTH_FAILED: {body.get('error')}")
        return False

    def send_scan(self, uid, alternatives):
        if not self.authenticated and not self.authenticate():
            return False
        payload = {'jsonrpc': '2.0', 'params': {
            'mac_name': self.cfg['mac_name'],
            'uid': uid,
            'alternatives': alternatives,
        }}
        url = f"{self.cfg['api_url'].rstrip('/')}/mes/api/kiosk/fob_scan"
        try:
            response = self.http.post(url, json=payload, timeout=15)
            response.raise_for_status()
            result = response.json().get('result') or {}
        except Exception as exc:
            log.error(f"SEND_FAILED: {exc}")
            self.authenticated = False
            return False

        if result.get('ok'):
            log.info(f"SCAN_{result.get('action', '').upper()}: "
                     f"{result.get('employee_name')} ({result.get('role')})")
        else:
            log.warning(f"SCAN_REJECTED: uid={uid} {result.get('error')}")
        return bool(result.get('ok'))


class CardReadObserver(CardObserver):

    def __init__(self, gateway):
        self.gateway = gateway
        self.last_uid = ''
        self.last_at = 0.0

    def update(self, observable, actions):
        added, _removed = actions
        for card in added:
            uid_bytes = self._read_uid(card)
            if not uid_bytes:
                continue
            candidates = build_uid_candidates(uid_bytes)
            if not candidates:
                continue
            now = time.time()
            if candidates[0] == self.last_uid and now - self.last_at < RESCAN_GUARD_SEC:
                continue
            self.last_uid, self.last_at = candidates[0], now
            log.info(f"CARD_READ: {toHexString(uid_bytes)} -> {candidates}")
            self.gateway.send_scan(candidates[0], candidates)

    def _read_uid(self, card):
        connection = card.createConnection()
        try:
            connection.connect(CardConnection.T0_protocol | CardConnection.T1_protocol)
            data, sw1, sw2 = connection.transmit(GET_UID_APDU)
            if (sw1, sw2) != (0x90, 0x00):
                log.warning(f"UID_APDU_FAILED: SW={sw1:02X}{sw2:02X}")
                return None
            return data
        except Exception as exc:
            log.warning(f"CARD_CONNECT_FAILED: {exc}")
            return None
        finally:
            try:
                connection.disconnect()
            except Exception:
                pass


def load_config():
    with open(CONFIG_PATH, encoding='utf-8') as handle:
        cfg = json.load(handle)
    required = ('mac_name', 'api_url', 'api_db', 'api_usr', 'api_pwd')
    missing = [key for key in required if not cfg.get(key)]
    if missing:
        raise RuntimeError(f"Config incomplete, missing: {', '.join(missing)}")
    return cfg


def main():
    requests.packages.urllib3.disable_warnings()
    cfg = load_config()
    log.info(f"BRIDGE_START machine={cfg['mac_name']} target={cfg['api_url']}")

    gateway = OdooGateway(cfg)
    gateway.authenticate()

    monitor = CardMonitor()
    observer = CardReadObserver(gateway)
    monitor.addObserver(observer)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        monitor.deleteObserver(observer)
        log.info("BRIDGE_STOP")


if __name__ == '__main__':
    main()
