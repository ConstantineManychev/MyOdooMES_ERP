import time
import sqlite3
import pyads
import requests
import queue
import faulthandler
import signal
import subprocess
import threading
import logging
import logging.handlers
import traceback
import json
import os
import sys
import ctypes
import hashlib
import re
import base64
import shutil
import urllib3
from dataclasses import dataclass
from typing import List, Dict, Tuple, Any, Optional
from datetime import datetime, timedelta, timezone

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app_dir = os.path.dirname(os.path.abspath(__file__))
LOGGER_FILE = os.path.abspath(__file__)
UPDATE_MARKER_PATH = os.path.join(app_dir, 'update_pending.json')
UPDATE_FAILED_PATH = os.path.join(app_dir, 'update_failed.json')
CONFIG_PATH = os.path.join(app_dir, 'config.json')
CONFIG_MANAGED_PATH = os.path.join(app_dir, 'config_managed.json')
CONFIG_PENDING_PATH = os.path.join(app_dir, 'config_pending.json')
CONFIG_FAILED_PATH = os.path.join(app_dir, 'config_failed.json')
PROTECTED_CONFIG_KEYS = frozenset({
    'mac_name', 'plc_ip', 'plc_port', 'api_url', 'api_db', 'api_usr', 'api_pwd',
    'db_path', 'cache_path',
})
UPDATE_CHECK_MIN_SEC = 30.0
UPDATE_CHECK_MAX_SEC = 3600.0
COUNTER_STATE_PATH = os.path.join(app_dir, 'counter_state.json')
GAP_MARKER = '__ADS_GAP__'
DEFAULT_CUT_AFTER_SEC = 600.0
COUNTER_STATE_SAVE_SEC = 10.0
ADS_FRESH_MIN_SEC = 60.0


def read_json_file(path: str) -> Optional[Any]:
    try:
        with open(path, encoding='utf-8') as handle:
            return json.load(handle)
    except FileNotFoundError:
        return None


def write_json_atomic(path: str, data: Any):
    staged = path + '.new'
    with open(staged, 'w', encoding='utf-8') as handle:
        json.dump(data, handle, indent=4, ensure_ascii=False)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(staged, path)
UPDATE_EXIT_CODE = 75


def file_sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, 'rb') as handle:
        for chunk in iter(lambda: handle.read(65536), b''):
            digest.update(chunk)
    return digest.hexdigest()

_log_queue: "queue.SimpleQueue" = queue.SimpleQueue()
_heartbeat_path = os.path.join(app_dir, 'heartbeat.txt')


def write_heartbeat(payload: str):
    try:
        tmp = _heartbeat_path + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as fh:
            fh.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} {payload}\n")
        os.replace(tmp, _heartbeat_path)
    except Exception:
        pass


class SafeRotatingFileHandler(logging.handlers.RotatingFileHandler):

    def emit(self, record):
        try:
            super().emit(record)
        except Exception:
            write_heartbeat("LOG_EMIT_FAILED")

    def doRollover(self):
        try:
            super().doRollover()
        except Exception:
            write_heartbeat("LOG_ROLLOVER_FAILED")
            try:
                if self.stream is None:
                    self.stream = self._open()
            except Exception:
                pass


_crash_path = os.path.join(app_dir, 'crash.log')
try:
    _crash_file = open(_crash_path, 'a', encoding='utf-8', buffering=1)
    _crash_file.write(f"\n===== process start {datetime.now():%Y-%m-%d %H:%M:%S} pid={os.getpid()} =====\n")
    faulthandler.enable(file=_crash_file, all_threads=True)
except Exception:
    _crash_file = None

_file_handler = SafeRotatingFileHandler(
    os.path.join(app_dir, 'edge_node.log'),
    maxBytes=5 * 1024 * 1024, backupCount=5, encoding='utf-8', delay=True
)
_file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s'))
_root = logging.getLogger()
_root.setLevel(logging.INFO)
_root.addHandler(logging.handlers.QueueHandler(_log_queue))
_log_listener = logging.handlers.QueueListener(_log_queue, _file_handler, respect_handler_level=True)
_log_listener.start()
log = logging.getLogger('EdgeNode')


def _thread_excepthook(args):
    name = args.thread.name if args.thread else '?'
    trace = ''.join(traceback.format_exception(args.exc_type, args.exc_value, args.exc_traceback))
    log.critical(f"THREAD_CRASH [{name}]: {args.exc_value}\n{trace}")


threading.excepthook = _thread_excepthook


class LogGate:

    def __init__(self, window: float = 30.0):
        self.window = window
        self._sig: Dict[str, Tuple[float, int]] = {}
        self._mut = threading.Lock()

    def emit(self, level: str, sig: str, msg: Optional[str] = None):
        msg = msg or sig
        now = time.time()
        out = None
        with self._mut:
            if len(self._sig) > 500:
                cutoff = now - self.window
                self._sig = {k: v for k, v in self._sig.items() if v[0] >= cutoff}
            prev = self._sig.get(sig)
            if prev is None:
                self._sig[sig] = (now, 1)
                out = msg
            else:
                at, cnt = prev
                if now - at >= self.window:
                    self._sig[sig] = (now, 1)
                    out = msg + (f' (suppressed x{cnt - 1})' if cnt > 1 else '')
                else:
                    self._sig[sig] = (at, cnt + 1)
        if out is not None:
            getattr(log, level)(out)


gate = LogGate()


@dataclass
class SysCfg:
    mac_name: str
    plc_ip: str
    plc_port: int
    db_path: str
    retn_days: int
    cache_path: str
    api_url: str
    api_db: str
    api_usr: str
    api_pwd: str
    cfg_poll_min_sec: float = 5.0
    cfg_poll_max_sec: float = 300.0
    recovery_fail_window_sec: float = 120.0
    recovery_runtime_restart_threshold: int = 12
    recovery_runtime_restart_cooldown_sec: float = 150.0
    recovery_log_dedup_window_sec: float = 30.0
    recovery_runtime_restart_enabled: bool = False
    recovery_ads_restart_enabled: bool = True
    recovery_ipc_reboot_enabled: bool = False
    recovery_router_restart_threshold: int = 2
    recovery_ipc_reboot_threshold: int = 3
    recovery_os_action_cooldown_sec: float = 600.0
    recovery_ipc_reboot_grace_sec: int = 60
    recovery_odoo_alert_threshold: int = 3
    recovery_odoo_alert_cooldown_sec: float = 3600.0
    watchdog_interval_sec: float = 15.0
    watchdog_verify_reads: bool = True
    notif_mismatch_rebind_threshold: int = 2
    state_filter_max: float = 9.0
    health_interval_sec: float = 60.0
    watchdog_silence_threshold_sec: float = 120.0
    stall_threshold_sec: float = 300.0
    stall_restart_enabled: bool = True
    notif_max_delay_ms: int = 200
    notif_cycle_time_ms: int = 10
    tx_poison_threshold: int = 10
    pipeline_max_batch: int = 5000
    recovery_reboot_min_interval_sec: float = 1800.0
    recovery_reboot_backoff_factor: float = 2.0
    recovery_reboot_max_interval_sec: float = 21600.0
    recovery_max_reboots_per_day: int = 6
    self_update_enabled: bool = True
    update_check_interval_sec: float = 300.0
    pulse_interval_sec: float = 30.0


@dataclass
class TagCfg:
    tag_name: str
    type: str
    mode: str
    interval_sec: float = 1.0
    is_cumul: bool = False


@dataclass
class TxRec:
    ts: str
    mac_name: str
    tag_type: str
    tag_name: str
    val: str
    evt_id: str


@dataclass
class RawEvent:
    ts: float
    tag_name: str
    val: Any


def clean_val(val: Any) -> str:
    try:
        f = float(val)
        return str(int(f)) if f.is_integer() else f"{f:.4f}".rstrip('0').rstrip('.')
    except Exception:
        return str(val)


class CryptoHash:
    @staticmethod
    def gen_evt_id(ts: str, mac: str, tag: str, val: str) -> str:
        return hashlib.sha256(f"{ts}|{mac}|{tag}|{val}".encode()).hexdigest()

    @staticmethod
    def hash_cfg(data: List[Dict[str, Any]]) -> str:
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()


class TxRepo:

    def __init__(self, db_path: str, retn_days: int):
        self.db_path = db_path
        self.retn_days = retn_days
        self.mut = threading.Lock()
        self.conn = self._connect()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=60.0, isolation_level=None, check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        return conn

    def _reconnect(self):
        try:
            self.conn.close()
        except Exception:
            pass
        self.conn = self._connect()
        log.warning("DB_RECONNECTED")

    def _safe_rollback(self):
        try:
            if self.conn.in_transaction:
                self.conn.execute("ROLLBACK;")
        except Exception:
            pass

    def _write_tx(self, label: str, fn) -> bool:
        with self.mut:
            for attempt in (1, 2):
                try:
                    self.conn.execute("BEGIN IMMEDIATE;")
                    fn(self.conn)
                    self.conn.execute("COMMIT;")
                    return True
                except sqlite3.Error as e:
                    self._safe_rollback()
                    gate.emit('error', f"DB_ERR_{label}", f"DB_ERR_{label}: {e}")
                    if attempt == 1:
                        self._reconnect()
                except Exception as e:
                    self._safe_rollback()
                    log.error(f"DB_ERR_{label}: {e}")
                    return False
            return False

    def _init_schema(self):
        def _do(conn):
            conn.execute("""
                CREATE TABLE IF NOT EXISTS tx_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts TEXT, mac_name TEXT, tag_type TEXT, tag_name TEXT,
                    val TEXT, evt_id TEXT UNIQUE, sync_status INTEGER DEFAULT 0
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_sync_ts ON tx_log(sync_status, ts);")
        if not self._write_tx("INIT", _do):
            raise RuntimeError("DB_INIT_FAILED")

    def insert_batch(self, recs: List[TxRec]) -> bool:
        if not recs:
            return True
        rows = [(r.ts, r.mac_name, r.tag_type, r.tag_name, r.val, r.evt_id) for r in recs]

        def _do(conn):
            conn.executemany(
                "INSERT OR IGNORE INTO tx_log (ts, mac_name, tag_type, tag_name, val, evt_id) VALUES (?, ?, ?, ?, ?, ?)",
                rows
            )
        return self._write_tx("INSERT", _do)

    def get_pending(self, limit: int = 500) -> List[Tuple[int, TxRec]]:
        with self.mut:
            for attempt in (1, 2):
                try:
                    rows = self.conn.execute(
                        "SELECT id, ts, mac_name, tag_type, tag_name, val, evt_id FROM tx_log "
                        "WHERE sync_status = 0 ORDER BY ts ASC LIMIT ?",
                        (limit,)
                    ).fetchall()
                    return [(r[0], TxRec(*r[1:])) for r in rows]
                except sqlite3.Error as e:
                    gate.emit('error', "DB_ERR_PENDING", f"DB_ERR_PENDING: {e}")
                    if attempt == 1:
                        self._reconnect()
                except Exception as e:
                    log.error(f"DB_ERR_PENDING: {e}")
                    return []
            return []

    def _mark(self, ids: List[int], status: int, label: str) -> bool:
        if not ids:
            return True

        def _do(conn):
            conn.execute(
                f"UPDATE tx_log SET sync_status = ? WHERE id IN ({','.join('?' * len(ids))})",
                [status] + ids
            )
        return self._write_tx(label, _do)

    def count_pending(self) -> int:
        with self.mut:
            try:
                row = self.conn.execute(
                    "SELECT COUNT(*) FROM tx_log WHERE sync_status = 0"
                ).fetchone()
                return int(row[0]) if row else 0
            except Exception as e:
                log.error(f"DB_ERR_COUNT: {e}")
                return -1

    def mark_synced(self, ids: List[int]) -> bool:
        return self._mark(ids, 1, "MARK_SYNCED")

    def mark_poison(self, ids: List[int]) -> bool:
        return self._mark(ids, 2, "MARK_POISON")

    def purge_stale(self) -> bool:
        cutoff = (datetime.now() - timedelta(days=self.retn_days)).strftime('%Y-%m-%d %H:%M:%S')

        def _do(conn):
            conn.execute("DELETE FROM tx_log WHERE ts <= ? AND sync_status = 1", (cutoff,))
        return self._write_tx("PURGE", _do)

    def count_poisoned(self) -> int:
        with self.mut:
            try:
                row = self.conn.execute(
                    "SELECT COUNT(*) FROM tx_log WHERE sync_status = 2"
                ).fetchone()
                return int(row[0]) if row else 0
            except Exception as e:
                log.error(f"DB_ERR_COUNT_POISON: {e}")
                return -1

    def requeue_poisoned(self) -> int:
        pending = self.count_poisoned()
        if pending <= 0:
            return 0

        def _do(conn):
            conn.execute("UPDATE tx_log SET sync_status = 0 WHERE sync_status = 2")
        if self._write_tx("REQUEUE_POISON", _do):
            return pending
        return 0

    def oldest_pending_ts(self) -> Optional[str]:
        with self.mut:
            try:
                row = self.conn.execute("SELECT MIN(ts) FROM tx_log WHERE sync_status = 0").fetchone()
                return row[0] if row else None
            except Exception as e:
                log.error(f"DB_ERR_OLDEST: {e}")
                return None

    def last_values(self, tag_names: List[str]) -> Dict[str, float]:
        wanted = set(tag_names)
        if not wanted:
            return {}
        with self.mut:
            try:
                rows = self.conn.execute(
                    "SELECT tag_name, val, MAX(ts) FROM tx_log WHERE tag_type = 'count' GROUP BY tag_name"
                ).fetchall()
            except Exception as e:
                log.error(f"DB_ERR_LAST_VALUES: {e}")
                return {}
        found = {}
        for tag_name, val, _ts in rows:
            if tag_name not in wanted:
                continue
            try:
                found[tag_name] = float(val)
            except (TypeError, ValueError):
                continue
        return found

    def close(self):
        try:
            self.conn.close()
        except Exception:
            pass


class CounterGuard:

    def __init__(self, state: Optional[Dict[str, Any]]):
        state = state if isinstance(state, dict) else {}
        self.lock = threading.Lock()
        self.restored = bool(state)
        self.cut_after_sec = float(state.get('cut_after_sec') or DEFAULT_CUT_AFTER_SEC)
        self.ads_ok_at = float(state.get('ads_ok_at') or 0.0)
        self.cut_at = float(state.get('cut_at') or 0.0)
        self.tags: Dict[str, Dict[str, Any]] = {
            name: dict(item) for name, item in (state.get('tags') or {}).items() if isinstance(item, dict)}
        self.pending: Dict[str, float] = {}
        self.dirty = False

    def preload(self, baseline: Dict[str, float]):
        with self.lock:
            for name, value in baseline.items():
                if name not in self.tags:
                    self.tags[name] = {'raw': None, 'offset': 0.0, 'sent': float(value)}
                    self.dirty = True

    def open_gap(self, tag_names: List[str], now: float):
        with self.lock:
            for name in tag_names:
                self.pending[name] = now
            self.cut_at = now
            self.dirty = True

    def set_cut_after(self, seconds: float):
        with self.lock:
            if seconds > 0 and abs(seconds - self.cut_after_sec) >= 1.0:
                self.cut_after_sec = seconds
                self.dirty = True

    def note_ads_ok(self, moment: float):
        with self.lock:
            if moment > self.ads_ok_at:
                self.ads_ok_at = moment
                self.dirty = True

    def adjust(self, tag_name: str, cumulative: bool, value: Any, now: float) -> Any:
        try:
            raw = float(value)
        except (TypeError, ValueError):
            return value
        with self.lock:
            cutting = self.pending.pop(tag_name, None) is not None
            if not cumulative:
                return None if cutting else raw
            item = self.tags.get(tag_name)
            if item is None:
                self.tags[tag_name] = {'raw': raw, 'offset': 0.0}
                self.dirty = True
                return raw
            offset = float(item.get('offset') or 0.0)
            last_raw = item.get('raw')
            if last_raw is None:
                baseline = item.get('sent')
                offset = raw - float(baseline) if baseline is not None else 0.0
            elif cutting:
                offset = raw - (float(last_raw) - offset)
            elif raw - offset < 0:
                offset = 0.0
            self.tags[tag_name] = {'raw': raw, 'offset': offset}
            self.dirty = True
            return raw - offset

    def snapshot(self) -> Dict[str, Any]:
        with self.lock:
            self.dirty = False
            return {
                'cut_after_sec': self.cut_after_sec,
                'ads_ok_at': self.ads_ok_at,
                'cut_at': self.cut_at,
                'tags': {name: dict(item) for name, item in self.tags.items()},
            }


class ServerRejection(Exception):
    pass


class SessionExpired(Exception):
    pass


class TransientRejection(Exception):
    pass


SESSION_ERROR_MARKERS = (
    'sessionexpiredexception',
    'session expired',
    'odoo session expired',
)


def _is_session_error(error_obj) -> bool:
    try:
        data = error_obj.get('data') or {}
        name = str(data.get('name', '')).lower()
        if 'sessionexpired' in name:
            return True
        blob = f"{error_obj.get('message', '')} {data.get('message', '')}".lower()
        return any(marker in blob for marker in SESSION_ERROR_MARKERS)
    except Exception:
        return False


class MesGw:
    def __init__(self, cfg: SysCfg):
        self.cfg = cfg
        self.lock = threading.Lock()
        self.http = requests.Session()
        self.http.headers.update({
            'User-Agent': 'MES-Edge-Node/3.8',
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Connection': 'close'
        })
        self._authenticate()

    def _authenticate(self) -> bool:
        try:
            self.http.get(self.cfg.api_url, timeout=10, verify=False)
            payload = {
                "jsonrpc": "2.0", "method": "call",
                "params": {"db": self.cfg.api_db, "login": self.cfg.api_usr, "password": self.cfg.api_pwd}
            }
            res = self.http.post(f"{self.cfg.api_url.rstrip('/')}/web/session/authenticate", json=payload, timeout=15, verify=False)
            res.raise_for_status()
            log.info("AUTH_OK")
            return True
        except Exception as e:
            gate.emit('error', "AUTH_FAIL", f"AUTH_FAIL: {e}")
            return False

    def _exec_rpc(self, url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        res = self.http.post(url, json=payload, timeout=25, verify=False)
        res.raise_for_status()
        if 'text/html' in res.headers.get('Content-Type', ''):
            raise ValueError("RESP_FORMAT_ERR_HTML")
        data = res.json()
        if "error" in data:
            if _is_session_error(data['error']):
                raise SessionExpired(f"SESSION_EXPIRED: {data['error']}")
            raise ServerRejection(f"RPC_ERR: {data['error']}")
        return data

    def invoke(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.cfg.api_url}{endpoint}"
        payload = {"jsonrpc": "2.0", "method": "call", "params": params}
        with self.lock:
            try:
                data = self._exec_rpc(url, payload)
            except SessionExpired as e:
                gate.emit('warning', "SESSION_RENEW", f"SESSION_RENEW_TRIG: {e}")
                if not self._authenticate():
                    raise TransientRejection("AUTH_RECV_FAIL")
                try:
                    data = self._exec_rpc(url, payload)
                except SessionExpired as retry_error:
                    raise TransientRejection(f"SESSION_RENEW_FAILED: {retry_error}")
            except ServerRejection:
                raise
            except Exception as e:
                gate.emit('warning', "INVOKE_RETRY", f"INVOKE_RETRY_TRIG: {e}")
                if not self._authenticate():
                    raise TransientRejection("AUTH_RECV_FAIL")
                try:
                    data = self._exec_rpc(url, payload)
                except SessionExpired as retry_error:
                    raise TransientRejection(f"SESSION_RENEW_FAILED: {retry_error}")
        return data.get("result", {})

    def transmit(self, records: List[Tuple[int, TxRec]]):
        payload = {"events": [], "counts": [], "processes": []}
        check_map = {'event': 'events', 'count': 'counts', 'process': 'processes'}
        rx_map = {'event': 'events_rx', 'count': 'counts_rx', 'process': 'processes_rx'}
        counters = {'event': 0, 'count': 0, 'process': 0}
        for _, r in records:
            try:
                val = float(r.val) if r.tag_type == 'process' else int(float(r.val))
            except Exception:
                val = str(r.val) if r.tag_type == 'process' and r.val is not None else 0
            payload[check_map[r.tag_type]].append([r.ts, r.ts, r.mac_name, r.tag_name, val, r.evt_id])
            counters[r.tag_type] += 1
        if any(payload.values()):
            res = self.invoke("/mes/api/import_historical", payload)
            if res.get('status') == 'error':
                if res.get('transient'):
                    raise TransientRejection(f"IMPORT_TRANSIENT: {res.get('message')}")
                raise ServerRejection(f"IMPORT_ERR: {res.get('message')}")
            for t_type, p_key in rx_map.items():
                sent = counters[t_type]
                if not sent:
                    continue
                rx = res.get(p_key, 0)
                rej = res.get(p_key.replace('_rx', '_rej'), 0)
                if rej:
                    log.error(f"TX_ROWS_REJECTED [{t_type}]: {rej} row(s) dropped server-side as invalid")
                if rx + rej != sent:
                    raise ServerRejection(f"ACK_ERR_{t_type}")

    def report_missing_tags(self, tag_names: List[str]) -> bool:
        if not tag_names:
            return True
        try:
            result = self.invoke('/mes/api/logger/missing_tags', {
                'mac_name': self.cfg.mac_name,
                'tag_names': tag_names,
            })
        except Exception as exc:
            log.error(f"MISSING_TAGS_SEND_FAILED: {exc}")
            return False
        if result and result.get('ok'):
            log.warning(f"MISSING_TAGS_FLAGGED: {result.get('flagged')} of {tag_names}")
            return True
        log.error(f"MISSING_TAGS_REJECTED: {result}")
        return False

    def send_logger_status(self, evt_type: str, ts_utc: Optional[datetime] = None, err_msg: Optional[str] = None) -> bool:
        try:
            params = {
                'mac_name': self.cfg.mac_name,
                'evt_type': evt_type,
                'ts': (ts_utc or datetime.utcnow()).strftime('%Y-%m-%d %H:%M:%S'),
            }
            if err_msg:
                params['err_msg'] = err_msg[:500]
            res = self.invoke("/mes/api/logger/status", params)
            return res.get('status') == 'ok'
        except Exception as e:
            gate.emit('warning', "STATUS_SEND_FAIL", f"STATUS_SEND_FAIL [{evt_type}]: {e}")
            return False


class CfgMgr:
    def __init__(self, cfg: SysCfg, gw: MesGw):
        self.cfg = cfg
        self.gw = gw
        self.hash = ""
        self.tags: List[TagCfg] = []
        self._load_local()

    def _load_local(self):
        if not os.path.exists(self.cfg.cache_path):
            return
        try:
            with open(self.cfg.cache_path, "r") as f:
                raw = json.load(f)
                self.tags = [TagCfg(**t) for t in raw]
                self.hash = CryptoHash.hash_cfg(raw)
        except Exception as e:
            log.error(f"CFG_LOAD_ERR: {e}")

    def refresh(self) -> bool:
        try:
            raw = self.gw.invoke("/mes/api/get_machine_config", {"mac_name": self.cfg.mac_name}).get("tags", [])
            if not raw:
                return False
            new_hash = CryptoHash.hash_cfg(raw)
            if new_hash != self.hash:
                with open(self.cfg.cache_path, "w") as f:
                    json.dump(raw, f)
                self.tags = [TagCfg(**t) for t in raw]
                self.hash = new_hash
                return True
        except Exception as e:
            gate.emit('error', "CFG_REFRESH_ERR", f"CFG_REFRESH_ERR: {e}")
        return False


_RECOVERABLE_CODES = ('1861', '1873', '1808', '(6)', '29')
_TWINCAT_DEPS = ('TcAdsWcfHost', 'TcEventLogger', 'TcMcGst', 'TcNcI')


def _is_recoverable_err(err: str) -> bool:
    low = err.lower()
    return any(code in err for code in _RECOVERABLE_CODES) or 'timeout' in low or 'timed out' in low


class RecoveryManager:

    def __init__(self, cfg: SysCfg, gw: Optional[MesGw] = None):
        self.cfg = cfg
        self.gw = gw
        self.fail_timestamps: List[float] = []
        self.last_escalation_at = 0.0
        self.last_os_action_at = 0.0
        self.last_odoo_alert_at = 0.0
        self.rounds = 0
        self.consecutive_reboots = 0
        self.reboot_history: List[float] = []
        self.state_path = os.path.join(app_dir, 'recovery_state.json')
        self._load_state()

    def _load_state(self):
        try:
            with open(self.state_path, 'r', encoding='utf-8') as fh:
                st = json.load(fh)
            now = time.time()

            def _ts(key):
                try:
                    v = float(st.get(key, 0.0))
                except Exception:
                    return 0.0
                return v if 0 <= v <= now + 300 else 0.0

            self.last_escalation_at = _ts('last_escalation_at')
            self.last_os_action_at = _ts('last_os_action_at')
            self.last_odoo_alert_at = _ts('last_odoo_alert_at')
            self.rounds = max(0, int(st.get('rounds', 0)))
            self.consecutive_reboots = max(0, int(st.get('consecutive_reboots', 0)))
            self.reboot_history = sorted(
                t for t in (float(x) for x in st.get('reboot_history', []))
                if 0 < t <= now + 300
            )[-20:]
            if self.reboot_history and now - self.reboot_history[-1] < 900:
                log.warning(f"RECOVERY_STATE: started {now - self.reboot_history[-1]:.0f}s after our own "
                            f"IPC reboot; consecutive_reboots={self.consecutive_reboots} rounds={self.rounds}")
        except FileNotFoundError:
            pass
        except Exception as e:
            log.error(f"RECOVERY_STATE_LOAD_ERR: {e}")

    def _save_state(self):
        try:
            tmp = self.state_path + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as fh:
                json.dump({
                    'last_escalation_at': self.last_escalation_at,
                    'last_os_action_at': self.last_os_action_at,
                    'last_odoo_alert_at': self.last_odoo_alert_at,
                    'rounds': self.rounds,
                    'consecutive_reboots': self.consecutive_reboots,
                    'reboot_history': self.reboot_history[-20:],
                }, fh)
            os.replace(tmp, self.state_path)
        except Exception as e:
            gate.emit('error', 'RECOVERY_STATE_SAVE_ERR', f"RECOVERY_STATE_SAVE_ERR: {e}")

    @staticmethod
    def is_recoverable(err: str) -> bool:
        return _is_recoverable_err(err)

    def record_failure(self):
        now = time.time()
        cutoff = now - self.cfg.recovery_fail_window_sec
        self.fail_timestamps = [t for t in self.fail_timestamps if t >= cutoff]
        self.fail_timestamps.append(now)

    def record_success(self):
        self.fail_timestamps.clear()
        if self.rounds or self.consecutive_reboots:
            log.info(f"RECOVERY_OK: link restored after rounds={self.rounds} "
                     f"consecutive_reboots={self.consecutive_reboots}")
        self.rounds = 0
        self.consecutive_reboots = 0
        self._save_state()

    def should_escalate(self) -> bool:
        enough = len(self.fail_timestamps) >= self.cfg.recovery_runtime_restart_threshold
        cooled = time.time() - self.last_escalation_at >= self.cfg.recovery_runtime_restart_cooldown_sec
        return enough and cooled

    def _os_cooled(self) -> bool:
        return time.time() - self.last_os_action_at >= self.cfg.recovery_os_action_cooldown_sec

    def _alert_cooled(self) -> bool:
        return time.time() - self.last_odoo_alert_at >= self.cfg.recovery_odoo_alert_cooldown_sec

    @staticmethod
    def _extract_ipv4(addr: str) -> Optional[str]:
        if not addr:
            return None
        parts = addr.split('.')
        if len(parts) == 4:
            return addr
        if len(parts) == 6:
            return '.'.join(parts[:4])
        return None

    def _ping_plc(self) -> bool:
        host = self._extract_ipv4(self.cfg.plc_ip)
        if not host:
            return True
        try:
            args = ['ping', '-n', '2', '-w', '1000', host] if os.name == 'nt' else ['ping', '-c', '2', '-W', '1', host]
            return subprocess.run(args, capture_output=True, timeout=10).returncode == 0
        except Exception:
            return False

    def _run(self, args: List[str], label: str, timeout: int = 10, ok_codes: Tuple[int, ...] = (0,)) -> bool:
        log.warning(f'{label}_CMD: {" ".join(args)}')
        try:
            r = subprocess.run(args, capture_output=True, timeout=timeout)
            if r.returncode in ok_codes:
                if r.returncode != 0:
                    log.info(f'{label}_OK: exit={r.returncode} (accepted)')
                return True
            err = r.stderr.decode(errors='replace')[:200].strip()
            log.error(f'{label}_FAIL: exit={r.returncode} stderr={err}')
            return False
        except subprocess.TimeoutExpired:
            log.error(f'{label}_TIMEOUT: {timeout}s')
            return False
        except Exception as e:
            log.error(f'{label}_ERROR: {e}')
            return False

    def try_runtime_restart(self) -> bool:
        log.warning(f'RUNTIME_RESTART_ATTEMPT fails={len(self.fail_timestamps)}')
        try:
            conn = pyads.Connection(self.cfg.plc_ip, pyads.PORT_SYSTEMSERVICE)
            conn.open()
            try:
                conn.write_control(pyads.ADSSTATE_RESET, 0, 0, pyads.PLCTYPE_BYTE)
            finally:
                conn.close()
            log.info('RUNTIME_RESTART_OK')
            return True
        except Exception as e:
            log.error(f'RUNTIME_RESTART_FAIL: {e}')
            return False

    def _service_status(self, name: str) -> str:
        try:
            r = subprocess.run(['sc', 'query', name], capture_output=True, timeout=10)
            out = r.stdout.decode(errors='replace')
            for token in ('RUNNING', 'STOPPED', 'STOP_PENDING', 'START_PENDING'):
                if token in out:
                    return token
        except Exception:
            pass
        return 'UNKNOWN'

    def try_tcsys_restart(self) -> bool:
        self.last_os_action_at = time.time()
        self._save_state()
        log.warning('TCSYS_RESTART_ATTEMPT')

        self._run(['net', 'stop', 'TcSysSrv', '/y'], 'TCSYS_NETSTOP', timeout=90, ok_codes=(0, 2))
        stopped = False
        for _ in range(20):
            if self._service_status('TcSysSrv') == 'STOPPED':
                stopped = True
                break
            time.sleep(2.0)

        if not stopped:
            self._run(['sc', 'stop', 'TcSysSrv'], 'TCSYS_STOP', timeout=15)
            for _ in range(10):
                if self._service_status('TcSysSrv') == 'STOPPED':
                    stopped = True
                    break
                time.sleep(2.0)

        if not stopped:
            self._run(['taskkill', '/F', '/IM', 'TcSysSrv.exe'], 'TCSYS_KILL', timeout=10)
            for dep in _TWINCAT_DEPS:
                self._run(['taskkill', '/F', '/IM', f'{dep}.exe'], f'{dep}_KILL', timeout=5)
            time.sleep(5.0)
            stopped = self._service_status('TcSysSrv') == 'STOPPED'

        self._run(['sc', 'start', 'TcSysSrv'], 'TCSYS_START', timeout=30, ok_codes=(0, 1056))
        time.sleep(10.0)
        running = self._service_status('TcSysSrv') == 'RUNNING'
        for dep in _TWINCAT_DEPS:
            self._run(['sc', 'start', dep], f'{dep}_START', timeout=15, ok_codes=(0, 1056))

        if not running:
            log.error('TCSYS_RESTART_FAIL: service not RUNNING after restart attempt')
            return False
        if not stopped:
            gate.emit('warning', 'TCSYS_RESTART_NOOP',
                      'TCSYS_RESTART_NOOP: router could not be stopped (dependent services held it); '
                      'it was NOT actually cycled')
            return False
        log.info('TCSYS_RESTART_OK')
        return True

    def _reboot_allowed(self) -> Tuple[bool, str]:
        now = time.time()
        recent = [t for t in self.reboot_history if t >= now - 86400]
        if len(recent) >= self.cfg.recovery_max_reboots_per_day:
            return False, f'daily budget exhausted ({len(recent)}/{self.cfg.recovery_max_reboots_per_day} in 24h)'
        if self.reboot_history:
            interval = min(
                self.cfg.recovery_reboot_max_interval_sec,
                self.cfg.recovery_reboot_min_interval_sec *
                (self.cfg.recovery_reboot_backoff_factor ** max(0, self.consecutive_reboots - 1))
            )
            since = now - self.reboot_history[-1]
            if since < interval:
                return False, (f'backoff {since:.0f}/{interval:.0f}s after '
                               f'{self.consecutive_reboots} unsuccessful reboot(s)')
        return True, ''

    def try_ipc_reboot(self) -> bool:
        now = time.time()
        self.last_os_action_at = now
        self.reboot_history = (self.reboot_history + [now])[-20:]
        self.consecutive_reboots += 1
        self._save_state()
        grace = self.cfg.recovery_ipc_reboot_grace_sec
        log.warning(f'IPC_REBOOT_ATTEMPT grace={grace}s consecutive={self.consecutive_reboots} '
                    f'last_24h={len([t for t in self.reboot_history if t >= now - 86400])}')
        return self._run(
            ['shutdown', '/r', '/t', str(grace), '/c', 'MES auto-recovery: ADS unreachable'],
            'IPC_REBOOT', timeout=15
        )

    def try_odoo_alert(self) -> bool:
        if not self.cfg.api_url or self.gw is None:
            return False
        self.last_odoo_alert_at = time.time()
        self._save_state()
        msg = (f'Edge node recovery: PLC/ADS unreachable, escalation round {self.rounds}, '
               f'consecutive_reboots={self.consecutive_reboots}, ping_ok={self._ping_plc()}')
        log.warning(f'ODOO_ALERT_ATTEMPT: {msg}')
        ok = self.gw.send_logger_status('err', datetime.utcnow(), msg)
        log.info('ODOO_ALERT_OK' if ok else 'ODOO_ALERT_FAIL')
        return ok

    def run_escalation(self):
        self.last_escalation_at = time.time()
        self.rounds += 1
        n = self.rounds
        log.warning(f'ESCALATION round={n} ping_ok={self._ping_plc()} '
                    f'consecutive_reboots={self.consecutive_reboots}')

        router_th = self.cfg.recovery_router_restart_threshold
        reboot_th = self.cfg.recovery_ipc_reboot_threshold

        if n < router_th:
            if self.cfg.recovery_runtime_restart_enabled:
                self.try_runtime_restart()
            else:
                gate.emit('warning', 'RUNTIME_RESTART_DISABLED',
                          'RUNTIME_RESTART_DISABLED: skipping ADS runtime reset (no TwinCAT credentials)')
        elif n < reboot_th:
            if self.cfg.recovery_ads_restart_enabled and self._os_cooled():
                self.try_tcsys_restart()
            else:
                gate.emit('warning', 'TCSYS_SKIP',
                          f'TCSYS_SKIP enabled={self.cfg.recovery_ads_restart_enabled} os_cooled={self._os_cooled()}')
        else:
            if not self.cfg.recovery_ipc_reboot_enabled:
                gate.emit('warning', 'IPC_REBOOT_DISABLED', 'IPC_REBOOT_DISABLED')
            elif not self._os_cooled():
                gate.emit('warning', 'IPC_REBOOT_SKIP', 'IPC_REBOOT_SKIP: os action cooldown active')
            else:
                allowed, reason = self._reboot_allowed()
                if allowed:
                    self.try_ipc_reboot()
                else:
                    gate.emit('warning', 'IPC_REBOOT_HELD',
                              f'IPC_REBOOT_HELD: {reason}; reboots are not helping - '
                              f'PLC-side intervention required')

        if n >= self.cfg.recovery_odoo_alert_threshold and self._alert_cooled():
            self.try_odoo_alert()
        self._save_state()


class PlcNode:
    def __init__(self, cfg: SysCfg, raw_q: queue.Queue, gw: Optional[MesGw] = None,
                 guard: Optional[CounterGuard] = None):
        self.cfg = cfg
        self.raw_q = raw_q
        self.guard = guard
        self.started_at = time.time()
        self.ads_ok_at = guard.ads_ok_at if guard else 0.0
        self.ads_up_since = 0.0
        self._link_pending = False
        self._gap_marked = False
        self.plc = pyads.Connection(cfg.plc_ip, cfg.plc_port)
        self.cfg_tags: List[TagCfg] = []
        self.sym_map: Dict[str, Any] = {}
        self.notif_map: Dict[str, List[tuple]] = {}
        self.missing_tags: set = set()
        self.task_map: Dict[str, Tuple[TagCfg, float]] = {}
        self.is_connected = False
        self.needs_rebind = False
        self.recovery = RecoveryManager(cfg, gw)
        self.last_evt_at = 0.0
        self.last_notified: Dict[str, str] = {}
        self.dropped_total = 0
        self._wd_last = 0.0
        self._wd_rr = 0
        self._notif_mismatch_rounds = 0
        self._wd_verified_ok_at = 0.0

    def apply_cfg(self, tags: List[TagCfg]):
        self.cfg_tags = self._deduplicate_tags(tags)
        self.needs_rebind = True

    def _deduplicate_tags(self, tags: List[TagCfg]) -> List[TagCfg]:
        unique_tags: Dict[str, TagCfg] = {}
        duplicates = 0
        for t in tags:
            if t.tag_name in unique_tags:
                duplicates += 1
                continue
            unique_tags[t.tag_name] = t
        if duplicates:
            log.warning(f"CFG_DUPLICATE_TAGS: collapsed {duplicates} duplicate tag entries, "
                        f"{len(unique_tags)} unique tags remain")
        return list(unique_tags.values())

    def _log_dedup(self, message: str, level: str = 'error'):
        gate.emit(level, message.split(':')[0], message)

    def _recreate_connection(self):
        try:
            self.plc.close()
        except Exception:
            pass
        try:
            self.plc = pyads.Connection(self.cfg.plc_ip, self.cfg.plc_port)
            log.info("PLC_CONNECTION_RECREATED")
        except Exception as e:
            log.error(f"PLC_CONNECTION_RECREATE_FAIL: {e}")

    STRING_DEFAULT_SIZE = 81

    def _is_string_symbol(self, sym: Any) -> bool:
        names = f"{getattr(sym, 'symbol_type', '') or ''} {getattr(sym, 'plc_type', '') or ''}".upper()
        if 'WSTRING' in names:
            return False
        return 'STRING' in names or 'C_CHAR' in names

    def _string_size(self, sym: Any) -> int:
        type_name = str(getattr(sym, 'symbol_type', '') or '')
        match = re.search(r'STRING\s*\(\s*(\d+)\s*\)', type_name, re.IGNORECASE)
        if match:
            return int(match.group(1)) + 1
        size = getattr(sym, 'size', None)
        if isinstance(size, int) and size > 1:
            return size
        return self.STRING_DEFAULT_SIZE

    def _resolve_type(self, sym: Any) -> Tuple[int, Any]:
        if self._is_string_symbol(sym):
            return self._string_size(sym), pyads.PLCTYPE_STRING
        pt = str(sym.plc_type).upper().strip()
        type_map = {
            'BOOL': (1, ctypes.c_bool), 'BYTE': (1, ctypes.c_uint8),
            'WORD': (2, ctypes.c_uint16), 'DWORD': (4, ctypes.c_uint32),
            'LWORD': (8, ctypes.c_uint64), 'SINT': (1, ctypes.c_int8),
            'INT': (2, ctypes.c_int16), 'DINT': (4, ctypes.c_int32),
            'LINT': (8, ctypes.c_int64), 'USINT': (1, ctypes.c_uint8),
            'UINT': (2, ctypes.c_uint16), 'UDINT': (4, ctypes.c_uint32),
            'ULINT': (8, ctypes.c_uint64), 'REAL': (4, ctypes.c_float),
            'LREAL': (8, ctypes.c_double),
        }
        declared = str(getattr(sym, 'symbol_type', '') or '').upper().strip()
        if declared in type_map:
            return type_map[declared]
        if pt in type_map:
            return type_map[pt]
        sz = getattr(sym, 'size', getattr(sym, 'index_length', 4))
        if 'REAL' in pt or 'FLOAT' in pt:
            return sz, ctypes.c_float if sz == 4 else ctypes.c_double
        if 'BOOL' in pt:
            return sz, ctypes.c_bool
        return {1: (1, ctypes.c_uint8), 2: (2, ctypes.c_uint16), 8: (8, ctypes.c_uint64)}.get(sz, (4, ctypes.c_uint32))

    def _notification_timestamp(self, dt_stamp: Any) -> float:
        if not hasattr(dt_stamp, 'timestamp'):
            return time.time()
        if dt_stamp.tzinfo is None:
            dt_stamp = dt_stamp.replace(tzinfo=timezone.utc)
        return dt_stamp.timestamp()

    def _build_cb(self, tag_name: str, c_type: Any):
        def _cb(notif, _):
            try:
                _, dt_stamp, val = self.plc.parse_notification(notif, c_type)
                ts_val = self._notification_timestamp(dt_stamp)
                self.raw_q.put_nowait(RawEvent(ts_val, tag_name, val))
                self.last_evt_at = time.time()
                self.ads_ok_at = self.last_evt_at
                self.last_notified[tag_name] = clean_val(val)
            except queue.Full:
                self.dropped_total += 1
                gate.emit('warning', 'RAW_Q_FULL_CB', f"RAW_Q_FULL: dropping notification [{tag_name}]")
            except Exception as e:
                gate.emit('error', f"PLC_CB_ERR [{tag_name}]", f"PLC_CB_ERR [{tag_name}]: {e}")
        return _cb

    SYMBOL_MISSING_MARKERS = ('1808', 'symbol not found', 'ADSERR_DEVICE_SYMBOLNOTFOUND')

    def _is_symbol_missing(self, err_str):
        lowered = (err_str or '').lower()
        return any(marker.lower() in lowered for marker in self.SYMBOL_MISSING_MARKERS)

    def take_missing_tags(self):
        missing = sorted(self.missing_tags)
        self.missing_tags.clear()
        return missing

    def _purge_binds(self):
        try:
            is_alive = self.plc.is_open
        except Exception:
            is_alive = False
        if is_alive:
            for handles in list(self.notif_map.values()):
                for h_notif in handles:
                    try:
                        self.plc.del_device_notification(*h_notif)
                    except Exception:
                        pass
            for sym in list(self.sym_map.values()):
                try:
                    sym.release_handle()
                except Exception:
                    pass
        self.notif_map.clear()
        self.sym_map.clear()
        self.task_map.clear()
        self.last_notified.clear()
        self._notif_mismatch_rounds = 0

    def _drop_link(self, reason: str, level: str = 'warning'):
        self._log_dedup(f"PLC_LINK_DOWN: {reason}", level=level)
        self._purge_binds()
        self.is_connected = False
        self.ads_up_since = 0.0
        try:
            self.plc.close()
        except Exception:
            pass

    def _handle_ads_failure(self, err_str: str):
        if not self.recovery.is_recoverable(err_str):
            return
        self.recovery.record_failure()
        if self.recovery.should_escalate():
            self.recovery.run_escalation()
            self._recreate_connection()

    def _note_gap(self, now: float):
        if self._gap_marked or not self.ads_ok_at or self.guard is None:
            return
        down = now - self.ads_ok_at
        if down < self.guard.cut_after_sec:
            return
        try:
            self.raw_q.put_nowait(RawEvent(now, GAP_MARKER, down))
        except queue.Full:
            self.dropped_total += 1
            log.error("ADS_GAP_MARKER_DROPPED: raw queue full, counter jump will not be removed")
            return
        self._gap_marked = True
        log.warning(f"ADS_GAP: no PLC data for {down:.0f}s, counters will be re-based")

    def _mark_link_up(self, now: float):
        self.ads_ok_at = now
        self.ads_up_since = now
        self._link_pending = False
        self._gap_marked = False

    def ads_status(self, now: float) -> Tuple[bool, float, float]:
        fresh = max(ADS_FRESH_MIN_SEC, 4.0 * self.cfg.watchdog_interval_sec)
        if self.is_connected and self.ads_up_since and now - self.ads_ok_at <= fresh:
            return True, 0.0, max(0.0, now - self.ads_up_since)
        since = max(self.ads_ok_at, self.started_at)
        return False, max(0.0, now - since), 0.0

    def shutdown(self):
        try:
            self._purge_binds()
        except Exception:
            pass
        try:
            self.plc.close()
        except Exception:
            pass

    def _watchdog(self):
        if not self.is_connected:
            return
        now = time.time()
        if now - self._wd_last < self.cfg.watchdog_interval_sec:
            return
        self._wd_last = now

        try:
            state, _ = self.plc.read_state()
        except Exception as e:
            err_str = str(e)
            self._drop_link(f"watchdog read_state failed: {err_str}", level='error')
            self._handle_ads_failure(err_str)
            return

        if state != pyads.ADSSTATE_RUN:
            self._drop_link(f"watchdog: PLC state={state} (not RUN)")
            return
        self.ads_ok_at = time.time()

        if not (self.cfg.watchdog_verify_reads and self.notif_map):
            return

        tags = sorted(self.notif_map.keys())
        tag = tags[self._wd_rr % len(tags)]
        self._wd_rr += 1
        sym = self.sym_map.get(tag)
        if sym is None:
            return
        try:
            val = sym.read()
        except pyads.ADSError as e:
            err_str = str(e)
            self._log_dedup(f"WD_READ_ERR [{tag}]: {err_str}")
            if self.recovery.is_recoverable(err_str):
                self._drop_link(f"watchdog verify read failed: {err_str}", level='error')
                self._handle_ads_failure(err_str)
            return
        except Exception as e:
            log.error(f"WD_READ_ERR [{tag}]: {e}")
            return

        self.ads_ok_at = time.time()
        cv = clean_val(val)
        last = self.last_notified.get(tag)
        if last is None:
            self.last_notified[tag] = cv
            return
        if cv != last:
            self._notif_mismatch_rounds += 1
            log.warning(f"NOTIF_STALE [{tag}]: read={cv} last_notified={last} round={self._notif_mismatch_rounds}")
            try:
                self.raw_q.put_nowait(RawEvent(time.time(), tag, val))
                self.last_evt_at = time.time()
                self.last_notified[tag] = cv
            except queue.Full:
                self.dropped_total += 1
            if self._notif_mismatch_rounds >= self.cfg.notif_mismatch_rebind_threshold:
                log.error("NOTIF_DEAD: notifications considered stale, forcing rebind")
                self._notif_mismatch_rounds = 0
                self.needs_rebind = True
        else:
            self._notif_mismatch_rounds = 0
            self._wd_verified_ok_at = now

        silence_threshold = self.cfg.watchdog_silence_threshold_sec
        verified_recently = (now - self._wd_verified_ok_at) <= silence_threshold
        if self.last_evt_at > 0 and (now - self.last_evt_at) > silence_threshold \
                and not verified_recently:
            age = now - self.last_evt_at
            silent_tags = [t for t in self.notif_map if t not in self.last_notified]
            seen_tags = {t: self.last_notified[t] for t in self.notif_map if t in self.last_notified}
            gate.emit('error', 'NOTIF_SILENCE',
                      f"NOTIF_SILENCE: no events for {age:.0f}s (threshold={silence_threshold:.0f}s) | "
                      f"notif_tags={len(self.notif_map)} silent={silent_tags} last_seen={seen_tags} | "
                      f"forcing rebind")
            self._notif_mismatch_rounds = 0
            self.last_evt_at = time.time()
            self.needs_rebind = True

    def cycle(self):
        if not self.is_connected:
            try:
                self.plc.open()
                if hasattr(self.plc, 'set_timeout'):
                    try:
                        self.plc.set_timeout(5000)
                    except Exception:
                        pass
                state, _ = self.plc.read_state()
                if state != pyads.ADSSTATE_RUN:
                    self._log_dedup(f"PLC_NOT_READY state={state}", level='warning')
                    self.plc.close()
                    time.sleep(5.0)
                    return
                self.is_connected = True
                self.needs_rebind = True
                self._wd_last = time.time()
                self._link_pending = True
                self._note_gap(self._wd_last)
                self.recovery.record_success()
                log.info("PLC_LINK_UP")
            except Exception as e:
                err_str = str(e)
                self._log_dedup(f"PLC_OPEN_FAIL: {err_str}")
                self._handle_ads_failure(err_str)
                time.sleep(5.0)
                return

        if self.needs_rebind:
            self._purge_binds()
            self.needs_rebind = False
            bound = 0
            for t in self.cfg_tags:
                try:
                    sym = self.plc.get_symbol(t.tag_name)
                    self.sym_map[t.tag_name] = sym
                    if t.mode == 'on_change':
                        sz, c_type = self._resolve_type(sym)
                        notif_delay_sec = int(getattr(self.cfg, 'notif_max_delay_ms', 200)) / 1000.0
                        notif_cycle_sec = int(getattr(self.cfg, 'notif_cycle_time_ms', 10)) / 1000.0
                        attr = pyads.NotificationAttrib(length=sz, trans_mode=pyads.ADSTRANS_SERVERONCHA, max_delay=notif_delay_sec, cycle_time=notif_cycle_sec)
                        hndl = self.plc.add_device_notification(t.tag_name, attr, self._build_cb(t.tag_name, c_type))
                        self.notif_map.setdefault(t.tag_name, []).append(hndl)
                    elif t.mode == 'cyclic':
                        self.task_map[t.tag_name] = (t, 0.0)
                    bound += 1
                except Exception as e:
                    err_str = str(e)
                    log.error(f"TAG_BIND_FAIL [{t.tag_name}]: {err_str}")
                    if self._is_symbol_missing(err_str):
                        self.missing_tags.add(t.tag_name)
                    if any(code in err_str for code in ("1873", "1861", "(6)", "29")):
                        log.critical("FATAL_BIND_ERROR")
                        break
            if self.cfg_tags and bound == 0:
                log.warning(f"PLC_BIND_FAILED: 0/{len(self.cfg_tags)} tags bound. Dropping link.")
                self._drop_link("bind failed for all tags")
                time.sleep(1.0)
                return
            elif self.cfg_tags and bound != len(self.cfg_tags):
                log.warning(f"PLC_PARTIAL_BIND: {bound}/{len(self.cfg_tags)}")
                if self.missing_tags:
                    log.warning(f"PLC_MISSING_SYMBOLS: {sorted(self.missing_tags)}")
            else:
                log.info(f"PLC_BOUND: {bound}/{len(self.cfg_tags)} tags.")
            if self._link_pending:
                self._mark_link_up(time.time())

        now = time.time()
        for t_name, (t_cfg, last) in list(self.task_map.items()):
            if now - last < t_cfg.interval_sec:
                continue
            if self.raw_q.full():
                self.dropped_total += 1
                gate.emit('warning', 'RAW_Q_FULL_CYC', f"RAW_Q_FULL: dropping {t_name}")
                continue
            try:
                val = self.sym_map[t_name].read()
                self.raw_q.put_nowait(RawEvent(now, t_name, val))
                self.last_evt_at = time.time()
                self.ads_ok_at = self.last_evt_at
                self.task_map[t_name] = (t_cfg, now)
            except pyads.ADSError as e:
                err_str = str(e)
                self._log_dedup(f"PLC_READ_ERR [{t_name}]: {err_str}")
                if self.recovery.is_recoverable(err_str):
                    self._drop_link(f"cyclic read failed: {err_str}", level='error')
                    self._handle_ads_failure(err_str)
                    break
            except Exception as e:
                log.error(f"PLC_READ_ERR [{t_name}]: {e}")

        self._watchdog()


class RuntimeManager:
    def __init__(self, cfg: SysCfg):
        self.cfg = cfg
        self.term_evt = threading.Event()
        self.raw_q = queue.Queue(maxsize=100000)
        self.repo = TxRepo(cfg.db_path, cfg.retn_days)
        self.gw = MesGw(cfg)
        self.cfg_mgr = CfgMgr(cfg, self.gw)
        self.guard = self._load_counter_guard()
        self.plc = PlcNode(cfg, self.raw_q, self.gw, self.guard)
        self.tag_cache_lock = threading.Lock()
        self.tag_cache: Dict[str, TagCfg] = {}
        self.prev_vals: Dict[str, str] = {}
        self.prev_vals_lock = threading.Lock()
        self._pipe_batch: List[TxRec] = []
        self._last_emitted: Dict[str, str] = {}
        self.heartbeats: Dict[str, float] = {}
        self._stall_dumps = 0
        self.exit_code = 0
        self._reported_blocked_update = ''
        self._reported_blocked_config = ''
        try:
            self.own_sha256 = file_sha256(LOGGER_FILE)
        except Exception as e:
            self.own_sha256 = ''
            log.error(f"SELF_HASH_FAIL: {e}")
        log.info(f"LOGGER_VERSION sha256={self.own_sha256}")
        self._update_tag_cache(self.cfg_mgr.tags)
        self._report_startup_state()
        signal.signal(signal.SIGINT, lambda s, f: self.term_evt.set())
        signal.signal(signal.SIGTERM, lambda s, f: self.term_evt.set())
        if hasattr(signal, 'SIGBREAK'):
            signal.signal(signal.SIGBREAK, lambda s, f: self.term_evt.set())

    def _load_counter_guard(self) -> CounterGuard:
        try:
            state = read_json_file(COUNTER_STATE_PATH)
        except Exception as e:
            log.error(f"COUNTER_STATE_UNREADABLE: {e}")
            state = None
        guard = CounterGuard(state)
        if guard.restored:
            log.info(f"COUNTER_STATE_LOADED counters={len(guard.tags)} "
                     f"cut_after={guard.cut_after_sec:.0f}s")
            return guard
        cumulative = [t.tag_name for t in self.cfg_mgr.tags if t.type == 'count' and t.is_cumul]
        baseline = self.repo.last_values(cumulative)
        guard.preload(baseline)
        log.warning(f"COUNTER_STATE_MISSING: {len(baseline)} of {len(cumulative)} cumulative counter(s) "
                    f"continue from the last value sent by this PC")
        return guard

    def _save_counter_state(self):
        self.guard.note_ads_ok(self.plc.ads_ok_at)
        if not self.guard.dirty:
            return
        try:
            write_json_atomic(COUNTER_STATE_PATH, self.guard.snapshot())
        except Exception as e:
            gate.emit('error', 'COUNTER_STATE_SAVE_FAIL', f"COUNTER_STATE_SAVE_FAIL: {e}")

    def _open_counter_gap(self, down_sec: Any):
        with self.tag_cache_lock:
            counters = [name for name, t_cfg in self.tag_cache.items() if t_cfg.type == 'count']
        self.guard.open_gap(counters, time.time())
        log.warning(f"COUNTER_GAP_CUT: PLC unreadable for {float(down_sec):.0f}s, the first value of "
                    f"{len(counters)} counter(s) after reconnect becomes the new baseline")

    def _report_startup_state(self):
        pending = self.repo.count_pending()
        write_heartbeat(f"startup pid={os.getpid()} pending_records={pending}")
        log.info(f"STARTUP: pid={os.getpid()} pending_records={pending} tags={len(self.cfg_mgr.tags)}")
        if pending:
            log.warning(f"STARTUP_BACKLOG: {pending} record(s) collected but never transmitted")

    DATA_PATH_THREADS = ('PLC_IO', 'PIPE')
    BEAT_SLICE_SEC = 5.0

    def _beat(self, thread_name):
        self.heartbeats[thread_name] = time.time()

    def _wait_with_beat(self, thread_name, seconds):
        remaining = float(seconds)
        while remaining > 0 and not self.term_evt.is_set():
            slice_sec = min(self.BEAT_SLICE_SEC, remaining)
            if self.term_evt.wait(slice_sec):
                return True
            remaining -= slice_sec
            self._beat(thread_name)
        return self.term_evt.is_set()

    def _dump_stalled_threads(self, stalled):
        details = ", ".join(f"{name}={age:.0f}s" for name, age in sorted(stalled.items()))
        log.critical(f"THREAD_STALL detected: {details}")
        frames = sys._current_frames()
        for thread in threading.enumerate():
            frame = frames.get(thread.ident)
            if frame is None:
                continue
            stack = "".join(traceback.format_stack(frame))
            log.critical(f"THREAD_STACK [{thread.name}] alive={thread.is_alive()}\n{stack}")
        try:
            faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
        except Exception:
            pass

    def _ensure_log_listener(self):
        thread = getattr(_log_listener, '_thread', None)
        if thread is not None and thread.is_alive():
            return True
        write_heartbeat("LOG_LISTENER_DEAD restarting")
        try:
            _log_listener.start()
            log.critical("LOG_LISTENER_RESTARTED: log output had stopped silently")
            return True
        except Exception:
            write_heartbeat("LOG_LISTENER_RESTART_FAILED")
            return False

    def _write_state_heartbeat(self, now):
        ages = " ".join(f"{name}={now - beat:.0f}s"
                        for name, beat in sorted(self.heartbeats.items()))
        write_heartbeat(f"alive plc_connected={self.plc.is_connected} "
                        f"q={self.raw_q.qsize()} unflushed={len(self._pipe_batch)} {ages}")

    def loop_stall_guard(self):
        while not self.term_evt.wait(15.0):
            self._beat('STALL_GUARD')
            now = time.time()
            self._ensure_log_listener()
            self._write_state_heartbeat(now)
            threshold = self.cfg.stall_threshold_sec
            stalled = {name: now - beat for name, beat in list(self.heartbeats.items())
                       if name != 'STALL_GUARD' and beat and (now - beat) > threshold}
            if not stalled:
                self._stall_dumps = 0
                continue

            data_path_stalled = {n: a for n, a in stalled.items()
                                 if n in self.DATA_PATH_THREADS}
            if not data_path_stalled:
                log.warning("THREAD_SLOW (non critical): %s",
                            ", ".join(f"{n}={a:.0f}s" for n, a in sorted(stalled.items())))
                continue

            self._stall_dumps += 1
            write_heartbeat(f"THREAD_STALL {sorted(data_path_stalled)}")
            self._dump_stalled_threads(data_path_stalled)
            if self._stall_dumps >= 2 and self.cfg.stall_restart_enabled:
                log.critical("THREAD_STALL persists, terminating process for a clean restart")
                try:
                    self.gw.send_logger_status('err', datetime.utcnow(),
                                               f"Logger stalled: {sorted(data_path_stalled)}")
                except Exception:
                    pass
                write_heartbeat("EXIT_ON_STALL code=3")
                logging.shutdown()
                os._exit(3)

    def _update_tag_cache(self, tags):
        with self.tag_cache_lock:
            self.tag_cache = {t.tag_name: t for t in tags}

    def _eval_drift(self, t_name: str, val_str: str) -> bool:
        with self.prev_vals_lock:
            if self.prev_vals.get(t_name) == val_str:
                return False
            self.prev_vals[t_name] = val_str
            return True

    def _passes_drift_filter(self, t_cfg: TagCfg, t_name: str, val_str: str) -> bool:
        if t_cfg.mode == 'on_change':
            if self._last_emitted.get(t_name) == val_str:
                return False
            self._last_emitted[t_name] = val_str
            return True
        return self._eval_drift(t_name, val_str)

    def loop_plc(self):
        self.plc.apply_cfg(self.cfg_mgr.tags)
        try:
            while not self.term_evt.is_set():
                self._beat('PLC_IO')
                try:
                    self.plc.cycle()
                except Exception as e:
                    log.error(f"LOOP_PLC_CRASH: {e}\n{traceback.format_exc()}")
                    time.sleep(1.0)
                time.sleep(0.01)
        finally:
            if self.term_evt.is_set():
                self.plc.shutdown()

    def _flush_pipe_batch(self) -> bool:
        if not self._pipe_batch:
            return True
        if self.repo.insert_batch(self._pipe_batch):
            self._pipe_batch.clear()
            return True
        overflow = len(self._pipe_batch) - self.cfg.pipeline_max_batch
        if overflow > 0:
            del self._pipe_batch[:overflow]
            log.critical(f"PIPE_OVERFLOW: dropped {overflow} oldest unflushed records (sqlite unavailable)")
        return False

    def _stage_raw(self, raw: RawEvent) -> bool:
        with self.tag_cache_lock:
            t_cfg = self.tag_cache.get(raw.tag_name)
        if not t_cfg:
            gate.emit('warning', f"PIPE_TAG_MISS: {raw.tag_name}")
            return False
        val_s = clean_val(raw.val)
        if raw.tag_name == 'OEE.nMachineState':
            try:
                if float(val_s) > self.cfg.state_filter_max:
                    gate.emit('warning', 'STATE_FILTERED',
                              f"STATE_FILTERED: nMachineState={val_s} > {self.cfg.state_filter_max}, dropped")
                    return False
            except Exception:
                pass
        if t_cfg.type == 'count':
            adjusted = self.guard.adjust(raw.tag_name, t_cfg.is_cumul, raw.val, time.time())
            if adjusted is None:
                self._passes_drift_filter(t_cfg, raw.tag_name, val_s)
                return False
            val_s = clean_val(adjusted)
        if not self._passes_drift_filter(t_cfg, raw.tag_name, val_s):
            return False
        ts_s = datetime.fromtimestamp(raw.ts).strftime('%Y-%m-%d %H:%M:%S.%f')
        eid = CryptoHash.gen_evt_id(ts_s, self.cfg.mac_name, raw.tag_name, val_s)
        self._pipe_batch.append(TxRec(ts_s, self.cfg.mac_name, t_cfg.type, raw.tag_name, val_s, eid))
        return t_cfg.type == 'event'

    def loop_pipeline(self):
        last_flush = time.time()
        last_state_save = time.time()
        drain_deadline = None
        while True:
            self._beat('PIPE')
            if self.term_evt.is_set():
                if drain_deadline is None:
                    drain_deadline = time.time() + 10.0
                if self.raw_q.empty() or time.time() > drain_deadline:
                    self._flush_pipe_batch()
                    self._save_counter_state()
                    return
            force_flush = False
            try:
                raw = self.raw_q.get(timeout=0.5)
                if raw.tag_name == GAP_MARKER:
                    self._open_counter_gap(raw.val)
                else:
                    force_flush = self._stage_raw(raw)
            except queue.Empty:
                pass
            except Exception as e:
                log.error(f"PIPE_ERR: {e}\n{traceback.format_exc()}")
            if time.time() - last_state_save >= COUNTER_STATE_SAVE_SEC:
                self._save_counter_state()
                last_state_save = time.time()
            try:
                if len(self._pipe_batch) >= 500 or force_flush or (self._pipe_batch and time.time() - last_flush >= 5.0):
                    if self._flush_pipe_batch():
                        last_flush = time.time()
                    else:
                        self.term_evt.wait(1.0)
            except Exception as e:
                log.error(f"PIPE_FLUSH_ERR: {e}\n{traceback.format_exc()}")
                self.term_evt.wait(1.0)

    def loop_tx(self):
        last_purge = time.time()
        poison_head: Optional[int] = None
        poison_count = 0
        transient_streak = 0
        while not self.term_evt.is_set():
            self._beat('TX')
            try:
                if time.time() - last_purge > 3600:
                    self.repo.purge_stale()
                    last_purge = time.time()
                batch = self.repo.get_pending(500)
                if not batch:
                    self.term_evt.wait(2.0)
                    continue
                head_id = batch[0][0]
                try:
                    self.gw.transmit(batch)
                    self.repo.mark_synced([r[0] for r in batch])
                    log.info(f"TX_OK: {len(batch)} records")
                    poison_head, poison_count = None, 0
                    transient_streak = 0
                except TransientRejection as e:
                    poison_head, poison_count = None, 0
                    transient_streak += 1
                    gate.emit('warning', 'TX_TRANSIENT',
                              f"TX_TRANSIENT (streak {transient_streak}): {e}")
                    self.term_evt.wait(min(60.0, 5.0 * min(transient_streak, 12)))
                except ServerRejection as e:
                    transient_streak = 0
                    if head_id == poison_head:
                        poison_count += 1
                    else:
                        poison_head, poison_count = head_id, 1
                    log.error(f"TX_REJECT ({poison_count}/{self.cfg.tx_poison_threshold}): {e}")
                    if poison_count >= self.cfg.tx_poison_threshold:
                        ids = [r[0] for r in batch]
                        self.repo.mark_poison(ids)
                        log.critical(f"TX_POISON: quarantined {len(ids)} records (head id={head_id}), sync_status=2")
                        poison_head, poison_count = None, 0
                    self.term_evt.wait(5.0)
                except Exception as e:
                    poison_head, poison_count = None, 0
                    transient_streak += 1
                    gate.emit('error', 'TX_FAIL', f"TX_FAIL (streak {transient_streak}): {e}")
                    self.term_evt.wait(min(60.0, 5.0 * min(transient_streak, 12)))
            except Exception as e:
                log.error(f"LOOP_TX_CRASH: {e}\n{traceback.format_exc()}")
                self.term_evt.wait(5.0)

    def loop_cfg(self):
        curr = self.cfg.cfg_poll_min_sec
        while not self.term_evt.is_set():
            self._beat('CFG')
            try:
                missing = self.plc.take_missing_tags()
                if missing:
                    self.gw.report_missing_tags(missing)
                if self.cfg_mgr.refresh() and self.cfg_mgr.tags:
                    self._update_tag_cache(self.cfg_mgr.tags)
                    self.plc.apply_cfg(self.cfg_mgr.tags)
                    curr = self.cfg.cfg_poll_min_sec
                else:
                    curr = min(self.cfg.cfg_poll_max_sec, curr * 1.5)
            except Exception as e:
                log.error(f"LOOP_CFG_CRASH: {e}\n{traceback.format_exc()}")
                curr = min(self.cfg.cfg_poll_max_sec, curr * 1.5)
            self._wait_with_beat('CFG', curr)

    def loop_health(self):
        last_sent_evt = 0.0
        was_connected = False
        last_stats = time.time()
        while not self.term_evt.is_set():
            self._beat('HEALTH')
            try:
                if self.plc.is_connected and not was_connected:
                    self.gw.send_logger_status('conn')
                was_connected = self.plc.is_connected

                ts_evt = self.plc.last_evt_at
                if ts_evt and ts_evt != last_sent_evt:
                    if self.gw.send_logger_status('plc_recv', datetime.utcfromtimestamp(ts_evt)):
                        last_sent_evt = ts_evt

                if time.time() - last_stats >= 300:
                    age = (time.time() - self.plc.last_evt_at) if self.plc.last_evt_at else -1
                    log.info(f"STATS connected={self.plc.is_connected} q={self.raw_q.qsize()} "
                             f"dropped={self.plc.dropped_total} last_evt_age_sec={age:.0f} "
                             f"unflushed={len(self._pipe_batch)}")
                    last_stats = time.time()
            except Exception as e:
                log.error(f"LOOP_HEALTH_CRASH: {e}\n{traceback.format_exc()}")
            self._wait_with_beat('HEALTH', self.cfg.health_interval_sec)

    def loop_pulse(self):
        gw: Optional[MesGw] = None
        while not self.term_evt.is_set():
            self._beat('PULSE')
            try:
                if gw is None:
                    gw = MesGw(self.cfg)
                self._send_pulse(gw)
            except Exception as e:
                gate.emit('warning', 'PULSE_FAIL', f"PULSE_FAIL: {e}")
            if self._wait_with_beat('PULSE', max(5.0, self.cfg.pulse_interval_sec)):
                return

    def _send_pulse(self, gw: MesGw):
        now = time.time()
        ads_ok, down_sec, up_sec = self.plc.ads_status(now)
        backlog = self.repo.count_pending()
        oldest = self.repo.oldest_pending_ts() if backlog > 0 else None
        backlog_age = None
        if oldest:
            try:
                backlog_age = max(0.0, now - datetime.strptime(oldest[:26], '%Y-%m-%d %H:%M:%S.%f').timestamp())
            except ValueError:
                backlog_age = None
        cut_at = self.guard.cut_at
        res = gw.invoke("/mes/api/logger/pulse", {
            'mac_name': self.cfg.mac_name,
            'clock': now,
            'ads_ok': ads_ok,
            'ads_down_sec': round(down_sec),
            'ads_up_sec': round(up_sec),
            'backlog': backlog if backlog >= 0 else None,
            'backlog_age_sec': round(backlog_age) if backlog_age is not None else None,
            'uptime_sec': round(now - self.plc.started_at),
            'cut_ago_sec': round(now - cut_at) if cut_at else None,
        })
        if res.get('status') != 'ok':
            gate.emit('warning', 'PULSE_REJECTED', f"PULSE_REJECTED: {res}")
            return
        limit = res.get('cut_after_sec')
        if limit:
            self.guard.set_cut_after(float(limit))

    def loop_update(self):
        if self._wait_with_beat('UPDATE', 60.0):
            return
        while not self.term_evt.is_set():
            self._beat('UPDATE')
            try:
                self._check_for_update()
            except Exception as e:
                log.error(f"UPDATE_CHECK_FAIL: {e}")
            interval = min(max(float(self.cfg.update_check_interval_sec), UPDATE_CHECK_MIN_SEC),
                           UPDATE_CHECK_MAX_SEC)
            if self._wait_with_beat('UPDATE', interval):
                return

    def _check_for_update(self):
        res = self.gw.invoke("/mes/api/logger/update_check", {
            "mac_name": self.cfg.mac_name,
            "sha256": self.own_sha256,
            "config_hash": self._applied_config_hash(),
        })
        if res.get('status') != 'ok':
            log.warning(f"UPDATE_CHECK_REJECTED: {res}")
            return
        self._confirm_running_version()
        self._confirm_running_config()

        config_changed = False
        if 'config' in res and res.get('config_hash'):
            config_changed = self._apply_config(res.get('config') or {}, str(res['config_hash']))

        update = res.get('update')
        if (self.cfg.self_update_enabled and self.own_sha256 and update
                and update.get('sha256') != self.own_sha256):
            self._apply_update(update)
            if self.term_evt.is_set():
                return
        if config_changed:
            self._request_restart("SELF_CONFIG_APPLIED")

    def _request_restart(self, reason: str):
        log.critical(f"{reason}, restarting to apply")
        self.exit_code = UPDATE_EXIT_CODE
        self.term_evt.set()

    def _applied_config_hash(self) -> str:
        try:
            managed = read_json_file(CONFIG_MANAGED_PATH) or {}
            return str(managed.get('hash') or '')
        except Exception as e:
            log.error(f"CONFIG_MANAGED_READ_FAIL: {e}")
            return ''

    def _confirm_running_config(self):
        try:
            pending = read_json_file(CONFIG_PENDING_PATH)
        except Exception as e:
            log.error(f"CONFIG_PENDING_READ_FAIL: {e}")
            pending = None
        if pending is None:
            return
        try:
            write_json_atomic(CONFIG_MANAGED_PATH, {'hash': pending.get('hash', ''),
                                                    'keys': pending.get('keys', [])})
            os.remove(CONFIG_PENDING_PATH)
            log.info(f"SELF_CONFIG_CONFIRMED hash={str(pending.get('hash', ''))[:8]}")
        except Exception as e:
            log.error(f"SELF_CONFIG_CONFIRM_FAIL: {e}")

    def _config_rolled_back(self, config_hash: str) -> bool:
        try:
            failed = read_json_file(CONFIG_FAILED_PATH) or {}
            return failed.get('hash') == config_hash
        except Exception:
            return False

    def _apply_config(self, config: Dict[str, Any], config_hash: str) -> bool:
        if self._config_rolled_back(config_hash):
            if self._reported_blocked_config != config_hash:
                log.error(f"SELF_CONFIG_SKIPPED {config_hash[:8]}: rolled back on this machine before")
                self.gw.send_logger_status(
                    'err', datetime.utcnow(),
                    "Logger configuration from Odoo failed here and was rolled back; "
                    "correct it in the Logger Config tab")
                self._reported_blocked_config = config_hash
            return False

        wanted = {k: v for k, v in config.items() if k not in PROTECTED_CONFIG_KEYS}
        ignored = sorted(set(config) - set(wanted))
        if ignored:
            log.error(f"SELF_CONFIG_PROTECTED_KEYS_IGNORED: {ignored}")

        try:
            current = read_json_file(CONFIG_PATH)
        except Exception as e:
            log.error(f"SELF_CONFIG_LOCAL_UNREADABLE, not touching config.json: {e}")
            return False
        if current is None:
            current = {}
        if not isinstance(current, dict):
            log.error("SELF_CONFIG_LOCAL_INVALID, not touching config.json")
            return False

        managed = read_json_file(CONFIG_MANAGED_PATH) or {}
        previously_managed = set(managed.get('keys') or [])
        updated = dict(current)
        for key in previously_managed - set(wanted):
            if key not in PROTECTED_CONFIG_KEYS:
                updated.pop(key, None)
        updated.update(wanted)

        record = {'hash': config_hash, 'keys': sorted(wanted)}
        if updated == current:
            write_json_atomic(CONFIG_MANAGED_PATH, record)
            log.info(f"SELF_CONFIG_IN_SYNC hash={config_hash[:8]}")
            return False

        if os.path.exists(CONFIG_PATH):
            shutil.copy2(CONFIG_PATH, CONFIG_PATH + '.bak')
        write_json_atomic(CONFIG_PENDING_PATH, record)
        write_json_atomic(CONFIG_PATH, updated)
        added = sorted(set(wanted) - set(current))
        removed = sorted(set(current) - set(updated))
        changed = sorted(k for k in wanted if k in current and current[k] != wanted[k])
        log.warning(f"SELF_CONFIG_WRITTEN hash={config_hash[:8]} "
                    f"added={added} changed={changed} removed={removed}")
        return True

    def _confirm_running_version(self):
        if not os.path.exists(UPDATE_MARKER_PATH):
            return
        try:
            os.remove(UPDATE_MARKER_PATH)
            log.info(f"SELF_UPDATE_CONFIRMED sha256={self.own_sha256}")
        except Exception as e:
            log.error(f"SELF_UPDATE_CONFIRM_FAIL: {e}")

    def _is_rolled_back(self, target: str) -> bool:
        try:
            with open(UPDATE_FAILED_PATH, encoding='utf-8') as handle:
                return json.load(handle).get('to') == target
        except Exception:
            return False

    def _apply_update(self, update: Dict[str, Any]):
        target = str(update.get('sha256') or '')
        if self._is_rolled_back(target):
            if self._reported_blocked_update != target:
                log.error(f"SELF_UPDATE_SKIPPED {target[:8]}: rolled back on this machine before")
                self.gw.send_logger_status(
                    'err', datetime.utcnow(),
                    f"Logger update {update.get('name')} failed here and was rolled back; "
                    f"publish a fixed version")
                self._reported_blocked_update = target
            return
        log.warning(f"SELF_UPDATE_AVAILABLE {update.get('name')} sha256={target}")
        res = self.gw.invoke("/mes/api/logger/download",
                             {"mac_name": self.cfg.mac_name, "sha256": target})
        if res.get('status') != 'ok':
            log.error(f"SELF_UPDATE_DOWNLOAD_REJECTED: {res.get('msg')}")
            return
        data = base64.b64decode(res.get('content') or '')
        actual = hashlib.sha256(data).hexdigest()
        if not data or actual != target:
            log.error(f"SELF_UPDATE_HASH_MISMATCH expected={target} actual={actual}")
            return
        try:
            compile(data, LOGGER_FILE, 'exec')
        except SyntaxError as e:
            log.error(f"SELF_UPDATE_SYNTAX_ERROR: {e}")
            return

        staged = LOGGER_FILE + '.new'
        with open(staged, 'wb') as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        shutil.copy2(LOGGER_FILE, LOGGER_FILE + '.bak')
        with open(UPDATE_MARKER_PATH, 'w', encoding='utf-8') as handle:
            json.dump({'from': self.own_sha256, 'to': target,
                       'applied_at': datetime.utcnow().isoformat()}, handle)
        os.replace(staged, LOGGER_FILE)

        self._request_restart(f"SELF_UPDATE_APPLIED {self.own_sha256[:8]} -> {target[:8]}")

    def run(self):
        try:
            requeued = self.repo.requeue_poisoned()
            if requeued:
                log.critical(f"REQUEUE_POISON: returned {requeued} quarantined records to the send queue")
        except Exception as e:
            log.error(f"REQUEUE_POISON_FAIL: {e}")

        specs = {
            "PLC_IO": self.loop_plc,
            "PIPE": self.loop_pipeline,
            "TX": self.loop_tx,
            "CFG": self.loop_cfg,
            "HEALTH": self.loop_health,
            "PULSE": self.loop_pulse,
            "STALL_GUARD": self.loop_stall_guard,
        }
        specs["UPDATE"] = self.loop_update
        threads: Dict[str, threading.Thread] = {}

        def _start(name):
            t = threading.Thread(target=specs[name], name=name)
            t.start()
            return t

        for name in specs:
            threads[name] = _start(name)

        while not self.term_evt.wait(5.0):
            for name, t in list(threads.items()):
                if not t.is_alive():
                    log.critical(f"THREAD_DEAD [{name}] - restarting")
                    threads[name] = _start(name)

        for t in threads.values():
            t.join(timeout=15.0)
        self.repo.close()
        log.info("SHUTDOWN_COMPLETE")


def main():
    cfg_path = os.path.join(app_dir, 'config.json')
    raw: Dict[str, Any] = {}
    if os.path.exists(cfg_path):
        try:
            with open(cfg_path, encoding='utf-8') as fh:
                raw = json.load(fh)
        except Exception as e:
            log.critical(f"CONFIG_PARSE_FAIL: {e}")
    else:
        log.warning(f"CONFIG_MISSING: {cfg_path}, using env/defaults")

    def val(key: str, env_key: str, default: Any) -> Any:
        return raw.get(key, os.getenv(env_key, default))

    cfg = SysCfg(
        mac_name=val('mac_name', 'MAC_NAME', 'M7'),
        plc_ip=val('plc_ip', 'PLC_IP', '192.168.2.1.1.1'),
        plc_port=int(val('plc_port', 'PLC_PORT', 851)),
        db_path=val('db_path', 'DB_PATH', os.path.join(app_dir, 'tx.sqlite')),
        retn_days=int(val('retn_days', 'RETN_DAYS', 30)),
        cache_path=val('cache_path', 'CACHE_PATH', os.path.join(app_dir, 'cfg.json')),
        api_url=val('api_url', 'API_URL', ''),
        api_db=val('api_db', 'API_DB', 'Abellio_Odoo'),
        api_usr=val('api_usr', 'API_USR', 'Telemetry'),
        api_pwd=val('api_pwd', 'API_PWD', ''),
        recovery_runtime_restart_enabled=bool(raw.get('recovery_runtime_restart_enabled', False)),
        recovery_ads_restart_enabled=bool(raw.get('recovery_ads_restart_enabled', True)),
        recovery_ipc_reboot_enabled=bool(raw.get('recovery_ipc_reboot_enabled', False)),
        recovery_fail_window_sec=float(raw.get('recovery_fail_window_sec', 120.0)),
        recovery_runtime_restart_threshold=int(raw.get('recovery_runtime_restart_threshold', 12)),
        recovery_runtime_restart_cooldown_sec=float(raw.get('recovery_runtime_restart_cooldown_sec', 150.0)),
        recovery_router_restart_threshold=int(raw.get('recovery_router_restart_threshold', 2)),
        recovery_ipc_reboot_threshold=int(raw.get('recovery_ipc_reboot_threshold', 3)),
        recovery_os_action_cooldown_sec=float(raw.get('recovery_os_action_cooldown_sec', 600.0)),
        recovery_ipc_reboot_grace_sec=int(raw.get('recovery_ipc_reboot_grace_sec', 60)),
        recovery_log_dedup_window_sec=float(raw.get('recovery_log_dedup_window_sec', 30.0)),
        recovery_odoo_alert_threshold=int(raw.get('recovery_odoo_alert_threshold', 3)),
        recovery_odoo_alert_cooldown_sec=float(raw.get('recovery_odoo_alert_cooldown_sec', 3600.0)),
        watchdog_interval_sec=float(raw.get('watchdog_interval_sec', 15.0)),
        watchdog_verify_reads=bool(raw.get('watchdog_verify_reads', True)),
        notif_mismatch_rebind_threshold=int(raw.get('notif_mismatch_rebind_threshold', 2)),
        state_filter_max=float(raw.get('state_filter_max', 9.0)),
        health_interval_sec=float(raw.get('health_interval_sec', 60.0)),
        watchdog_silence_threshold_sec=float(raw.get('watchdog_silence_threshold_sec', 120.0)),
        stall_threshold_sec=float(raw.get('stall_threshold_sec', 300.0)),
        stall_restart_enabled=bool(raw.get('stall_restart_enabled', True)),
        notif_max_delay_ms=int(raw.get('notif_max_delay_ms', 200)),
        notif_cycle_time_ms=int(raw.get('notif_cycle_time_ms', 10)),
        tx_poison_threshold=int(raw.get('tx_poison_threshold', 10)),
        pipeline_max_batch=int(raw.get('pipeline_max_batch', 5000)),
        recovery_reboot_min_interval_sec=float(raw.get('recovery_reboot_min_interval_sec', 1800.0)),
        recovery_reboot_backoff_factor=float(raw.get('recovery_reboot_backoff_factor', 2.0)),
        recovery_reboot_max_interval_sec=float(raw.get('recovery_reboot_max_interval_sec', 21600.0)),
        recovery_max_reboots_per_day=int(raw.get('recovery_max_reboots_per_day', 6)),
        self_update_enabled=bool(raw.get('self_update_enabled', True)),
        update_check_interval_sec=float(raw.get('update_check_interval_sec', 300.0)),
        pulse_interval_sec=float(raw.get('pulse_interval_sec', 30.0)),
    )
    gate.window = cfg.recovery_log_dedup_window_sec

    exit_code = 0
    while True:
        try:
            runtime = RuntimeManager(cfg)
            runtime.run()
            exit_code = runtime.exit_code
            break
        except KeyboardInterrupt:
            break
        except Exception as e:
            log.critical(f"FATAL_RESTART: {e}\n{traceback.format_exc()}")
            time.sleep(10.0)

    try:
        _log_listener.stop()
    except Exception:
        pass
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
