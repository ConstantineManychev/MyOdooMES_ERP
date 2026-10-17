import json
import os
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timedelta

APP_DIR = os.path.dirname(os.path.abspath(__file__))
TABLES = {'count': 'telemetry_count', 'event': 'telemetry_event', 'process': 'telemetry_process'}
TS_FORMATS = ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S')
MARGIN = timedelta(days=1)


def parse_ts(text):
    for fmt in TS_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except (TypeError, ValueError):
            continue
    return None


def load_config():
    path = os.path.join(APP_DIR, 'config.json')
    try:
        with open(path, encoding='utf-8') as handle:
            return json.load(handle)
    except (OSError, ValueError):
        return {}


def main():
    if len(sys.argv) != 3:
        print("usage: python fix_misattributed_rows.py <name_sent_by_mistake> <real_machine_name>")
        print("example: python fix_misattributed_rows.py M9 M2")
        return 2
    wrong, right = sys.argv[1], sys.argv[2]
    if wrong == right:
        print("The two names must differ.")
        return 2

    cfg = load_config()
    if cfg.get('mac_name') and cfg.get('mac_name') != right:
        print(f"WARNING: config.json on this PC says mac_name={cfg.get('mac_name')!r}, "
              f"not {right!r}. Run this on the PC of machine {right}.")

    db_path = cfg.get('db_path') or os.path.join(APP_DIR, 'tx.sqlite')
    if not os.path.exists(db_path):
        print(f"Local buffer not found: {db_path}")
        return 1

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=30.0)
    try:
        rows = conn.execute(
            "SELECT tag_type, evt_id, ts FROM tx_log WHERE mac_name = ? AND evt_id IS NOT NULL",
            (wrong,)).fetchall()
    finally:
        conn.close()

    rows = [(tag_type, evt_id, ts) for tag_type, evt_id, ts in rows if tag_type in TABLES]
    if not rows:
        print(f"This PC holds no rows sent under the name {wrong!r}. Nothing to move.")
        return 0

    moments = [m for m in (parse_ts(ts) for _t, _e, ts in rows) if m]
    first, last = min(moments), max(moments)
    lower = (first - MARGIN).strftime('%Y-%m-%d %H:%M:%S')
    upper = (last + MARGIN).strftime('%Y-%m-%d %H:%M:%S')
    per_type = Counter(tag_type for tag_type, _e, _t in rows)

    out_path = os.path.join(APP_DIR, f"move_{wrong}_to_{right}.sql")
    with open(out_path, 'w', encoding='ascii', newline='\n') as out:
        out.write("\\set ON_ERROR_STOP on\n")
        out.write("BEGIN;\n")
        out.write("CREATE TEMP TABLE moved (evt_id varchar(64) PRIMARY KEY, tag_type text NOT NULL) "
                  "ON COMMIT DROP;\n")
        out.write("COPY moved (evt_id, tag_type) FROM STDIN;\n")
        seen = set()
        for tag_type, evt_id, _ts in rows:
            if evt_id in seen:
                continue
            seen.add(evt_id)
            out.write(f"{evt_id}\t{tag_type}\n")
        out.write("\\.\n")
        out.write("SELECT tag_type, count(*) AS rows_sent_by_this_pc FROM moved GROUP BY 1 ORDER BY 1;\n")
        for tag_type, table in TABLES.items():
            out.write(
                f"UPDATE {table} t SET machine_name = '{right}' FROM moved m "
                f"WHERE m.tag_type = '{tag_type}' AND t.evt_id = m.evt_id "
                f"AND t.machine_name = '{wrong}' "
                f"AND t.time >= '{lower}' AND t.time < '{upper}';\n")
        out.write("COMMIT;\n")

    print(f"Rows this PC sent as {wrong!r}: {len(seen)}  {dict(per_type)}")
    print(f"Local time span: {first:%Y-%m-%d %H:%M:%S} .. {last:%Y-%m-%d %H:%M:%S}")
    print(f"SQL written to: {out_path}")
    print("Copy it to the server and run from the project folder, for example in PowerShell:")
    print(f"  Get-Content move_{wrong}_to_{right}.sql | docker compose exec -T timescale_db "
          f"psql -U AbellioProduction -d telemetry_db")
    return 0


if __name__ == '__main__':
    sys.exit(main())
