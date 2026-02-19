import os
import sys
import time
import pyads
import psycopg2
from psycopg2 import pool
from queue import Queue
from threading import Thread
from datetime import datetime

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')
DB_NAME = os.getenv('DB_NAME')

missing_envs = []
for var_name, var_value in [("DB_HOST", DB_HOST), ("DB_USER", DB_USER), ("DB_PASS", DB_PASS), ("DB_NAME", DB_NAME)]:
    if not var_value:
        missing_envs.append(var_name)

if missing_envs:
    print(f"CRITICAL ERROR: Missing required environment variables: {', '.join(missing_envs)}")
    sys.exit(1)

BASE_TICK_RATE = 0.1 
data_queue = Queue(maxsize=50000)

try:
    db_pool = psycopg2.pool.SimpleConnectionPool(
        1, 10, 
        host=DB_HOST, 
        port=DB_PORT,
        database=DB_NAME, 
        user=DB_USER, 
        password=DB_PASS
    )
    print(f"Connected to {DB_NAME} at {DB_HOST}:{DB_PORT}")
except Exception as e:
    print(f"CRITICAL ERROR: Database connection failed: {e}")
    sys.exit(1)

def db_writer_worker():
    conn = db_pool.getconn()
    conn.autocommit = True
    cursor = conn.cursor()
    
    while True:
        try:
            record = data_queue.get()
            category = record['category']
            machine_name = record['machine_name']
            
            if category == 'process':
                table_name = "telemetry_process"
            elif category == 'count':
                table_name = "telemetry_count"
            elif category == 'event':
                table_name = "telemetry_event"
            else:
                data_queue.task_done()
                continue

            query = f"INSERT INTO {table_name} (time, machine_name, tag_name, value) VALUES (%s, %s, %s, %s)"
            cursor.execute(query, (record['timestamp'], machine_name, record['tag_name'], record['value']))
            
            data_queue.task_done()
            
        except Exception as e:
            print(f"DB Write Error: {e}")
            time.sleep(1)

def load_all_configurations():
    conn = db_pool.getconn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT machine_name, ams_net_id, ip_connection FROM config_machine")
    machines = cursor.fetchall()
    
    configs = []
    for m in machines:
        m_name, ams_net_id, ip_conn = m[0], m[1], m[2]
        
        cursor.execute("""
            SELECT tag_name, poll_type, poll_frequency, signal_category 
            FROM config_signals 
            WHERE machine_name = %s
        """, (m_name,))
        
        signals = []
        for row in cursor.fetchall():
            signals.append({
                'tag_name': row[0],
                'poll_type': row[1], 
                'poll_frequency': row[2] / 1000.0 if row[2] else 1.0,
                'category': row[3],
                'last_poll_time': 0,
                'last_value': None
            })
            
        configs.append({
            'machine_name': m_name,
            'ams_net_id': ams_net_id,
            'ip_connection': ip_conn,
            'signals': signals
        })
        
    cursor.close()
    db_pool.putconn(conn)
    return configs

def machine_poller_worker(config):
    machine_name = config['machine_name']
    ams_net_id = config['ams_net_id']
    ip_connection = config['ip_connection']
    signals = config['signals']

    if not ams_net_id or not signals:
        return

    try:
        if ip_connection:
            plc = pyads.Connection(ams_net_id, pyads.PORT_TC3PLC1, ip_address=ip_connection)
        else:
            plc = pyads.Connection(ams_net_id, pyads.PORT_TC3PLC1)
        plc.open()
    except Exception as e:
        print(f"[{machine_name}] Connection failed: {e}")
        return

    symbols = {}
    for sig in signals:
        tag = sig['tag_name']
        try:
            symbols[tag] = plc.get_symbol(tag)
        except Exception:
            pass

    print(f"[{machine_name}] Polling {len(symbols)} tags (AMS: {ams_net_id}, IP: {ip_connection})")

    try:
        while True:
            current_time = time.time()
            timestamp_now = datetime.now()

            for sig in signals:
                tag = sig['tag_name']
                if tag not in symbols:
                    continue

                if sig['poll_type'] == 'cyclic':
                    if (current_time - sig['last_poll_time']) >= sig['poll_frequency']:
                        try:
                            value = symbols[tag].read()
                            sig['last_poll_time'] = current_time
                            
                            data_queue.put({
                                'timestamp': timestamp_now,
                                'machine_name': machine_name,
                                'tag_name': tag,
                                'value': value,
                                'category': sig['category']
                            })
                        except Exception:
                            pass

                elif sig['poll_type'] == 'on_change':
                    try:
                        value = symbols[tag].read()
                        if value != sig['last_value']:
                            sig['last_value'] = value
                            
                            data_queue.put({
                                'timestamp': timestamp_now,
                                'machine_name': machine_name,
                                'tag_name': tag,
                                'value': value,
                                'category': sig['category']
                            })
                    except Exception:
                        pass

            time.sleep(BASE_TICK_RATE)

    except Exception as e:
        print(f"[{machine_name}] Error: {e}")
    finally:
        for tag, sym in symbols.items():
            try: 
                sym.release_handle()
            except: 
                pass
        if plc.is_open:
            plc.close()

def main():
    writer_thread = Thread(target=db_writer_worker, daemon=True)
    writer_thread.start()
    
    try:
        configs = load_all_configurations()
    except Exception as e:
        print(f"Config load failed: {e}")
        sys.exit(1)
        
    worker_threads = []
    for config in configs:
        t = Thread(target=machine_poller_worker, args=(config,), daemon=True)
        t.start()
        worker_threads.append(t)
        
    if not worker_threads:
        return

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()