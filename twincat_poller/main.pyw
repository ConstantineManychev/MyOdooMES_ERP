import os
import time
import pyads
import psycopg2
from psycopg2 import pool
from queue import Queue
from threading import Thread
from datetime import datetime

MACHINE_NAME = os.getenv('MACHINE_NAME', 'Machine_1')
DB_HOST = os.getenv('DB_HOST', 'timescaledb')
DB_USER = os.getenv('DB_USER', 'telemetry_user')
DB_PASS = os.getenv('DB_PASS', 'timescale_strong_password')
DB_NAME = os.getenv('DB_NAME', 'telemetry')

BASE_TICK_RATE = 0.5 

data_queue = Queue(maxsize=10000)

db_pool = psycopg2.pool.SimpleConnectionPool(1, 5, host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)

def db_writer_worker():
    conn = db_pool.getconn()
    conn.autocommit = True
    cursor = conn.cursor()
    
    print("DB Writer thread started.")
    while True:
        try:
            record = data_queue.get()
            
            table_name = ""
            category = record['category']
            
            if category == 'process':
                table_name = "telemetry_process"
            elif category == 'count':
                table_name = "telemetry_count"
            elif category == 'event':
                table_name = "telemetry_event"
            else:
                print(f"Unknown category {category}, skipping.")
                data_queue.task_done()
                continue

            query = f"INSERT INTO {table_name} (time, machine_name, tag_name, value) VALUES (%s, %s, %s, %s)"
            cursor.execute(query, (record['timestamp'], MACHINE_NAME, record['tag_name'], record['value']))
            
            data_queue.task_done()
            
        except Exception as e:
            print(f"DB Write Error: {e}")
            time.sleep(1)

def load_configuration():
    conn = db_pool.getconn()
    cursor = conn.cursor()
    
    cursor.execute("SELECT ams_net_id FROM config_machine WHERE machine_name = %s", (MACHINE_NAME,))
    machine_row = cursor.fetchone()
    if not machine_row:
        raise ValueError(f"Machine {MACHINE_NAME} not found in config_machine table.")
    ams_net_id = machine_row[0]
    
    cursor.execute("""
        SELECT tag_name, poll_type, poll_frequency, signal_category 
        FROM config_signals 
        WHERE machine_name = %s
    """, (MACHINE_NAME,))
    
    signals = []
    for row in cursor.fetchall():
        signals.append({
            'tag_name': row[0],
            'poll_type': row[1], 
            'poll_frequency': row[2],    
            'category': row[3],
            'last_poll_time': 0,
            'last_value': None
        })
        
    cursor.close()
    db_pool.putconn(conn)
    return ams_net_id, signals

def main():
    print(f"Starting telemetry worker for {MACHINE_NAME}...")
    
    writer_thread = Thread(target=db_writer_worker, daemon=True)
    writer_thread.start()
    
    ams_net_id, signals = load_configuration()
    print(f"Loaded {len(signals)} tags for NetID {ams_net_id}")
    if not signals:
        return

    plc = pyads.Connection(ams_net_id, pyads.PORT_TC3PLC1)
    
    symbols = {}

    try:
        plc.open()
        
        for sig in signals:
            tag = sig['tag_name']
            try:
                symbols[tag] = plc.get_symbol(tag)
            except Exception as e:
                print(f"Failed to load tag {tag}: {e}")

        while True:
            current_time = time.time()
            timestamp_now = datetime.now()

            for sig in signals:
                tag = sig['tag_name']
                if tag not in symbols:
                    continue

                if sig['poll_type'] == 'periodic':
                    if (current_time - sig['last_poll_time']) >= sig['poll_frequency']:
                        try:
                            value = symbols[tag].read()
                            sig['last_poll_time'] = current_time
                            
                            data_queue.put({
                                'timestamp': timestamp_now,
                                'tag_name': tag,
                                'value': value,
                                'category': sig['category']
                            })
                        except Exception as e:
                            print(f"Read error for {tag}: {e}")

                elif sig['poll_type'] == 'on_change':
                    try:
                        value = symbols[tag].read()
                        if value != sig['last_value']:
                            sig['last_value'] = value
                            
                            data_queue.put({
                                'timestamp': timestamp_now,
                                'tag_name': tag,
                                'value': value,
                                'category': sig['category']
                            })
                    except Exception as e:
                        print(f"Read error for {tag}: {e}")

            time.sleep(BASE_TICK_RATE)

    except KeyboardInterrupt:
        print("Stopping script...")
    finally:
        for tag, sym in symbols.items():
            try:
                sym.release_handle()
            except:
                pass
        if plc.is_open:
            plc.close()

if __name__ == "__main__":
    main()