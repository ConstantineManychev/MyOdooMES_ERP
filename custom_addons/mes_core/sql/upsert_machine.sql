INSERT INTO config_machine (machine_name, ip_connection, ip_data) 
VALUES (%s, %s, %s)
ON CONFLICT (machine_name) 
DO UPDATE SET 
    ip_connection = EXCLUDED.ip_connection, 
    ip_data = EXCLUDED.ip_data;