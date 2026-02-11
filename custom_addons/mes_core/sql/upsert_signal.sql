INSERT INTO config_signals (machine_name, tag_name, poll_type, poll_frequency, param_type, signal_category)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (machine_name, tag_name) 
DO UPDATE SET 
    poll_type = EXCLUDED.poll_type, 
    poll_frequency = EXCLUDED.poll_frequency, 
    param_type = EXCLUDED.param_type, 
    signal_category = EXCLUDED.signal_category;