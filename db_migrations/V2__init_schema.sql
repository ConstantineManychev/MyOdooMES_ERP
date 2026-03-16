
DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.compression_settings WHERE hypertable_name = 'telemetry_count') THEN
        ALTER TABLE telemetry_count SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'machine_name, tag_name',
            timescaledb.compress_orderby = 'time DESC'
        );
    END IF;
END $$;
SELECT add_compression_policy('telemetry_count', INTERVAL '30 days', if_not_exists => TRUE);

DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.compression_settings WHERE hypertable_name = 'telemetry_event') THEN
        ALTER TABLE telemetry_event SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'machine_name, tag_name',
            timescaledb.compress_orderby = 'time DESC'
        );
    END IF;
END $$;
SELECT add_compression_policy('telemetry_event', INTERVAL '30 days', if_not_exists => TRUE);

DO $$ 
BEGIN 
    IF NOT EXISTS (SELECT 1 FROM timescaledb_information.compression_settings WHERE hypertable_name = 'telemetry_process') THEN
        ALTER TABLE telemetry_process SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'machine_name, tag_name',
            timescaledb.compress_orderby = 'time DESC'
        );
    END IF;
END $$;
SELECT add_compression_policy('telemetry_process', INTERVAL '30 days', if_not_exists => TRUE);