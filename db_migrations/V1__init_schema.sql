CREATE TABLE IF NOT EXISTS config_machine (
    id SERIAL PRIMARY KEY,
    machine_name TEXT NOT NULL UNIQUE,
    ip_connection TEXT,
    ams_net_id TEXT,
    ip_data TEXT
);

CREATE TABLE IF NOT EXISTS config_signals (
    id SERIAL PRIMARY KEY,
    machine_name TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    poll_type TEXT,
    poll_frequency INT,
    param_type TEXT,
    signal_category TEXT,
    CONSTRAINT uniq_signal_per_machine UNIQUE (machine_name, tag_name)
);

CREATE TABLE IF NOT EXISTS telemetry_count (
    id BIGSERIAL, 
    time TIMESTAMPTZ NOT NULL,
    arrived_time TIMESTAMPTZ NOT NULL,
    machine_name TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    value BIGINT,
    CONSTRAINT uniq_count_time_machine_tag UNIQUE (time, machine_name, tag_name)
);
SELECT create_hypertable('telemetry_count', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS telemetry_event (
    id BIGSERIAL,
    time TIMESTAMPTZ NOT NULL,
    arrived_time TIMESTAMPTZ NOT NULL,
    machine_name TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    value INTEGER,
    CONSTRAINT uniq_event_time_machine_tag UNIQUE (time, machine_name, tag_name)
);
SELECT create_hypertable('telemetry_event', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS telemetry_process (
    id BIGSERIAL,
    time TIMESTAMPTZ NOT NULL,
    arrived_time TIMESTAMPTZ NOT NULL,
    machine_name TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    value DOUBLE PRECISION,
    value_str TEXT
);
SELECT create_hypertable('telemetry_process', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_telemetry_process_machine_tag ON telemetry_process (machine_name, tag_name, time DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_event_machine_tag ON telemetry_event (machine_name, tag_name, time DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_count_machine_tag ON telemetry_count (machine_name, tag_name, time DESC);

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_hourly_stats
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time) as bucket,
    machine_name,
    tag_name,
    COUNT(*) as total_events,
    AVG(value) as avg_val,
    MAX(value) as max_val,
    MIN(value) as min_val
FROM telemetry_count
GROUP BY bucket, machine_name, tag_name
WITH NO DATA;

CREATE OR REPLACE VIEW view_mes_anomalies AS
WITH last_hour AS (
    SELECT machine_name, COUNT(*) as stops_count 
    FROM telemetry_event 
    WHERE time > NOW() - INTERVAL '1 hour' AND value = 0 
    GROUP BY machine_name
),
monthly_avg AS (
    SELECT machine_name, COUNT(*) / 30.0 as avg_daily_stops 
    FROM telemetry_event 
    WHERE time > NOW() - INTERVAL '30 days' AND value = 0
    GROUP BY machine_name
)
SELECT 
    row_number() OVER () as id,
    lh.machine_name,
    lh.stops_count as current_stops,
    COALESCE(ma.avg_daily_stops, 0) as historical_avg,
    CASE 
        WHEN lh.stops_count > (ma.avg_daily_stops / 24 * 1.5) THEN 'critical'
        ELSE 'normal' 
    END as status
FROM last_hour lh
LEFT JOIN monthly_avg ma ON lh.machine_name = ma.machine_name;