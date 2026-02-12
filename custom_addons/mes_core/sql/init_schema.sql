CREATE TABLE IF NOT EXISTS config_machine (
    id SERIAL PRIMARY KEY,
    machine_name TEXT NOT NULL UNIQUE,
    ip_connection TEXT,
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
    machine_name TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    value BIGINT
);

SELECT create_hypertable('telemetry_count', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS telemetry_event (
    id BIGSERIAL,
    time TIMESTAMPTZ NOT NULL,
    machine_name TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    value INTEGER
);

SELECT create_hypertable('telemetry_event', 'time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS telemetry_process (
    id BIGSERIAL,
    time TIMESTAMPTZ NOT NULL,
    machine_name TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    value DOUBLE PRECISION,
    value_str TEXT
);

SELECT create_hypertable('telemetry_process', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_telemetry_process_machine_tag ON telemetry_process (machine_name, tag_name, time DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_event_machine_tag ON telemetry_event (machine_name, tag_name, time DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_count_machine_tag ON telemetry_count (machine_name, tag_name, time DESC);

CREATE OR REPLACE VIEW view_machine_events AS
SELECT
    id,
    time as start_time,
    LEAD(time, 1, NOW()) OVER (PARTITION BY machine_name ORDER BY time) as end_time,
    EXTRACT(EPOCH FROM (LEAD(time, 1, NOW()) OVER (PARTITION BY machine_name ORDER BY time) - time)) as duration,
    machine_name,
    tag_name,
    value as state_code
FROM telemetry_event
WHERE tag_name IN ('OEE.nMachineState', 'OEE.nStopRootReason');

CREATE MATERIALIZED VIEW IF NOT EXISTS view_hourly_stats AS
SELECT
    time_bucket('1 hour', time) as bucket,
    machine_name,
    tag_name,
    MAX(value) - MIN(value) as count_delta
FROM telemetry_count
GROUP BY bucket, machine_name, tag_name;