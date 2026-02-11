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
    val_num DOUBLE PRECISION,
    val_int BIGINT,
    val_bool BOOLEAN,
    val_str TEXT
);

SELECT create_hypertable('telemetry_process', 'time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_telemetry_process_machine_tag ON telemetry_process (machine_name, tag_name, time DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_event_machine_tag ON telemetry_event (machine_name, tag_name, time DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_count_machine_tag ON telemetry_count (machine_name, tag_name, time DESC);