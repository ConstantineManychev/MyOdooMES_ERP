-- V1 creates telemetry_hourly_stats WITH NO DATA but never calls
-- add_continuous_aggregate_policy: that call only exists in
-- custom_addons/mes_core/sql/init_schema.sql, which nothing executes.
-- Flyway is the only path that touches the telemetry schema, so the aggregate
-- was created empty and no job was ever scheduled to fill it. Confirmed on the
-- live system: timescaledb_information.jobs returns 0 rows for this aggregate
-- and SELECT count(*) FROM telemetry_hourly_stats returns 0.
--
-- This migration adds the missing policy and deliberately does NOT swallow
-- errors: if it cannot be created the migration should fail loudly.
--
-- start_offset is bounded on purpose. The unexecuted copy used NULL, which would
-- make every hourly run rescan the whole hypertable. Buckets older than the
-- window are not filled by the policy, so run the one-time backfill manually,
-- outside a transaction:
--
--     CALL refresh_continuous_aggregate('telemetry_hourly_stats', NULL, NULL);

DO $$
DECLARE
    policy_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO policy_count
    FROM timescaledb_information.jobs
    WHERE proc_name = 'policy_refresh_continuous_aggregate'
      AND hypertable_name = 'telemetry_hourly_stats';

    IF policy_count = 0 THEN
        PERFORM add_continuous_aggregate_policy('telemetry_hourly_stats',
            start_offset => INTERVAL '7 days',
            end_offset   => INTERVAL '1 hour',
            schedule_interval => INTERVAL '1 hour');
        RAISE NOTICE 'Refresh policy created for telemetry_hourly_stats';
    ELSE
        RAISE NOTICE 'Refresh policy already present for telemetry_hourly_stats';
    END IF;
END $$;
