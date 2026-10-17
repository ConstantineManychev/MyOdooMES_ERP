DO $$
DECLARE
    version_parts INTEGER[];
    target_table TEXT;
BEGIN
    SELECT string_to_array(split_part(extversion, '-', 1), '.')::INTEGER[] INTO version_parts
    FROM pg_extension
    WHERE extname = 'timescaledb';

    IF version_parts < ARRAY[2, 11] THEN
        RAISE EXCEPTION 'TimescaleDB % cannot accept late rows into compressed chunks; upgrade to 2.11 or later first',
            array_to_string(version_parts, '.');
    END IF;

    FOREACH target_table IN ARRAY ARRAY['telemetry_count', 'telemetry_event', 'telemetry_process'] LOOP
        IF NOT EXISTS (
            SELECT 1 FROM timescaledb_information.jobs
            WHERE proc_name = 'policy_compression' AND hypertable_name = target_table
        ) THEN
            PERFORM add_compression_policy(target_table::regclass, compress_after => INTERVAL '10 days');
            RAISE NOTICE 'Compression policy created for %', target_table;
        ELSE
            RAISE NOTICE 'Compression policy already present for %', target_table;
        END IF;
    END LOOP;
END $$;
