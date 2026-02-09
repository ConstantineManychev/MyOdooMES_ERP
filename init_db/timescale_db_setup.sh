#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    CREATE EXTENSION IF NOT EXISTS postgres_fdw;

    CREATE SERVER IF NOT EXISTS timescaledb_server
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host '$TELEMETRY_HOST', port '$TELEMETRY_PORT', dbname '$TELEMETRY_DB');

    DROP USER MAPPING IF EXISTS FOR $POSTGRES_USER SERVER timescaledb_server;

    CREATE USER MAPPING FOR $POSTGRES_USER
    SERVER timescaledb_server
    OPTIONS (user '$TELEMETRY_USER', password '$TELEMETRY_PASS');

EOSQL