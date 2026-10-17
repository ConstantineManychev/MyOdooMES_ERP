-- config_machine and config_signals were the configuration source of the
-- server-side poller (twincat_poller / telemetry_worker service), which has
-- been removed from the project. It never worked in production: Odoo filled
-- these tables but never supplied ams_net_id, so the poller could not connect
-- to any PLC.
--
-- The live collector (externals/logger_automate.py) takes its configuration
-- from Odoo via /mes/api/get_machine_config and never reads these tables.
-- Odoo no longer writes to them either.

DROP TABLE IF EXISTS config_signals;
DROP TABLE IF EXISTS config_machine;
