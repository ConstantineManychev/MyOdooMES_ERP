-- Anomaly detection moved into the Odoo database (model mes.anomaly.report).
-- It is computed from mes_performance_alarm / mes_machine_performance /
-- mes_flat_downtime, which all live in the Odoo schema, so this TimescaleDB
-- view has no consumers left.

DROP VIEW IF EXISTS view_mes_anomalies;
