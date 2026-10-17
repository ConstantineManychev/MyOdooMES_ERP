from psycopg2 import sql
from odoo import models, fields


class MesTelemetryHourlyFDW(models.Model):
    _name = 'mes.telemetry.hourly.fdw'
    _description = 'Hourly Telemetry Aggregates'
    _auto = False

    bucket = fields.Datetime(string="Hour Bucket", readonly=True)
    machine_name = fields.Char(string="Machine", readonly=True)
    tag_name = fields.Char(string="Tag", readonly=True)
    total_events = fields.Integer(string="Total Events", readonly=True)
    avg_val = fields.Float(string="Average Value", readonly=True)
    max_val = fields.Integer(string="Max Value", readonly=True)
    min_val = fields.Integer(string="Min Value", readonly=True)

    def init(self):
        table = sql.Identifier(self._table)
        cr = self.env.cr
        cr.execute(sql.SQL("DROP FOREIGN TABLE IF EXISTS {}").format(table))
        cr.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(table))
        cr.execute(sql.SQL("DROP VIEW IF EXISTS {}").format(table))
        cr.execute(sql.SQL("""
            CREATE FOREIGN TABLE {} (
                bucket TIMESTAMPTZ,
                machine_name TEXT,
                tag_name TEXT,
                total_events BIGINT,
                avg_val DOUBLE PRECISION,
                max_val BIGINT,
                min_val BIGINT
            )
            SERVER timescaledb_server
            OPTIONS (schema_name 'public', table_name 'telemetry_hourly_stats')
        """).format(table))
