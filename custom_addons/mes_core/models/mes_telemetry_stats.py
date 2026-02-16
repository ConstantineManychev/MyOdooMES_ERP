from odoo import models, fields, tools

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
        self.env.cr.execute("DROP FOREIGN TABLE IF EXISTS %s" % self._table)
        self.env.cr.execute("DROP TABLE IF EXISTS %s" % self._table)
        self.env.cr.execute("DROP VIEW IF EXISTS %s" % self._table)
        
        self.env.cr.execute("""
            CREATE FOREIGN TABLE %s (
                bucket TIMESTAMPTZ,
                machine_name TEXT,
                tag_name TEXT,
                total_events BIGINT,
                avg_val DOUBLE PRECISION,
                max_val BIGINT,
                min_val BIGINT
            )
            SERVER timescaledb_server
            OPTIONS (schema_name 'public', table_name 'telemetry_hourly_stats');
        """ % self._table)

class MesAnomalyFDW(models.Model):
    _name = 'mes.anomaly.fdw'
    _description = 'Machine Anomalies Snapshot'
    _auto = False

    machine_name = fields.Char(string="Machine", readonly=True)
    current_stops = fields.Float(string="Stops (Last Hour)", readonly=True)
    historical_avg = fields.Float(string="Avg Stops (Daily)", readonly=True)
    status = fields.Selection([
        ('normal', 'Normal'), 
        ('critical', 'Critical')
    ], string="Status", readonly=True)

    def init(self):
        self.env.cr.execute("DROP FOREIGN TABLE IF EXISTS %s" % self._table)
        self.env.cr.execute("DROP TABLE IF EXISTS %s" % self._table)
        self.env.cr.execute("DROP VIEW IF EXISTS %s" % self._table)
        
        self.env.cr.execute("""
            CREATE FOREIGN TABLE %s (
                id bigint,
                machine_name TEXT,
                current_stops DOUBLE PRECISION,
                historical_avg DOUBLE PRECISION,
                status TEXT
            )
            SERVER timescaledb_server
            OPTIONS (schema_name 'public', table_name 'view_mes_anomalies');
        """ % self._table)