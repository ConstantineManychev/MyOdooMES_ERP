import os
import logging
import psycopg2
from odoo import models, fields, api

_logger = logging.getLogger(__name__)

class MesTimescaleBase(models.AbstractModel):
    _name = 'mes.timescale.base'
    _description = 'Base class for TimescaleDB connection'

    def _get_ts_connection(self):
        try:
            return psycopg2.connect(
                host=os.environ.get('TIMESCALE_HOST', 'timescaledb'),
                port='5432',
                database=os.environ.get('TIMESCALE_DB', 'mes_telemetry'),
                user=os.environ.get('TIMESCALE_USER', 'timescale_user'),
                password=os.environ.get('TIMESCALE_PASS', 'timescale_pass')
            )
        except Exception as e:
            _logger.error(f"TimescaleDB Connection Error: {e}")
            return None

    def _execute_ts_query(self, query, params=None):
        conn = self._get_ts_connection()
        if not conn:
            return
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
            conn.commit()
        except Exception as e:
            conn.rollback()
            _logger.error(f"TS Query Error: {e}")
        finally:
            conn.close()

class MesMachineSettings(models.Model):
    _name = 'mes.machine.settings'
    _description = 'Machine Connection Settings'
    _inherit = ['mail.thread', 'mail.activity.mixin', 'mes.timescale.base']

    name = fields.Char(string='Machine Name', required=True, copy=False, tracking=True)
    ip_connection = fields.Char(string='Connection IP')
    ip_data = fields.Char(string='TwinCAT/Data IP')
    
    signal_ids = fields.One2many('mes.signal.tag', 'machine_settings_id', string='Monitored Signals')

    _sql_constraints = [('name_uniq', 'unique (name)', 'Machine Name must be unique!')]

    @api.model
    def create(self, vals):
        rec = super().create(vals)
        query = """
            INSERT INTO config_machine (machine_name, ip_connection, ip_data) VALUES (%s, %s, %s)
            ON CONFLICT (machine_name) DO UPDATE SET ip_connection=EXCLUDED.ip_connection, ip_data=EXCLUDED.ip_data;
        """
        rec._execute_ts_query(query, (rec.name, rec.ip_connection, rec.ip_data))
        return rec

    def write(self, vals):
        res = super().write(vals)
        for rec in self:
            query = """
                INSERT INTO config_machine (machine_name, ip_connection, ip_data) VALUES (%s, %s, %s)
                ON CONFLICT (machine_name) DO UPDATE SET ip_connection=EXCLUDED.ip_connection, ip_data=EXCLUDED.ip_data;
            """
            rec._execute_ts_query(query, (rec.name, rec.ip_connection, rec.ip_data))
        return res

    def unlink(self):
        for rec in self:
            rec._execute_ts_query("DELETE FROM config_machine WHERE machine_name = %s;", (rec.name,))
        return super().unlink()

class MesSignalTag(models.Model):
    _name = 'mes.signal.tag'
    _description = 'Monitored Signal Configuration'
    _inherit = ['mes.timescale.base']
    _rec_name = 'tag_name'

    machine_settings_id = fields.Many2one('mes.machine.settings', string='Machine', required=True, ondelete='cascade')
    tag_name = fields.Char(string='Signal Tag', required=True)
    
    poll_type = fields.Selection([('cyclic', 'Cyclic'), ('on_change', 'On Change')], default='cyclic', required=True)
    poll_frequency = fields.Integer(string='Freq (ms)', default=1000)
    
    param_type = fields.Selection([
        ('auto', 'Auto'),
        ('bool', 'Boolean'),
        ('int', 'Integer'),
        ('double', 'Double/Real'),
        ('string', 'String')
    ], string='Data Type', default='auto', required=True)
    
    signal_type = fields.Selection([
        ('count', 'Count (Integer)'),
        ('event', 'Event (Integer)'),
        ('process', 'Process (Mixed)')
    ], string='Category', required=True)

    @api.model
    def create(self, vals):
        rec = super().create(vals)
        self._sync_to_timescale(rec)
        return rec

    def write(self, vals):
        res = super().write(vals)
        for rec in self:
            self._sync_to_timescale(rec)
        return res

    def unlink(self):
        for rec in self:
            query = "DELETE FROM config_signals WHERE machine_name = %s AND tag_name = %s;"
            rec._execute_ts_query(query, (rec.machine_settings_id.name, rec.tag_name))
        return super().unlink()

    def _sync_to_timescale(self, rec):
        query = """
            INSERT INTO config_signals (machine_name, tag_name, poll_type, poll_frequency, param_type, signal_category)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (machine_name, tag_name) DO UPDATE 
            SET poll_type=EXCLUDED.poll_type, poll_frequency=EXCLUDED.poll_frequency, 
                param_type=EXCLUDED.param_type, signal_category=EXCLUDED.signal_category;
        """
        rec._execute_ts_query(query, (
            rec.machine_settings_id.name, rec.tag_name, 
            rec.poll_type, rec.poll_frequency, rec.param_type, rec.signal_type
        ))

class MesTimescaleManager(models.AbstractModel):
    _name = 'mes.timescale.manager'
    _description = 'TimescaleDB Initialization'
    _inherit = ['mes.timescale.base']

    @api.model
    def init_timescale_tables(self):
        conn = self._get_ts_connection()
        if not conn:
            return

        queries = [
            """
            CREATE TABLE IF NOT EXISTS config_machine (
                machine_name TEXT PRIMARY KEY,
                ip_connection TEXT,
                ip_data TEXT
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS config_signals (
                machine_name TEXT NOT NULL,
                tag_name TEXT NOT NULL,
                poll_type TEXT,
                poll_frequency INT,
                param_type TEXT,
                signal_category TEXT,
                PRIMARY KEY (machine_name, tag_name)
            );
            """,
            """
            CREATE TABLE IF NOT EXISTS telemetry_count (
                time TIMESTAMPTZ NOT NULL,
                machine_name TEXT NOT NULL,
                tag_name TEXT NOT NULL,
                value BIGINT
            );
            """,
            "SELECT create_hypertable('telemetry_count', 'time', if_not_exists => TRUE);",
            """
            CREATE TABLE IF NOT EXISTS telemetry_event (
                time TIMESTAMPTZ NOT NULL,
                machine_name TEXT NOT NULL,
                tag_name TEXT NOT NULL,
                value INTEGER
            );
            """,
            "SELECT create_hypertable('telemetry_event', 'time', if_not_exists => TRUE);",
            """
            CREATE TABLE IF NOT EXISTS telemetry_process (
                time TIMESTAMPTZ NOT NULL,
                machine_name TEXT NOT NULL,
                tag_name TEXT NOT NULL,
                val_num DOUBLE PRECISION,
                val_int BIGINT,
                val_bool BOOLEAN,
                val_str TEXT
            );
            """,
            "SELECT create_hypertable('telemetry_process', 'time', if_not_exists => TRUE);"
        ]
        
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_telemetry_process_machine_tag ON telemetry_process (machine_name, tag_name, time DESC);",
            "CREATE INDEX IF NOT EXISTS idx_telemetry_event_machine_tag ON telemetry_event (machine_name, tag_name, time DESC);",
            "CREATE INDEX IF NOT EXISTS idx_telemetry_count_machine_tag ON telemetry_count (machine_name, tag_name, time DESC);"
        ]

        try:
            with conn.cursor() as cur:
                for q in queries:
                    cur.execute(q)
                for idx in indexes:
                    cur.execute(idx)
                conn.commit()
            _logger.info("TimescaleDB Schema Initialized (Process table now has val_int).")
        except Exception as e:
            conn.rollback()
            _logger.error(f"Init Error: {e}")
        finally:
            conn.close()