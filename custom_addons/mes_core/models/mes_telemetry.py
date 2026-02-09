import os
import logging
import psycopg2
from odoo import models, fields, api, tools

_logger = logging.getLogger(__name__)

class MesTimescaleBase(models.AbstractModel):
    """
    Базовый класс для получения параметров подключения из Env
    и выполнения прямых запросов в TimescaleDB.
    """
    _name = 'mes.timescale.base'
    _description = 'Base class for TimescaleDB connection'

    def _get_ts_params(self):
        """Возвращает словарь с параметрами подключения из ENV"""
        return {
            'host': os.environ.get('TIMESCALE_HOST', 'timescaledb'),
            'port': os.environ.get('TIMESCALE_PORT', '5432'),
            'dbname': os.environ.get('TIMESCALE_DB', 'mes_telemetry'),
            'user': os.environ.get('TIMESCALE_USER', 'timescale_user'),
            'password': os.environ.get('TIMESCALE_PASS', 'timescale_pass')
        }

    def _get_ts_connection(self):
        params = self._get_ts_params()
        try:
            return psycopg2.connect(
                host=params['host'],
                port=params['port'],
                database=params['dbname'],
                user=params['user'],
                password=params['password']
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

class MesInfrastructureManager(models.AbstractModel):
    """
    Класс отвечает за инициализацию инфраструктуры:
    1. Создание таблиц в удаленной TimescaleDB.
    2. Настройка FDW (Foreign Data Wrapper) в локальной базе Odoo.
    """
    _name = 'mes.infrastructure.manager'
    _description = 'Timescale Infrastructure Manager'
    _inherit = ['mes.timescale.base']

    @api.model
    def _init_remote_timescale(self):
        """Создает таблицы и гипертаблицы в самой TimescaleDB (Remote)"""
        conn = self._get_ts_connection()
        if not conn:
            _logger.warning("Could not connect to TimescaleDB to init tables.")
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
            _logger.info("Remote TimescaleDB Schema Initialized.")
        except Exception as e:
            conn.rollback()
            _logger.error(f"Remote Init Error: {e}")
        finally:
            conn.close()

    @api.model
    def _init_local_fdw(self):
        params = self._get_ts_params()
        
        self.env.cr.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw;")

        self.env.cr.execute("""
            DROP USER MAPPING IF EXISTS FOR %s SERVER timescaledb_server;
        """ % self.env.cr.dbname) 
        
        server_sql = """
            CREATE SERVER IF NOT EXISTS timescaledb_server
            FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host '%s', port '%s', dbname '%s');
        """ % (params['host'], params['port'], params['dbname'])
        self.env.cr.execute(server_sql)
        self.env.cr.execute("""
            ALTER SERVER timescaledb_server 
            OPTIONS (SET host '%s', SET port '%s', SET dbname '%s');
        """ % (params['host'], params['port'], params['dbname']))

        current_db_user = self.env.cr.connection.info.user
        
        mapping_sql = """
            CREATE USER MAPPING IF NOT EXISTS FOR "%s"
            SERVER timescaledb_server
            OPTIONS (user '%s', password '%s');
        """ % (current_db_user, params['user'], params['password'])
        
        try:
            self.env.cr.execute(mapping_sql)
            self.env.cr.execute("""
                ALTER USER MAPPING FOR "%s" SERVER timescaledb_server 
                OPTIONS (SET user '%s', SET password '%s');
            """ % (current_db_user, params['user'], params['password']))
            _logger.info("Local FDW Server & Mapping Configured.")
        except Exception as e:
            _logger.error(f"FDW Setup Error: {e}")


class MesMachineSettings(models.Model):
    _name = 'mes.machine.settings'
    _description = 'Machine Connection Settings'
    _inherit = ['mail.thread', 'mail.activity.mixin', 'mes.timescale.base']

    name = fields.Char(string='Machine Name', required=True, copy=False, tracking=True)
    ip_connection = fields.Char(string='Connection IP')
    ip_data = fields.Char(string='TwinCAT/Data IP')
    
    signal_ids = fields.One2many('mes.signal.tag', 'machine_settings_id', string='Monitored Signals')

    _sql_constraints = [('name_uniq', 'unique (name)', 'Machine Name must be unique!')]

    def init(self):
        super().init()
        self.env['mes.infrastructure.manager']._init_remote_timescale()
        self.env['mes.infrastructure.manager']._init_local_fdw()

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


class MesTelemetryEventFDW(models.Model):
    _name = 'mes.telemetry.event.fdw'
    _description = 'Telemetry Events (Foreign Table)'
    _auto = False 

    time = fields.Datetime(string='Time', readonly=True)
    machine_name = fields.Char(string='Machine', readonly=True)
    tag_name = fields.Char(string='Tag', readonly=True)
    value = fields.Integer(string='Value', readonly=True)

    def init(self):
        """Создает связь FOREIGN TABLE"""
        tools.drop_view_if_exists(self.env.cr, self._table)
        self.env.cr.execute("""
            CREATE FOREIGN TABLE IF NOT EXISTS %s (
                time TIMESTAMPTZ,
                machine_name TEXT,
                tag_name TEXT,
                value INTEGER
            )
            SERVER timescaledb_server
            OPTIONS (schema_name 'public', table_name 'telemetry_event');
        """ % self._table)

class MesTelemetryCountFDW(models.Model):
    _name = 'mes.telemetry.count.fdw'
    _description = 'Telemetry Counts (Foreign Table)'
    _auto = False

    time = fields.Datetime(string='Time', readonly=True)
    machine_name = fields.Char(string='Machine', readonly=True)
    tag_name = fields.Char(string='Tag', readonly=True)
    value = fields.Integer(string='Value', readonly=True)

    def init(self):
        tools.drop_view_if_exists(self.env.cr, self._table)
        self.env.cr.execute("""
            CREATE FOREIGN TABLE IF NOT EXISTS %s (
                time TIMESTAMPTZ,
                machine_name TEXT,
                tag_name TEXT,
                value BIGINT
            )
            SERVER timescaledb_server
            OPTIONS (schema_name 'public', table_name 'telemetry_count');
        """ % self._table)