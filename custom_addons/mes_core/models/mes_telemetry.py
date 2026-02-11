import os
import logging
import psycopg2
from psycopg2 import pool
from contextlib import contextmanager
from odoo import models, fields, api, tools
from odoo.modules import get_module_resource
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

_TS_CONNECTION_POOL = None

class MesTimescaleBase(models.AbstractModel):
    _name = 'mes.timescale.base'
    _description = 'Base class for TimescaleDB connection'

    def _init_pool(self):
        global _TS_CONNECTION_POOL
        if _TS_CONNECTION_POOL:
            return

        params = self._get_connection_params()
        try:
            _TS_CONNECTION_POOL = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                host=params['host'],
                port=params['port'],
                dbname=params['dbname'],
                user=params['user'],
                password=params['password']
            )
        except psycopg2.Error as e:
            _logger.critical(f"Failed to create TimescaleDB pool: {e}")
            raise UserError("External Database Connection Failed")

    def _get_connection_params(self):
        env = os.environ.get
        param = self.env['ir.config_parameter'].sudo().get_param
        
        return {
            'host': env('TELEMETRY_HOST') or param('timescale.host') or 'timescaledb',
            'port': env('TELEMETRY_PORT') or param('timescale.port') or '5432',
            'dbname': env('TELEMETRY_DB') or param('timescale.db') or 'mes_telemetry',
            'user': env('TELEMETRY_USER') or param('timescale.user') or 'timescale_user',
            'password': env('TELEMETRY_PASS') or param('timescale.password') or 'timescale_pass'
        }

    @contextmanager
    def _cursor(self):
        if not _TS_CONNECTION_POOL:
            self._init_pool()

        conn = _TS_CONNECTION_POOL.getconn()
        try:
            with conn.cursor() as cur:
                yield cur
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            _TS_CONNECTION_POOL.putconn(conn)

    def _get_sql_query(self, filename):
        path = get_module_resource('mes_core', 'sql', filename)
        if not path or not os.path.isfile(path):
            _logger.error(f"SQL file {filename} not found in mes_core/sql/")
            return ""
        with open(path, 'r') as f:
            return f.read()

    def _execute_from_file(self, filename, params=None):
        query = self._get_sql_query(filename)
        if not query:
            return
        with self._cursor() as cur:
            cur.execute(query, params)

class MesInfrastructureManager(models.AbstractModel):
    _name = 'mes.infrastructure.manager'
    _description = 'Timescale Infrastructure Manager'
    _inherit = ['mes.timescale.base']

    @api.model
    def _init_DB(self):
        self._execute_from_file('init_schema.sql')

    @api.model
    def _init_local_fdw(self):
        params = self._get_connection_params()
        self.env.cr.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw;")
        
        server_name = 'timescaledb_server'
        self.env.cr.execute(f"DROP SERVER IF EXISTS {server_name} CASCADE;")
        
        self.env.cr.execute(f"""
            CREATE SERVER {server_name}
            FOREIGN DATA WRAPPER postgres_fdw
            OPTIONS (host %s, port %s, dbname %s);
        """, (params['host'], params['port'], params['dbname']))

        current_db_user = self.env.cr.connection.info.user
        self.env.cr.execute(f"""
            CREATE USER MAPPING FOR "{current_db_user}"
            SERVER {server_name}
            OPTIONS (user %s, password %s);
        """, (params['user'], params['password']))

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
        self.env['mes.infrastructure.manager']._init_DB()
        self.env['mes.infrastructure.manager']._init_local_fdw()

    @api.model
    def create(self, vals):
        rec = super().create(vals)
        self._execute_from_file('upsert_machine.sql', (rec.name, rec.ip_connection, rec.ip_data))
        return rec

    def write(self, vals):
        res = super().write(vals)
        for rec in self:
            self._execute_from_file('upsert_machine.sql', (rec.name, rec.ip_connection, rec.ip_data))
        return res

    def unlink(self):
        for rec in self:
            self._execute_from_file('delete_machine.sql', (rec.name,))
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
        self._sync(rec)
        return rec

    def write(self, vals):
        res = super().write(vals)
        for rec in self:
            self._sync(rec)
        return res

    def unlink(self):
        for rec in self:
            self._execute_from_file('delete_signal.sql', (rec.machine_settings_id.name, rec.tag_name))
        return super().unlink()

    def _sync(self, rec):
        self._execute_from_file('upsert_signal.sql', (
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
                id bigint,
                time TIMESTAMPTZ,
                machine_name TEXT,
                tag_name TEXT,
                value BIGINT
            )
            SERVER timescaledb_server
            OPTIONS (schema_name 'public', table_name 'telemetry_count');
        """ % self._table)