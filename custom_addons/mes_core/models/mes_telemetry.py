import os
import logging
import psycopg2
from contextlib import contextmanager
from odoo import models, api, tools
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

class MesTimescaleBase(models.AbstractModel):
    _name = 'mes.timescale.base'
    _description = 'TimescaleDB Utilities'

    def _get_connection_params(self):
        params = self.env['ir.config_parameter'].sudo()
        env = os.environ.get
        return {
            'host': env('TELEMETRY_HOST') or params.get_param('timescale.host') or 'timescaledb',
            'port': env('TELEMETRY_PORT') or params.get_param('timescale.port') or '5432',
            'dbname': env('TELEMETRY_DB') or params.get_param('timescale.db') or 'mes_telemetry',
            'user': env('TELEMETRY_USER') or params.get_param('timescale.user') or 'timescale_user',
            'password': env('TELEMETRY_PASS') or params.get_param('timescale.password') or 'timescale_pass'
        }

    @contextmanager
    def _connection(self):
        params = self._get_connection_params()
        conn = False
        try:
            conn = psycopg2.connect(**params)
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            _logger.exception("TimescaleDB Connection Error")
            raise UserError(f"Telemetry DB Error: {str(e)}")
        finally:
            if conn:
                conn.close()

    def _get_sql_query(self, filename):
        path = tools.file_path(f'mes_core/sql/{filename}')
        if not path or not os.path.isfile(path):
            return ""
        with open(path, 'r') as f:
            return f.read()

    def _execute_from_file(self, filename, params=None):
        query = self._get_sql_query(filename)
        if not query:
            return
        
        with self._connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)

class MesTimescaleDBManager(models.AbstractModel):
    _name = 'mes.timescale.db.manager'
    _description = 'Timescale DB Manager'
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