import os
import logging
import psycopg2
from psycopg2 import sql
from contextlib import contextmanager
from odoo import models, api
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

_REQUIRED_PARAMS = ('host', 'port', 'dbname', 'user', 'password')


class MesTimescaleBase(models.AbstractModel):
    _name = 'mes.timescale.base'
    _description = 'TimescaleDB Utilities'

    def _get_connection_params(self):
        ICP = self.env['ir.config_parameter'].sudo()
        env = os.environ.get
        params = {
            'host': env('TELEMETRY_HOST') or ICP.get_param('timescale.host'),
            'port': env('TELEMETRY_PORT') or ICP.get_param('timescale.port'),
            'dbname': env('TELEMETRY_DB') or ICP.get_param('timescale.db'),
            'user': env('TELEMETRY_USER') or ICP.get_param('timescale.user'),
            'password': env('TELEMETRY_PASS') or ICP.get_param('timescale.password'),
        }
        missing = [k for k in _REQUIRED_PARAMS if not params.get(k)]
        if missing:
            raise UserError(f"TimescaleDB config incomplete: missing {', '.join(missing)}")
        params['connect_timeout'] = 5
        return params

    @contextmanager
    def _connection(self):
        params = self._get_connection_params()
        conn = None
        try:
            conn = psycopg2.connect(**params)
            yield conn
            conn.commit()
        except Exception as exc:
            if conn:
                conn.rollback()
            _logger.exception("TimescaleDB connection error")
            raise UserError(f"Telemetry DB error: {exc}")
        finally:
            if conn:
                conn.close()


class MesTimescaleDBManager(models.AbstractModel):
    _name = 'mes.timescale.db.manager'
    _description = 'Timescale DB Manager'
    _inherit = ['mes.timescale.base']

    _LOCAL_SERVER_NAME = 'timescaledb_server'

    def _init_local_fdw(self):
        params = self._get_connection_params()
        server = sql.Identifier(self._LOCAL_SERVER_NAME)
        db_user = sql.Identifier(self.env.cr.connection.info.user)
        cr = self.env.cr
        cr.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw;")
        cr.execute(sql.SQL("DROP SERVER IF EXISTS {} CASCADE").format(server))
        cr.execute(
            sql.SQL(
                "CREATE SERVER {srv} FOREIGN DATA WRAPPER postgres_fdw "
                "OPTIONS (host %s, port %s, dbname %s)"
            ).format(srv=server),
            (params['host'], str(params['port']), params['dbname']),
        )
        cr.execute(
            sql.SQL(
                "CREATE USER MAPPING FOR {usr} SERVER {srv} OPTIONS (user %s, password %s)"
            ).format(usr=db_user, srv=server),
            (params['user'], params['password']),
        )

    def action_init_timescale_connection(self):
        self._init_local_fdw()
