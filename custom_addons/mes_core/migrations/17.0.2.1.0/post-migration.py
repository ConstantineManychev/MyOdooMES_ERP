import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    cr.execute("""
        SELECT value FROM ir_config_parameter WHERE key = 'gemba.maintainx_token'
    """)
    row = cr.fetchone()
    if not row or not row[0]:
        _logger.info("MaintainX migration: no legacy token found, nothing to migrate")
        return

    token = row[0]

    cr.execute("""
        SELECT value FROM ir_config_parameter WHERE key = 'mes_core.use_maintainx'
    """)
    enabled_row = cr.fetchone()
    enabled = bool(enabled_row and enabled_row[0] == 'True')

    cr.execute("SELECT id, name FROM res_company ORDER BY id LIMIT 1")
    company_row = cr.fetchone()
    if not company_row:
        _logger.warning("MaintainX migration: no companies found")
        return

    company_id, company_name = company_row
    cr.execute("""
        UPDATE res_company
        SET maintainx_api_token = %s,
            maintainx_enabled   = %s
        WHERE id = %s
          AND (maintainx_api_token IS NULL OR maintainx_api_token = '')
    """, (token, enabled, company_id))
    _logger.info(
        "MaintainX token migrated from system parameter into company id=%s (%s)",
        company_id, company_name,
    )

    cr.execute("""
        DELETE FROM ir_config_parameter
        WHERE key IN ('gemba.maintainx_token', 'mes_core.use_maintainx')
    """)
