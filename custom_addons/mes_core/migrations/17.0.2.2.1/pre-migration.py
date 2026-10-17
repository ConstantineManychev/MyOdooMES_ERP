import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    cr.execute("DROP INDEX IF EXISTS mes_kiosk_session_active_employee_uniq")

    cr.execute("""
        SELECT workcenter_id, COUNT(*)
        FROM mes_kiosk_session
        WHERE logout_at IS NULL
        GROUP BY workcenter_id
        HAVING COUNT(*) > 1
    """)
    conflicts = cr.fetchall()
    if conflicts:
        _logger.warning("Cleaning up %s workcenters with multiple active sessions", len(conflicts))
        cr.execute("""
            WITH ranked AS (
                SELECT id,
                       ROW_NUMBER() OVER (PARTITION BY workcenter_id ORDER BY login_at DESC, id DESC) AS rn
                FROM mes_kiosk_session
                WHERE logout_at IS NULL
            )
            UPDATE mes_kiosk_session
            SET logout_at = NOW()
            WHERE id IN (SELECT id FROM ranked WHERE rn > 1)
        """)

    cr.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS mes_kiosk_session_active_workcenter_uniq
        ON mes_kiosk_session (workcenter_id)
        WHERE logout_at IS NULL
    """)
    _logger.info("Kiosk session index migrated: one active session per workcenter enforced")
