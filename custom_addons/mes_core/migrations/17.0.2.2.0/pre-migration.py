import logging

_logger = logging.getLogger(__name__)


def migrate(cr, version):
    cr.execute("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'mes_kiosk_session' AND column_name = 'user_id'
    """)
    if not cr.fetchone():
        return

    cr.execute("ALTER TABLE mes_kiosk_session ADD COLUMN IF NOT EXISTS employee_id integer")

    cr.execute("""
        UPDATE mes_kiosk_session ks
        SET employee_id = sub.employee_id
        FROM (
            SELECT DISTINCT ON (rp.user_id) rp.user_id, e.id AS employee_id
            FROM hr_employee e
            JOIN resource_resource rr ON rr.id = e.resource_id
            JOIN res_users u ON u.id = rr.user_id
            JOIN res_partner rp ON rp.id = u.partner_id
            ORDER BY rp.user_id, e.id
        ) sub
        WHERE ks.user_id = sub.user_id AND ks.employee_id IS NULL
    """)
    cr.execute("SELECT COUNT(*) FROM mes_kiosk_session WHERE employee_id IS NULL")
    leftover = cr.fetchone()[0]
    if leftover:
        _logger.warning(
            "Kiosk sessions migration: %s sessions have no matching employee. "
            "They will be auto-closed since employee_id becomes required.", leftover
        )
        cr.execute("UPDATE mes_kiosk_session SET logout_at = COALESCE(logout_at, NOW()) WHERE employee_id IS NULL")
        cr.execute("DELETE FROM mes_kiosk_session WHERE employee_id IS NULL")
