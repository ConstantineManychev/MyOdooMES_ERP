import logging

log = logging.getLogger(__name__)

LEGACY_STATE_MAP = {
    'help_tech_requested_sub': 'new',
    'help_tech_in_progress': 'assigned',
    'help_tech_paused': 'on_hold',
    'help_tech_closed': 'done',
    'help_tech_rejected': 'cancel',
}


def migrate(cr, version):
    if not version:
        return

    cr.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'mes_task' AND column_name = 'state'
    """)
    if not cr.fetchone():
        return

    for legacy_state, target_state in LEGACY_STATE_MAP.items():
        cr.execute("""
            UPDATE mes_task
            SET state = %s
            WHERE state = %s
              AND parent_id IS NOT NULL
        """, (target_state, legacy_state))
        if cr.rowcount:
            log.info("mes_core migration: %s subtasks moved from %s to %s",
                     cr.rowcount, legacy_state, target_state)

    cr.execute("""
        UPDATE mes_task
        SET state = 'help_completed'
        WHERE state IN ('help_tech_closed', 'help_tech_rejected')
          AND parent_id IS NULL
    """)
    if cr.rowcount:
        log.info("mes_core migration: %s root help requests normalised", cr.rowcount)

    cr.execute("""
        SELECT DISTINCT state FROM mes_task
        WHERE state LIKE 'help_tech_%%'
          AND state <> 'help_tech_requested'
    """)
    leftovers = [row[0] for row in cr.fetchall()]
    if leftovers:
        log.warning("mes_core migration: unmapped legacy states remain: %s", leftovers)
