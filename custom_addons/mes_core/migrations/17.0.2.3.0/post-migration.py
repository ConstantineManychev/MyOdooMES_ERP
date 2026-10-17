def migrate(cr, version):
    """Heal help requests stuck on the kiosk screen: parents whose LAST
    subtask was rejected by a technician before the help_rejected finalization
    mechanism existed. They sit forever in help_tech_requested with no active
    subtask, blocking new help requests on the workcenter.

    Only touches parents whose latest (by id) subtask is help_tech_rejected
    and that have no live subtask; successful-close paths
    (help_awaiting_operator, help_completed) are excluded."""
    cr.execute("""
        UPDATE mes_task p
        SET state = 'help_rejected',
            closed_at = COALESCE(p.closed_at, (now() AT TIME ZONE 'UTC'))
        WHERE p.task_type = 'kiosk_help'
          AND p.parent_id IS NULL
          AND p.state NOT IN ('help_completed', 'help_rejected', 'cancel',
                              'help_awaiting_operator')
          AND (
              SELECT c.state FROM mes_task c
              WHERE c.parent_id = p.id
              ORDER BY c.id DESC LIMIT 1
          ) = 'help_tech_rejected'
          AND NOT EXISTS (
              SELECT 1 FROM mes_task c
              WHERE c.parent_id = p.id
                AND c.state IN ('help_tech_requested', 'help_tech_in_progress',
                                'help_tech_paused', 'help_tech_closed')
          )
        RETURNING p.id
    """)
    healed_ids = [row[0] for row in cr.fetchall()]
    if healed_ids:
        cr.execute("""
            INSERT INTO mes_task_status_history (task_id, status, change_date, source, comment)
            SELECT unnest(%s::int[]), 'Help: Rejected by Technician',
                   (now() AT TIME ZONE 'UTC'), 'odoo',
                   'Auto-finalized by migration 17.0.2.3.0: last subtask was rejected'
        """, (healed_ids,))
