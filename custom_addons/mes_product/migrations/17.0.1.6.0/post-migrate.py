TABLES = ('mes_performance_alarm', 'mes_performance_running', 'mes_performance_slowing')


def migrate(cr, version):
    for table in TABLES:
        cr.execute(f"""
            UPDATE "{table}" AS fact
            SET workcenter_id = doc.machine_id,
                shift_date = doc.date,
                shift_id = doc.shift_id
            FROM mes_machine_performance AS doc
            WHERE fact.performance_id = doc.id
              AND (fact.workcenter_id IS DISTINCT FROM doc.machine_id
                   OR fact.shift_date IS DISTINCT FROM doc.date
                   OR fact.shift_id IS DISTINCT FROM doc.shift_id)
        """)
    cr.execute('UPDATE "mes_performance_alarm" SET stop_count = 1 WHERE stop_count IS NULL')
