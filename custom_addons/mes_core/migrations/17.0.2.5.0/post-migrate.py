import logging

log = logging.getLogger(__name__)

WEEKDAY_SETS = {
    'daily': ['0', '1', '2', '3', '4'],
    'weekend': ['5', '6'],
}


def _to_float_hour(value):
    return value.hour + value.minute / 60.0


def migrate(cr, version):
    if not version:
        return

    cr.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'mes_planned_downtime' AND column_name = 'rule_type'
    """)
    if not cr.fetchone():
        return

    cr.execute("""
        SELECT id, name, rule_type, date_start, date_end
        FROM mes_planned_downtime
        WHERE rule_type IN ('daily', 'weekend')
    """)
    legacy_rules = cr.fetchall()
    if not legacy_rules:
        log.info("mes_core migration: no legacy recurring downtime rules found")
        return

    migrated_lines = 0
    for rule_id, name, rule_type, date_start, date_end in legacy_rules:
        weekdays = WEEKDAY_SETS.get(rule_type, [])
        if not weekdays or not date_start or not date_end:
            log.warning("mes_core migration: rule %s (%s) skipped, incomplete definition",
                        rule_id, name)
            continue

        hour_from = _to_float_hour(date_start)
        hour_to = _to_float_hour(date_end)
        if hour_to <= hour_from:
            hour_to = 24.0

        for weekday in weekdays:
            cr.execute("""
                INSERT INTO mes_planned_downtime_line
                    (rule_id, weekday, hour_from, hour_to)
                VALUES (%s, %s, %s, %s)
            """, (rule_id, weekday, hour_from, hour_to))
            migrated_lines += 1

        cr.execute("""
            UPDATE mes_planned_downtime
            SET rule_type = 'weekly', valid_from = %s
            WHERE id = %s
        """, (date_start.date(), rule_id))

        log.info("mes_core migration: rule %s (%s) converted from %s to weekly, "
                 "%s..%s on %s days",
                 rule_id, name, rule_type, hour_from, hour_to, len(weekdays))

    log.info("mes_core migration: %s legacy rules converted, %s weekly intervals created",
             len(legacy_rules), migrated_lines)
