import math
from collections import Counter
from datetime import timedelta

import pytz

from odoo import fields
from odoo.addons.mes_core.models import mes_anomaly_report as anomaly

WINDOWS = (7, 30, 60)
TOP = 15
HISTORY_LIST_DAYS = 60
DROP_LOOKBACK_DAYS = 14
FIELDS = ['machine_id', 'product_id', 'kind', 'loss_id', 'count_id', 'subject', 'runtime_hours',
          'stops_per_hour', 'stops_per_hour_avg', 'stops_per_hour_std',
          'downtime_pct', 'downtime_pct_avg', 'downtime_pct_std',
          'avg_stop_min', 'avg_stop_min_avg',
          'waste_per_hour', 'waste_per_hour_avg', 'waste_per_hour_std',
          'sample_count', 'z_score', 'status', 'driver', 'performance_id']

Report = env['mes.anomaly.report'].sudo()
companies = env['res.company'].sudo().search([])
now = fields.Datetime.now()


class Rollback(Exception):
    pass


def key_of(row):
    reason = row['loss_id'][0] if row['loss_id'] else row['count_id'][0]
    return (row['machine_id'][0], row['product_id'] and row['product_id'][0], row['kind'], reason)


def simulate(days):
    original = anomaly.HISTORY_DAYS
    anomaly.HISTORY_DAYS = days
    rows = []
    try:
        with env.cr.savepoint():
            Report._rebuild(companies)
            rows = Report.search_read([('is_current_shift', '=', True)], FIELDS)
            raise Rollback()
    except Rollback:
        pass
    finally:
        anomaly.HISTORY_DAYS = original
        env.invalidate_all()
    return rows


def impact(row):
    hours = row['runtime_hours'] or 0.0
    if row['kind'] == 'stop':
        current = row['downtime_pct'] / 100.0 * hours * 60.0
        excess = (row['downtime_pct'] - row['downtime_pct_avg']) / 100.0 * hours * 60.0
        return current, excess, 'min'
    current = row['waste_per_hour'] * hours
    excess = (row['waste_per_hour'] - row['waste_per_hour_avg']) * hours
    return current, excess, 'pcs'


def label(row):
    return (f"{row['machine_id'][1]} | {row['product_id'][1] if row['product_id'] else '-'} | "
            f"{row['kind']} | {row['subject']}")


def percentile(values, share):
    ordered = sorted(values)
    if not ordered:
        return 0.0
    return ordered[min(len(ordered) - 1, int(math.ceil(share * len(ordered))) - 1)]


print("CONFIG history_days=%s min_samples=%s ratio=%s z_warning=%s z_critical=%s min_runtime_min=%s" % (
    anomaly.HISTORY_DAYS, anomaly.MIN_SAMPLES, anomaly.DEVIATION_RATIO, anomaly.Z_WARNING,
    anomaly.Z_CRITICAL, anomaly.MIN_RUNTIME_MIN))

results = {days: simulate(days) for days in WINDOWS}
print()
print("1. STATUS COUNTS FOR THE CURRENT SHIFTS BY HISTORY LENGTH")
for days, rows in results.items():
    counts = Counter(row['status'] for row in rows)
    print(f"   {days:3d} days: " + ", ".join(f"{status}={counts[status]}" for status in
                                            ('critical', 'warning', 'new', 'normal', 'insufficient', 'gap')))

base = {key_of(row): row for row in results[7]}
by_window = {days: {key_of(row): row for row in rows} for days, rows in results.items()}
critical = sorted((row for row in results[7] if row['status'] == 'critical'),
                  key=lambda row: row['z_score'], reverse=True)

print()
print("2. SIZE OF THE CRITICAL ROWS (7 days): excess over the 7-day average in this shift")
stop_excess = [impact(row)[1] for row in critical if row['kind'] == 'stop']
waste_excess = [impact(row)[1] for row in critical if row['kind'] == 'waste']
for name, values, edges in (('stops, extra minutes', stop_excess, (2, 5, 15)),
                            ('waste, extra pieces', waste_excess, (10, 100, 1000))):
    buckets = Counter()
    for value in values:
        bucket = next((f"<{edge}" for edge in edges if value < edge), f">={edges[-1]}")
        buckets[bucket] += 1
    order = [f"<{edge}" for edge in edges] + [f">={edges[-1]}"]
    print(f"   {name}: " + ", ".join(f"{bucket}: {buckets[bucket]}" for bucket in order))

print()
print("3. SAME ROWS WITH 30 AND 60 DAYS OF HISTORY")
moved = Counter()
for row in critical:
    key = key_of(row)
    states = [by_window[days].get(key, {}).get('status', 'missing') for days in WINDOWS]
    moved[tuple(states)] += 1
for states, count in moved.most_common():
    print(f"   7d/30d/60d = {'/'.join(states)}: {count}")

print()
print(f"4. TOP {TOP} CRITICAL ROWS: current vs history (per-shift values, oldest first)")
today = fields.Date.context_today(Report)
docs = env['mes.machine.performance'].search([
    ('date', '>=', today - timedelta(days=HISTORY_LIST_DAYS + 2)), ('shift_id', '!=', False)])
windows = {doc.id: Report._doc_window(doc) for doc in docs}
slices = Report._anomaly_slices(docs, windows, now)
runs, stops, waste = Report._load_facts(docs, windows, now)
breakdown = env['mes.oee.breakdown'].sudo()
earliest = min((window[0] for window in windows.values()), default=now)
excluded = {machine.id: breakdown._excluded_windows(machine, earliest, now) for machine in docs.mapped('machine_id')}
totals = anomaly.build_totals([(doc.id, doc.machine_id.id) for doc in docs], windows, slices, runs, stops,
                              excluded, now)
docs_by_id = {doc.id: doc for doc in docs}

for row in critical[:TOP]:
    machine_id, product_id, kind, reason_id = key_of(row)
    current_doc = row['performance_id'][0]
    series = []
    for (doc_id, slice_product), (runtime, per_event) in sorted(
            totals.items(), key=lambda item: windows[item[0][0]][0]):
        doc = docs_by_id[doc_id]
        if doc.machine_id.id != machine_id or (slice_product or False) != (product_id or False) or doc_id == current_doc:
            continue
        if windows[doc_id][1] > now:
            continue
        hours = runtime / 3600.0
        if kind == 'stop':
            count, seconds = per_event.get(reason_id, (0, 0.0))
            series.append((doc, count / hours if hours else 0.0))
        else:
            products = {product for (other_doc, product) in totals if other_doc == doc_id}
            if len(products) != 1:
                continue
            series.append((doc, waste.get(doc_id, {}).get(reason_id, 0.0) / hours if hours else 0.0))
    values = [value for _doc, value in series]
    last7 = [value for doc, value in series if doc.date >= today - timedelta(days=7)]
    current, excess, unit = impact(row)
    metric = 'stops/h' if kind == 'stop' else 'pcs/h'
    current_rate = row['stops_per_hour'] if kind == 'stop' else row['waste_per_hour']
    print(f" - {label(row)}")
    print(f"   z={row['z_score']} driver={row['driver']} runtime={row['runtime_hours']} h "
          f"current={current_rate} {metric} ({current:.0f} {unit}, +{excess:.0f} {unit} over average)")
    print(f"   60d: n={len(values)} mean={sum(values) / len(values) if values else 0:.2f} "
          f"median={percentile(values, 0.5):.2f} p90={percentile(values, 0.9):.2f} max={max(values) if values else 0:.2f} "
          f"zeros={sum(1 for value in values if not value)} | 7d: n={len(last7)}")
    print("   values: " + " ".join(f"{value:.1f}" for value in values))

print()
print("5. RAW COUNTER CHECK FOR CRITICAL WASTE ROWS (resets inside the window inflate MAX-MIN)")
Machine = env['mes.machine.settings'].sudo()
checked = set()
with env['mes.timescale.base']._connection() as connection:
    with connection.cursor() as cursor:
        for row in critical:
            if row['kind'] != 'waste':
                continue
            workcenter = env['mrp.workcenter'].browse(row['machine_id'][0])
            count = env['mes.counts'].browse(row['count_id'][0])
            tag, cumulative = count.get_count_config_for_machine(workcenter.machine_settings_id)
            if not tag or (workcenter.id, tag) in checked:
                continue
            checked.add((workcenter.id, tag))
            tz = pytz.timezone(workcenter.company_id.tz or 'UTC')
            wall = lambda moment: pytz.utc.localize(moment).astimezone(tz).strftime('%Y-%m-%d %H:%M:%S')
            start, end = windows.get(row['performance_id'][0]) or Report._doc_window(
                env['mes.machine.performance'].browse(row['performance_id'][0]))
            cursor.execute("""
                SELECT count(*), count(*) FILTER (WHERE d < 0),
                       COALESCE(max(value) - min(value), 0),
                       COALESCE(sum(d) FILTER (WHERE d > 0), 0), COALESCE(sum(value), 0)
                FROM (SELECT value, value - lag(value) OVER (ORDER BY time) AS d
                      FROM telemetry_count
                      WHERE machine_name = %s AND tag_name = %s AND time >= %s AND time < %s) s
            """, (workcenter.machine_settings_id.name, tag, wall(start), wall(min(end, now))))
            rows_n, drops, max_min, rises, total = cursor.fetchone()
            cursor.execute("""
                SELECT time, prev, value FROM (
                    SELECT time, value, lag(value) OVER (ORDER BY time) AS prev
                    FROM telemetry_count
                    WHERE machine_name = %s AND tag_name = %s AND time >= %s) s
                WHERE value < prev ORDER BY time DESC LIMIT 8
            """, (workcenter.machine_settings_id.name, tag, wall(now - timedelta(days=DROP_LOOKBACK_DAYS))))
            recent = cursor.fetchall()
            print(f" - {workcenter.name} {tag} cumulative={cumulative} | shift: rows={rows_n} drops={drops} "
                  f"max-min={max_min:.0f} rises={rises:.0f} sum={total:.0f}")
            if recent:
                print("   drops in last %d days: " % DROP_LOOKBACK_DAYS
                      + "; ".join(f"{stamp:%m-%d %H:%M} {prev:.0f}->{value:.0f}" for stamp, prev, value in recent))

print()
print("6. STOP REASONS FLAGGED THAT MAY BE PLANNED OR TECHNICIAN WORK")
for row in results[7]:
    if row['kind'] == 'stop' and row['status'] in ('critical', 'new', 'warning'):
        event = env['mes.event'].browse(row['loss_id'][0])
        if not event.calculable or 'tech' in (event.display_name or '').lower():
            print(f"   {label(row)} calculable={event.calculable} status={row['status']}")

env.cr.rollback()
print()
print("done")
