import os
from datetime import datetime, timedelta

import pytz

from odoo import fields

MACHINE = os.environ.get('MACHINE', 'M4')
DAY = os.environ.get('DATE') or fields.Date.today().isoformat()
START = os.environ.get('START', '06:00')
END = os.environ.get('END', '14:00')
MAJOR_MIN = float(os.environ.get('MAJOR_MIN', '10'))
TOP_WASTE = int(os.environ.get('TOP_WASTE', '8'))
FINAL_HELP_STATES = ('help_completed', 'help_rejected', 'done', 'cancel')

workcenter = env['mrp.workcenter'].search([('name', '=', MACHINE)], limit=1)
if not workcenter:
    raise SystemExit(f"Machine {MACHINE} not found")
machine = workcenter.machine_settings_id
tz = pytz.timezone(workcenter.company_id.tz or 'UTC')
now = fields.Datetime.now()


def to_utc(moment):
    return tz.localize(moment, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)


def to_local(moment):
    return pytz.utc.localize(moment).astimezone(tz).replace(tzinfo=None)


def hhmm(moment):
    return to_local(moment).strftime('%H:%M') if moment else '--:--'


def minutes(start, end):
    return max((end - start).total_seconds(), 0.0) / 60.0


start_local = datetime.strptime(f"{DAY} {START}", '%Y-%m-%d %H:%M')
end_local = datetime.strptime(f"{DAY} {END}", '%Y-%m-%d %H:%M')
if end_local <= start_local:
    end_local += timedelta(days=1)
start_utc = to_utc(start_local)
end_utc = min(to_utc(end_local), now)
end_local = to_local(end_utc)

intervals, planned_sec = machine._get_planned_working_intervals(start_utc, end_utc, workcenter)
run_sec = machine._fetch_interval_stats(intervals, workcenter.id, mode='runtime') or 0.0
produced = 0.0
with env['mes.timescale.base']._connection() as connection:
    with connection.cursor() as cursor:
        raw = machine._fetch_waste_stats_raw(cursor, start_local, end_local)
for signal in machine.count_tag_ids:
    if signal.count_id == workcenter.production_count_id:
        totals = raw.get(signal.tag_name, {})
        produced = totals.get('cum' if signal.is_cumulative else 'sum', 0.0)
        break
kpi = machine._calculate_kpi(run_sec, produced, planned_sec, workcenter)
run_hours = run_sec / 3600.0

breakdown = env['mes.oee.breakdown']
excluded = breakdown._excluded_windows(workcenter, start_utc, end_utc)

print()
print("=" * 78)
print(f"SHIFT ISSUES - {workcenter.name} - {DAY} {START}-{END}")
print("=" * 78)

if 'mes.production.run' in env:
    runs = env['mes.production.run'].search([
        ('workcenter_id', '=', workcenter.id),
        ('date_start', '<', end_utc),
        '|', ('date_end', '=', False), ('date_end', '>', start_utc),
    ], order='date_start')
    for run in runs:
        name = run.product_id.display_name if run.product_id else (run.unmapped_label or 'Not recognised')
        print(f"Product: {name}  ({hhmm(max(run.date_start, start_utc))}-{hhmm(min(run.date_end or end_utc, end_utc))})")

print(f"OEE {kpi['oee']:.1f}% | Availability {kpi['availability']:.1f}% | "
      f"Downtime {kpi['downtime_losses']:.1f}% | Waste {kpi['waste_losses']:.1f}%")
print(f"Run time {kpi['runtime_formatted']} of {planned_sec / 3600.0:.2f} h planned | "
      f"Good output {produced:,.0f} ({kpi['produced_per_hour']:,.0f}/run h)")

alarms = env['mes.performance.alarm'].search([
    ('performance_id.machine_id', '=', workcenter.id),
    ('start_time', '<', end_utc),
    '|', ('end_time', '=', False), ('end_time', '>', start_utc),
], order='start_time')
stops = []
for alarm in alarms:
    begin = max(alarm.start_time, start_utc)
    finish = min(alarm.end_time or end_utc, end_utc)
    if finish <= begin:
        continue
    planned = any(begin < window_end and finish > window_start for window_start, window_end in excluded)
    stops.append({'alarm': alarm, 'start': begin, 'end': finish, 'min': minutes(begin, finish), 'planned': planned})

unplanned = [stop for stop in stops if not stop['planned']]
print(f"Stops: {len(unplanned)} unplanned, {sum(s['min'] for s in unplanned):.0f} min in total")

print()
print(f"1. MAJOR STOPS (single stop >= {MAJOR_MIN:.0f} min)")
major = [stop for stop in unplanned if stop['min'] >= MAJOR_MIN]
if not major:
    print("   none")
for stop in major:
    alarm = stop['alarm']
    print(f"   {hhmm(stop['start'])}-{hhmm(stop['end'])}  {stop['min']:5.0f} min  {alarm.loss_id.display_name}")
    print(f"      operator: {alarm.comment or '(no comment)'}")

print()
print(f"   Reasons adding up to >= {MAJOR_MIN:.0f} min from repeated stops")
by_reason = {}
for stop in unplanned:
    key = stop['alarm'].loss_id.display_name
    count, total = by_reason.get(key, (0, 0.0))
    by_reason[key] = (count + 1, total + stop['min'])
repeated = sorted(((total, count, name) for name, (count, total) in by_reason.items()
                   if count > 1 and total >= MAJOR_MIN), reverse=True)
if not repeated:
    print("   none")
for total, count, name in repeated:
    print(f"   {name}: {count} stops, {total:.0f} min")

planned_stops = [stop for stop in stops if stop['planned']]
if planned_stops:
    print(f"   (not counted: {len(planned_stops)} stops, {sum(s['min'] for s in planned_stops):.0f} min "
          f"inside planned downtime or technician work)")

print()
print("2. TECHNICIAN CALLS")
Task = env['mes.task']
states = dict(Task._fields['state']._description_selection(env))
helps = Task.search([
    ('task_type', '=', 'kiosk_help'),
    ('parent_id', '=', False),
    ('machine_id', '=', workcenter.id),
    ('create_date', '<', end_utc),
    ('create_date', '>=', start_utc - timedelta(days=2)),
], order='create_date')
helps = helps.filtered(lambda task: task.create_date >= start_utc or task.state not in FINAL_HELP_STATES
                       or (task.operator_confirmed_at and task.operator_confirmed_at >= start_utc))
if not helps:
    print("   no help requests from the kiosk")
for task in helps:
    closed = task.operator_confirmed_at or (task.write_date if task.state in FINAL_HELP_STATES else None)
    window_end = min(closed or end_utc, end_utc)
    lost = sum(minutes(max(s['start'], task.create_date), min(s['end'], window_end))
               for s in unplanned if s['start'] < window_end and s['end'] > task.create_date)
    requester = task.operator_session_id.employee_id.name or '?'
    print(f"   {hhmm(task.create_date)}  {task.reason_snapshot or task.name}  (called by {requester})")
    if task.operator_description:
        print(f"      problem: {task.operator_description.strip()}")
    response = (f"{minutes(task.create_date, task.technician_arrived_at):.0f} min"
                if task.technician_arrived_at else 'no arrival recorded')
    print(f"      technician arrived: {response} | downtime while open: {lost:.0f} min")
    resolved = {'confirmed': 'YES', 'rejected': 'NO'}.get(task.operator_confirmation_state or '', 'not confirmed')
    print(f"      status: {states.get(task.state, task.state)} | resolved (operator): {resolved}"
          + (f" | MaintainX {task.maintainx_id}" if task.maintainx_id else ''))
    if task.operator_confirmation_comment:
        print(f"      operator comment: {task.operator_confirmation_comment.strip()}")

work = env['mes.task.work.interval'].search([
    ('machine_id', '=', workcenter.id),
    ('start_time', '<', end_utc),
    '|', ('end_time', '=', False), ('end_time', '>', start_utc),
], order='start_time')
if work:
    print("   Technician time on the machine:")
    for interval in work:
        begin = max(interval.start_time, start_utc)
        finish = min(interval.end_time or end_utc, end_utc)
        print(f"   {hhmm(begin)}-{hhmm(finish)}  {minutes(begin, finish):4.0f} min  "
              f"{interval.employee_id.name or '?'}  {interval.task_id.name or ''}")

print()
print("3. QUALITY / WASTE")
items = [item for item in breakdown._collect_counts(workcenter, start_utc, end_utc)
         if item['count'] != workcenter.production_count_id and item['calculable']]
items.sort(key=lambda item: item['qty'], reverse=True)
if not items:
    print("   no waste counters")
for item in items[:TOP_WASTE]:
    rate = item['qty'] / run_hours if run_hours else 0.0
    print(f"   {item['name']}: {item['qty']:,.0f} pcs ({rate:,.0f}/run h)")
print("   QC findings: (not in Odoo yet - from the QC technician report)")

if 'mes.link.outage' in env:
    gaps = env['mes.link.outage'].search([
        ('workcenter_id', '=', workcenter.id),
        ('state', '!=', 'planned'),
        ('date_start', '<', end_utc),
        '|', ('date_end', '=', False), ('date_end', '>', start_utc),
    ])
    if gaps:
        print()
        print("DATA GAPS (figures above are incomplete for these periods)")
        for gap in gaps:
            print(f"   {hhmm(gap.date_start)}-{hhmm(gap.date_end)}  {gap.cause}  {gap.state}")

print("=" * 78)
