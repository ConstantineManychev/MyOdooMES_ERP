import logging
import pytz
from datetime import datetime, timedelta
from odoo import models, fields, api

_logger = logging.getLogger(__name__)

DASHBOARD_INTERVAL_OPTIONS = (15, 30, 60, 120, 240, 480)
DASHBOARD_INTERVAL_SELECTION = [
    ('15', '15 minutes'),
    ('30', '30 minutes'),
    ('60', '1 hour'),
    ('120', '2 hours'),
    ('240', '4 hours'),
    ('480', '8 hours'),
]
DASHBOARD_PERIOD_MODES = [
    ('shift', 'Since Shift Start'),
    ('hours', 'Last Hours'),
]
DASHBOARD_VIEW_MODES = [
    ('table', 'Table'),
    ('chart', 'Chart'),
]

DEFAULT_INTERVAL_MINUTES = 60
DEFAULT_PERIOD_HOURS = 8
MIN_PERIOD_HOURS = 2
MAX_PERIOD_HOURS = 24
MAX_DASHBOARD_BUCKETS = 240

DASHBOARD_PREF_FIELDS = (
    'mes_dashboard_count_ids',
    'mes_dashboard_last_count_id',
    'mes_dashboard_interval',
    'mes_dashboard_period_mode',
    'mes_dashboard_period_hours',
    'mes_dashboard_view_mode',
)

TIMESTAMP_FORMAT = '%Y-%m-%d %H:%M'


class MesCountDashboard(models.AbstractModel):
    _name = 'mes.count.dashboard'
    _description = 'Counts Dashboard'
    _inherit = ['mes.timescale.base']

    @api.model
    def _clamp_interval(self, interval_minutes):
        try:
            requested = int(interval_minutes or DEFAULT_INTERVAL_MINUTES)
        except (TypeError, ValueError):
            return DEFAULT_INTERVAL_MINUTES
        if requested in DASHBOARD_INTERVAL_OPTIONS:
            return requested
        return min(DASHBOARD_INTERVAL_OPTIONS, key=lambda option: abs(option - requested))

    @api.model
    def _clamp_hours(self, period_hours):
        try:
            requested = int(period_hours or DEFAULT_PERIOD_HOURS)
        except (TypeError, ValueError):
            return DEFAULT_PERIOD_HOURS
        return min(MAX_PERIOD_HOURS, max(MIN_PERIOD_HOURS, requested))

    @api.model
    def _company_timezone(self):
        return pytz.timezone(self.env.company.tz or self.env.user.tz or 'UTC')

    @api.model
    def _now_local(self):
        return pytz.utc.localize(fields.Datetime.now()).astimezone(self._company_timezone()).replace(tzinfo=None)

    @api.model
    def _align_down(self, moment, interval_minutes):
        return self.env['mrp.workcenter']._align_to_bucket(moment, interval_minutes)

    @api.model
    def _align_up(self, moment, interval_minutes):
        return self.env['mrp.workcenter']._align_to_bucket(moment, interval_minutes, round_up=True)

    @api.model
    def _get_dashboard_workcenters(self, workcenter_ids=None):
        domain = [('machine_settings_id', '!=', False)]
        if workcenter_ids:
            domain.append(('id', 'in', [int(value) for value in workcenter_ids]))
        return self.env['mrp.workcenter'].search(domain, order='name')

    @api.model
    def _get_mapped_count_signals(self, workcenters):
        machines = workcenters.machine_settings_id
        if not machines:
            return self.env['mes.signal.count']
        return self.env['mes.signal.count'].sudo().search([
            ('machine_id', 'in', machines.ids),
            ('count_id', '!=', False),
        ])

    @api.model
    def _serialize_counts(self, counts):
        return [
            {'id': count.id, 'name': count.complete_name or count.name}
            for count in counts.sorted(lambda record: record.complete_name or record.name)
        ]

    @api.model
    def _get_selectable_counts(self, workcenters):
        return self._get_mapped_count_signals(workcenters).count_id

    @api.model
    def _get_available_counts(self, workcenters):
        selected = self._get_selectable_counts(workcenters) & self.env.user.mes_dashboard_count_ids
        return self._serialize_counts(selected)

    @api.model
    def _resolve_active_count(self, count_id, available):
        if not available:
            return self.env['mes.counts']
        available_ids = [entry['id'] for entry in available]
        requested = int(count_id) if count_id else 0
        if requested not in available_ids:
            last_used = self.env.user.mes_dashboard_last_count_id
            requested = last_used.id if last_used.id in available_ids else available_ids[0]
        return self.env['mes.counts'].browse(requested)

    @api.model
    def _resolve_window(self, workcenters, period_mode, period_hours, interval_minutes):
        now_local = self._now_local()
        if period_mode == 'shift':
            starts = []
            ends = []
            for workcenter in workcenters:
                shift_start, shift_end = self.env['mes.shift'].sudo().get_current_shift_window(workcenter)
                if shift_start:
                    starts.append(shift_start)
                    ends.append(shift_end)
            if starts:
                window_start = min(starts)
                window_end = self._align_up(min(now_local, max(ends)), interval_minutes)
                if window_end > window_start:
                    return window_start, window_end, 'shift'
        window_end = self._align_up(now_local, interval_minutes)
        return window_end - timedelta(hours=period_hours), window_end, 'hours'

    @api.model
    def _fit_interval(self, window_start, window_end, interval_minutes):
        span_minutes = max(1, int((window_end - window_start).total_seconds() // 60))
        for option in DASHBOARD_INTERVAL_OPTIONS:
            if option >= interval_minutes and span_minutes / option <= MAX_DASHBOARD_BUCKETS:
                return option
        return DASHBOARD_INTERVAL_OPTIONS[-1]

    @staticmethod
    def _build_buckets(grid_start, window_start, window_end, interval_minutes):
        step = timedelta(minutes=interval_minutes)
        buckets = []
        cursor = grid_start
        while cursor < window_end:
            stop = cursor + step
            data_start = max(cursor, window_start)
            data_stop = min(stop, window_end)
            if data_stop > data_start:
                buckets.append({
                    'start': cursor.strftime(TIMESTAMP_FORMAT),
                    'end': stop.strftime(TIMESTAMP_FORMAT),
                    'label': cursor.strftime('%H:%M'),
                    'long_label': cursor.strftime('%d.%m %H:%M'),
                    'partial': data_start > cursor or data_stop < stop,
                })
            cursor += step
        return buckets

    def _fetch_count_buckets(self, machine_names, tag_names, grid_start, window_start, window_end, interval_minutes):
        origin_string = grid_start.strftime('%Y-%m-%d %H:%M:%S')
        start_string = window_start.strftime('%Y-%m-%d %H:%M:%S')
        end_string = window_end.strftime('%Y-%m-%d %H:%M:%S')
        bucket_seconds = interval_minutes * 60
        with self._connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT machine_name,
                           tag_name,
                           FLOOR(EXTRACT(EPOCH FROM (time - %s::timestamptz)) / %s)::int AS bucket_idx,
                           COALESCE(SUM(value), 0) AS sum_val,
                           COALESCE(MAX(value) - MIN(value), 0) AS cum_val
                    FROM telemetry_count
                    WHERE machine_name = ANY(%s)
                      AND tag_name = ANY(%s)
                      AND time >= %s::timestamptz
                      AND time < %s::timestamptz
                    GROUP BY machine_name, tag_name, bucket_idx
                    ORDER BY bucket_idx
                """, (origin_string, bucket_seconds, machine_names, tag_names, start_string, end_string))
                return cursor.fetchall()

    @api.model
    def _build_rows(self, workcenters, count, grid_start, window_start, window_end,
                    interval_minutes, bucket_count):
        rows = []
        row_by_workcenter = {}
        owner_by_tag = {}
        cumulative_by_tag = {}
        machine_names = set()
        tag_names = set()

        for workcenter in workcenters:
            machine = workcenter.machine_settings_id
            signals = machine.count_tag_ids.filtered(
                lambda signal: signal.count_id == count and signal.tag_name
            ) if machine else self.env['mes.signal.count']
            row = {
                'workcenter_id': workcenter.id,
                'name': workcenter.display_name,
                'machine': machine.name if machine else '',
                'mapped': bool(signals),
                'values': [0.0] * bucket_count,
                'total': 0.0,
            }
            rows.append(row)
            row_by_workcenter[workcenter.id] = row
            for signal in signals:
                key = (machine.name, signal.tag_name)
                owner_by_tag.setdefault(key, workcenter.id)
                cumulative_by_tag[key] = signal.is_cumulative
                machine_names.add(machine.name)
                tag_names.add(signal.tag_name)

        if machine_names and tag_names and bucket_count:
            raw = self._fetch_count_buckets(
                sorted(machine_names), sorted(tag_names),
                grid_start, window_start, window_end, interval_minutes)
            for machine_name, tag_name, bucket_index, sum_value, cumulative_value in raw:
                key = (machine_name, tag_name)
                target = row_by_workcenter.get(owner_by_tag.get(key))
                if target is None or bucket_index is None:
                    continue
                if bucket_index < 0 or bucket_index >= bucket_count:
                    continue
                quantity = float(cumulative_value if cumulative_by_tag.get(key) else sum_value)
                target['values'][bucket_index] += quantity

        for row in rows:
            row['values'] = [round(value, 2) for value in row['values']]
            row['total'] = round(sum(row['values']), 2)
        return rows

    @api.model
    def _empty_payload(self, period_mode, period_hours, interval_minutes, message):
        return {
            'counts': [],
            'empty': True,
            'message': message,
            'buckets': [],
            'rows': [],
            'column_totals': [],
            'grand_total': 0,
            'interval_minutes': interval_minutes,
            'period_mode': period_mode,
            'effective_mode': period_mode,
            'period_hours': period_hours,
            'window_start': '',
            'window_end': '',
        }

    @api.model
    def _prepare_context(self, count_id, period_mode, period_hours, interval_minutes, workcenter_ids):
        interval_minutes = self._clamp_interval(interval_minutes)
        period_hours = self._clamp_hours(period_hours)
        if period_mode not in dict(DASHBOARD_PERIOD_MODES):
            period_mode = 'shift'

        workcenters = self._get_dashboard_workcenters(workcenter_ids)
        if not workcenters:
            return None, self._empty_payload(
                period_mode, period_hours, interval_minutes,
                'No work centers with telemetry settings are available')

        available = self._get_available_counts(workcenters)
        if not available:
            return None, self._empty_payload(
                period_mode, period_hours, interval_minutes,
                'No count tabs are selected. Use the Counts button to pick them.')

        count = self._resolve_active_count(count_id, available)
        window_start, window_end, effective_mode = self._resolve_window(
            workcenters, period_mode, period_hours, interval_minutes)
        interval_minutes = self._fit_interval(window_start, window_end, interval_minutes)
        grid_start = self._align_down(window_start, interval_minutes)

        context = {
            'workcenters': workcenters,
            'available': available,
            'count': count,
            'grid_start': grid_start,
            'window_start': window_start,
            'window_end': window_end,
            'interval_minutes': interval_minutes,
            'period_mode': period_mode,
            'period_hours': period_hours,
            'effective_mode': effective_mode,
        }
        return context, None

    @api.model
    def get_dashboard_config(self):
        user = self.env.user
        workcenters = self._get_dashboard_workcenters()
        selectable = self._get_selectable_counts(workcenters)
        available = self._get_available_counts(workcenters)
        active = self._resolve_active_count(False, available)
        return {
            'counts': available,
            'selectable_counts': self._serialize_counts(selectable),
            'selected_count_ids': (selectable & user.mes_dashboard_count_ids).ids,
            'workcenters': [{'id': workcenter.id, 'name': workcenter.display_name} for workcenter in workcenters],
            'interval_options': [
                {'value': int(value), 'label': label}
                for value, label in DASHBOARD_INTERVAL_SELECTION
            ],
            'period_hours_range': list(range(MIN_PERIOD_HOURS, MAX_PERIOD_HOURS + 1)),
            'active_count_id': active.id if active else False,
            'period_mode': user.mes_dashboard_period_mode or 'shift',
            'period_hours': self._clamp_hours(user.mes_dashboard_period_hours),
            'interval_minutes': self._clamp_interval(user.mes_dashboard_interval),
            'view_mode': user.mes_dashboard_view_mode or 'table',
        }

    @api.model
    def get_dashboard_data(self, count_id=False, period_mode='shift', period_hours=DEFAULT_PERIOD_HOURS,
                           interval_minutes=DEFAULT_INTERVAL_MINUTES, workcenter_ids=None):
        try:
            context, empty = self._prepare_context(
                count_id, period_mode, period_hours, interval_minutes, workcenter_ids)
            if empty:
                return empty

            buckets = self._build_buckets(
                context['grid_start'], context['window_start'],
                context['window_end'], context['interval_minutes'])
            rows = self._build_rows(
                context['workcenters'], context['count'], context['grid_start'],
                context['window_start'], context['window_end'],
                context['interval_minutes'], len(buckets))

            column_totals = [
                round(sum(row['values'][index] for row in rows), 2)
                for index in range(len(buckets))
            ]

            return {
                'counts': context['available'],
                'active_count_id': context['count'].id if context['count'] else False,
                'active_count_name': (context['count'].complete_name or context['count'].name) if context['count'] else '',
                'buckets': buckets,
                'rows': rows,
                'column_totals': column_totals,
                'grand_total': round(sum(column_totals), 2),
                'interval_minutes': context['interval_minutes'],
                'period_mode': context['period_mode'],
                'effective_mode': context['effective_mode'],
                'period_hours': context['period_hours'],
                'window_start': context['window_start'].strftime(TIMESTAMP_FORMAT),
                'window_end': context['window_end'].strftime(TIMESTAMP_FORMAT),
            }
        except Exception:
            _logger.exception(
                "get_dashboard_data failed count=%s mode=%s hours=%s interval=%s",
                count_id, period_mode, period_hours, interval_minutes)
            return {'error': 'Counts dashboard data is unavailable'}

    @api.model
    def get_dashboard_tail(self, count_id=False, period_mode='shift', period_hours=DEFAULT_PERIOD_HOURS,
                           interval_minutes=DEFAULT_INTERVAL_MINUTES, last_bucket_start=None,
                           workcenter_ids=None):
        try:
            context, empty = self._prepare_context(
                count_id, period_mode, period_hours, interval_minutes, workcenter_ids)
            if empty:
                return empty

            buckets = self._build_buckets(
                context['grid_start'], context['window_start'],
                context['window_end'], context['interval_minutes'])
            if not buckets:
                return self.get_dashboard_data(
                    count_id, period_mode, period_hours, interval_minutes, workcenter_ids)

            tail = buckets[-1]
            if not last_bucket_start or last_bucket_start != tail['start']:
                payload = self.get_dashboard_data(
                    count_id, period_mode, period_hours, interval_minutes, workcenter_ids)
                payload['full'] = True
                return payload

            tail_grid_start = datetime.strptime(tail['start'], TIMESTAMP_FORMAT)
            tail_window_start = max(tail_grid_start, context['window_start'])
            rows = self._build_rows(
                context['workcenters'], context['count'], tail_grid_start,
                tail_window_start, context['window_end'],
                context['interval_minutes'], 1)

            values = {str(row['workcenter_id']): row['values'][0] for row in rows}
            return {
                'full': False,
                'bucket': tail,
                'values': values,
                'column_total': round(sum(values.values()), 2),
                'window_start': context['window_start'].strftime(TIMESTAMP_FORMAT),
                'window_end': context['window_end'].strftime(TIMESTAMP_FORMAT),
            }
        except Exception:
            _logger.exception(
                "get_dashboard_tail failed count=%s mode=%s hours=%s interval=%s",
                count_id, period_mode, period_hours, interval_minutes)
            return {'error': 'Counts dashboard data is unavailable'}

    @api.model
    def save_dashboard_prefs(self, period_mode=None, period_hours=None, interval_minutes=None,
                             view_mode=None, count_id=None, count_ids=None):
        values = {}
        if period_mode in dict(DASHBOARD_PERIOD_MODES):
            values['mes_dashboard_period_mode'] = period_mode
        if period_hours is not None:
            values['mes_dashboard_period_hours'] = self._clamp_hours(period_hours)
        if interval_minutes is not None:
            values['mes_dashboard_interval'] = str(self._clamp_interval(interval_minutes))
        if view_mode in dict(DASHBOARD_VIEW_MODES):
            values['mes_dashboard_view_mode'] = view_mode
        if count_id:
            values['mes_dashboard_last_count_id'] = int(count_id)
        if count_ids is not None:
            selectable = self._get_selectable_counts(self._get_dashboard_workcenters())
            requested = {int(value) for value in count_ids}
            values['mes_dashboard_count_ids'] = [(6, 0, [
                count.id for count in selectable if count.id in requested
            ])]
        if not values:
            return True
        if set(values) - set(DASHBOARD_PREF_FIELDS):
            raise ValueError("Unexpected dashboard preference field")
        self.env.user.sudo().write(values)
        return True
