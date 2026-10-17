import json
import logging
import psycopg2.extras
import pytz
from datetime import datetime, timedelta
from odoo import http, exceptions, fields
from odoo.http import request

log = logging.getLogger(__name__)

MACHINE_STATE_TAG = 'OEE.nMachineState'
STOP_REASON_TAG = 'OEE.nStopRootReason'
STOP_REASON_LOOKBACK_SEC = 180
MACHINE_STATE_STOPPED_VALUE = 1

class MesTelemetryApi(http.Controller):

    @staticmethod
    def _parse_ts(ts):
        s = str(ts).replace('T', ' ').replace('Z', '').strip()
        try:
            datetime.strptime(s[:26], '%Y-%m-%d %H:%M:%S.%f')
            return s[:26]
        except ValueError:
            datetime.strptime(s[:19], '%Y-%m-%d %H:%M:%S')
            return s[:19]

    @staticmethod
    def _number_or_text(val):
        try:
            return float(val), None
        except (TypeError, ValueError):
            text = str(val).replace('\x00', '').strip() if val is not None else ''
            if not text:
                raise ValueError('empty process value')
            return None, text[:255]

    def _parse_batch(self, batch, numeric='int'):
        if not batch:
            return [], 0
        res, rejected = [], 0
        now_utc = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
        for row in batch:
            try:
                if isinstance(row, dict):
                    ts, arr_ts, mac, tag, val, evt_id = row.get('time'), row.get('arrived_time', now_utc), row.get('machine_name'), row.get('tag_name'), row.get('value'), row.get('evt_id')
                elif len(row) == 6:
                    ts, arr_ts, mac, tag, val, evt_id = row
                else:
                    ts, arr_ts, mac, tag, val = row[:5]
                    evt_id = None
                if not mac or not tag or not isinstance(mac, str) or not isinstance(tag, str):
                    raise ValueError('bad machine/tag')
                ts = self._parse_ts(ts)
                arr_ts = self._parse_ts(arr_ts or now_utc)
                if evt_id is not None:
                    evt_id = str(evt_id)[:64]
                if numeric == 'float_or_text':
                    number, text = self._number_or_text(val)
                    res.append((ts, arr_ts, mac, tag, number, text, evt_id))
                    continue
                val = float(val)
                if numeric == 'int':
                    val = int(val)
                res.append((ts, arr_ts, mac, tag, val, evt_id))
            except Exception:
                rejected += 1
        return res, rejected

    @http.route('/mes/api/import_historical', type='json', auth='user', methods=['POST'], csrf=False)
    def import_hist(self, events=None, counts=None, processes=None, **kw):
        if not request.env.user.has_group('mes_core.group_mes_api_write'):
            raise exceptions.AccessDenied()

        evts, evts_rej = self._parse_batch(events, 'int')
        cnts, cnts_rej = self._parse_batch(counts, 'int')
        prcs, prcs_rej = self._parse_batch(processes, 'float_or_text')

        if evts_rej or cnts_rej or prcs_rej:
            log.warning("TX Import: rejected invalid rows events=%s counts=%s processes=%s",
                        evts_rej, cnts_rej, prcs_rej)

        db = request.env['mes.timescale.base']
        state_latest = self._extract_state_rows(evts)
        batch_reasons = self._extract_reason_rows(evts, set(state_latest)) if state_latest else {}
        state_previous, stored_reasons = (
            self._fetch_previous_states(db, state_latest) if state_latest else ({}, {}))

        try:
            with db._connection() as conn:
                with conn.cursor() as cur:
                    if evts: psycopg2.extras.execute_values(cur, "INSERT INTO telemetry_event (time, arrived_time, machine_name, tag_name, value, evt_id) VALUES %s ON CONFLICT DO NOTHING;", evts, page_size=10000)
                    if cnts: psycopg2.extras.execute_values(cur, "INSERT INTO telemetry_count (time, arrived_time, machine_name, tag_name, value, evt_id) VALUES %s ON CONFLICT DO NOTHING;", cnts, page_size=10000)
                    if prcs: psycopg2.extras.execute_values(cur, "INSERT INTO telemetry_process (time, arrived_time, machine_name, tag_name, value, value_str, evt_id) VALUES %s ON CONFLICT DO NOTHING;", prcs, page_size=10000)
            if state_latest:
                self._signal_state_changes(state_latest, state_previous,
                                           stored_reasons, batch_reasons)
                self._signal_reason_updates(state_latest, batch_reasons)
            self._clear_missing_flags(evts, cnts, prcs)
            return {
                'status': 'success',
                'events_rx': len(evts), 'counts_rx': len(cnts), 'processes_rx': len(prcs),
                'events_rej': evts_rej, 'counts_rej': cnts_rej, 'processes_rej': prcs_rej,
            }
        except Exception as e:
            log.error(f"TX Import Fault: {e}")
            return {'status': 'error', 'transient': True, 'message': str(e)}

    def _clear_missing_flags(self, *row_sets):
        received = {}
        for rows in row_sets:
            for row in rows:
                received.setdefault(row[2], set()).add(row[3])
        if not received:
            return
        try:
            Machine = request.env['mes.machine.settings'].sudo()
            for mac_name, tags in received.items():
                machine = Machine.search([('name', '=', mac_name)], limit=1)
                if machine:
                    machine.clear_missing_for_received(sorted(tags))
        except Exception:
            log.exception("Clearing missing-on-PLC flags failed")

    def _extract_state_rows(self, evts):
        latest = {}
        for ts, _arr_ts, mac, tag, val, _evt_id in evts:
            if tag != MACHINE_STATE_TAG:
                continue
            current = latest.get(mac)
            if current is None or ts >= current[0]:
                latest[mac] = (ts, val)
        return latest

    def _extract_reason_rows(self, evts, machines):
        latest = {}
        for ts, _arr_ts, mac, tag, val, _evt_id in evts:
            if tag != STOP_REASON_TAG or mac not in machines or not int(val):
                continue
            current = latest.get(mac)
            if current is None or ts >= current[0]:
                latest[mac] = (ts, int(val))
        return latest

    def _fetch_previous_states(self, db, state_latest):
        machines = sorted(state_latest)
        previous, reasons = {}, {}
        try:
            with db._connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT DISTINCT ON (machine_name) machine_name, time, value
                        FROM telemetry_event
                        WHERE machine_name = ANY(%s) AND tag_name = %s
                        ORDER BY machine_name, time DESC
                    """, (machines, MACHINE_STATE_TAG))
                    previous = {row[0]: (row[1], int(row[2])) for row in cur.fetchall()}
                    cur.execute("""
                        SELECT DISTINCT ON (machine_name) machine_name, time, value
                        FROM telemetry_event
                        WHERE machine_name = ANY(%s) AND tag_name = %s AND value <> 0
                          AND time > NOW() - INTERVAL '%s seconds'
                        ORDER BY machine_name, time DESC
                    """, (machines, STOP_REASON_TAG, STOP_REASON_LOOKBACK_SEC))
                    reasons = {row[0]: (row[1], int(row[2])) for row in cur.fetchall()}
        except Exception:
            log.exception("Machine state lookup failed")
        return previous, reasons

    def _signal_state_changes(self, state_latest, state_previous, stored_reasons, batch_reasons):
        changed = []
        for machine_name, (ts, value) in state_latest.items():
            previous = state_previous.get(machine_name)
            if previous is None:
                changed.append((machine_name, ts, value))
                continue
            prev_ts, prev_value = previous
            if self._is_older_than(ts, prev_ts):
                continue
            if int(value) != int(prev_value):
                changed.append((machine_name, ts, value))
        if not changed:
            return
        try:
            workcenters = request.env['mrp.workcenter'].sudo()
            found = workcenters.search([
                ('machine_settings_id.name', 'in', [name for name, _t, _v in changed])])
            by_machine = {wc.machine_settings_id.name: wc for wc in found}
            for machine_name, ts, value in changed:
                workcenter = by_machine.get(machine_name)
                if not workcenter:
                    continue
                reason = batch_reasons.get(machine_name) or stored_reasons.get(machine_name)
                workcenter._notify_kiosk_live_status(
                    value, self._telemetry_ts_to_utc(workcenter, ts),
                    reason[1] if reason else None)
                workcenter._enqueue_fsm_sync()
        except Exception:
            log.exception("Kiosk live status signal failed")

    def _signal_reason_updates(self, state_latest, batch_reasons):
        if not batch_reasons:
            return
        try:
            workcenters = request.env['mrp.workcenter'].sudo().search([
                ('machine_settings_id.name', 'in', list(batch_reasons))])
            for workcenter in workcenters:
                machine_name = workcenter.machine_settings_id.name
                state = state_latest.get(machine_name)
                if not state or int(state[1]) != MACHINE_STATE_STOPPED_VALUE:
                    continue
                workcenter._notify_kiosk_live_status(
                    state[1], self._telemetry_ts_to_utc(workcenter, state[0]),
                    batch_reasons[machine_name][1])
                workcenter._enqueue_fsm_sync()
        except Exception:
            log.exception("Kiosk reason update signal failed")

    def _telemetry_ts_to_utc(self, workcenter, ts):
        raw = str(ts)[:19]
        try:
            local = datetime.strptime(raw, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return raw
        tz_name = (workcenter.company_id.tz
                   or request.env.company.tz
                   or 'UTC')
        try:
            machine_tz = pytz.timezone(tz_name)
        except Exception:
            return raw
        return (machine_tz.localize(local, is_dst=False)
                .astimezone(pytz.utc).replace(tzinfo=None)
                .strftime('%Y-%m-%d %H:%M:%S'))

    def _is_older_than(self, ts, prev_ts):
        try:
            incoming = datetime.strptime(str(ts)[:19], '%Y-%m-%d %H:%M:%S')
            stored = prev_ts.replace(tzinfo=None) if hasattr(prev_ts, 'replace') else prev_ts
            return incoming < stored
        except Exception:
            return False

    @http.route('/mes/api/get_machine_config', type='json', auth='user', methods=['POST'])
    def get_mac_cfg(self, mac_name, **kw):
        if not request.env.user.has_group('mes_core.group_mes_api_read'):
            raise exceptions.AccessDenied()
            
        mac = request.env['mes.machine.settings'].sudo().search([('name', '=', mac_name)], limit=1)
        if not mac: return {'error': f"Machine {mac_name} not found"}
        
        tags = []
        seen_tag_names = set()

        def add_tag(tag_name, tag_type, poll_type, poll_frequency, is_cumul=False):
            if not tag_name or tag_name in seen_tag_names:
                return
            seen_tag_names.add(tag_name)
            tags.append({
                'tag_name': tag_name,
                'type': tag_type,
                'mode': poll_type,
                'interval_sec': (poll_frequency or 1000) / 1000.0,
                'is_cumul': is_cumul,
            })

        for ct in mac.count_tag_ids:
            add_tag(ct.tag_name, 'count', ct.poll_type, ct.poll_frequency,
                    bool(ct.is_cumulative or ct.count_id.is_cumulative))
        for et in mac.event_tag_ids:
            add_tag(et.tag_name, 'event', et.poll_type, et.poll_frequency)
        for pt in mac.process_tag_ids:
            add_tag(pt.tag_name, 'process', pt.poll_type, pt.poll_frequency)
        return {'tags': tags}

    @http.route('/mes/api/logger/missing_tags', type='json', auth='user', csrf=False)
    def api_logger_missing_tags(self, mac_name=None, tag_names=None, **kwargs):
        if not request.env.user.has_group('mes_core.group_mes_api_write'):
            raise exceptions.AccessDenied()

        tag_names = [t for t in (tag_names or []) if t]
        if not mac_name or not tag_names:
            return {'ok': False, 'error': 'mac_name and tag_names are required'}

        machine = request.env['mes.machine.settings'].sudo().search(
            [('name', '=', mac_name)], limit=1)
        if not machine:
            return {'ok': False, 'error': f'Machine settings not found: {mac_name}'}

        flagged = machine.flag_missing_tags(tag_names)
        return {'ok': True, 'flagged': flagged, 'requested': tag_names}

    @http.route('/mes/api/kiosk/fob_scan', type='json', auth='user', csrf=False)
    def api_kiosk_fob_scan(self, mac_name=None, workcenter_id=None, uid=None, alternatives=None, **kwargs):
        if not request.env.user.has_group('mes_core.group_mes_api_write'):
            raise exceptions.AccessDenied()

        Workcenter = request.env['mrp.workcenter'].sudo()
        if workcenter_id:
            wc = Workcenter.browse(int(workcenter_id)).exists()
        else:
            wc = Workcenter.search([('name', '=', mac_name)], limit=1)
        if not wc:
            return {'ok': False, 'error': f'Work center not found: {mac_name or workcenter_id}'}
        if not uid:
            return {'ok': False, 'error': 'No UID provided'}

        Session = request.env['mes.kiosk.session'].sudo()
        employee = Session._resolve_employee_by_fob(uid, alternatives)
        if not employee:
            log.info("Smart card not linked to any employee: uid=%s alternatives=%s", uid, alternatives)
            return {'ok': False, 'unknown_tag': True, 'scanned_uid': str(uid),
                    'error': 'This card is not linked to any employee.'}
        try:
            result = Session.toggle_fob_session(wc.id, employee, login_method='fob')
        except Exception as exc:
            log.exception("api_kiosk_fob_scan failed wc=%s uid=%s", wc.name, uid)
            return {'ok': False, 'error': f'{type(exc).__name__}: {exc}'}
        log.info("Smart card %s on %s: %s %s", uid, wc.name, result['action'], employee.name)
        return {'ok': True, 'action': result['action'], 'role': result['role'],
                'employee_name': employee.name}

    @http.route('/mes/api/logger/status', type='json', auth='user', methods=['POST'], csrf=False)
    def set_log_sts(self, mac_name, evt_type, ts, err_msg=None, **kw):
        if not request.env.user.has_group('mes_core.group_mes_api_write'):
            raise exceptions.AccessDenied()
            
        try:
            mac = request.env['mes.machine.settings'].sudo().search([('name', '=', mac_name)], limit=1)
            if not mac: return {'status': 'error', 'msg': 'mac_not_found'}
            dt_val = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
            f_map = {'conn': 'log_conn_dt', 'cfg_req': 'log_cfg_req_dt', 'cfg_ok': 'log_cfg_ok_dt', 'bind_req': 'log_bind_req_dt', 'bind_ok': 'log_bind_ok_dt', 'plc_recv': 'log_plc_recv_dt', 'odoo_send': 'log_odoo_send_dt', 'err': 'log_err_dt'}
            vals = {}
            if evt_type in f_map: vals[f_map[evt_type]] = dt_val
            if evt_type == 'err' and err_msg: vals['log_err_msg'] = err_msg
            if vals: mac.write(vals)
            return {'status': 'ok'}
        except Exception as e:
            return {'status': 'error', 'msg': str(e)}

    @http.route('/mes/api/logger/pulse', type='json', auth='user', methods=['POST'], csrf=False)
    def logger_pulse(self, mac_name=None, **payload):
        if not request.env.user.has_group('mes_core.group_mes_api_write'):
            raise exceptions.AccessDenied()
        machine = request.env['mes.machine.settings'].sudo().search([('name', '=', mac_name)], limit=1)
        if not machine:
            return {'status': 'error', 'msg': 'mac_not_found'}
        return {'status': 'ok', 'cut_after_sec': machine._register_pulse(payload)}

    def _logger_machine(self, mac_name):
        if not request.env.user.has_group('mes_core.group_mes_api_write'):
            raise exceptions.AccessDenied()
        return request.env['mes.machine.settings'].sudo().search([('name', '=', mac_name)], limit=1)

    @http.route('/mes/api/logger/update_check', type='json', auth='user', methods=['POST'], csrf=False)
    def logger_update_check(self, mac_name, sha256=None, config_hash=None, **kw):
        mac = self._logger_machine(mac_name)
        if not mac:
            return {'status': 'error', 'msg': 'mac_not_found'}
        reported = (sha256 or '')[:64]
        vals = {'logger_sha256': reported, 'logger_checked_at': fields.Datetime.now()}
        if config_hash is not None:
            vals['logger_config_hash'] = (config_hash or '')[:64] or False
        mac.write(vals)

        response = {'status': 'ok', 'update': None}
        target = request.env['mes.logger.release'].sudo()._target_for_machine(mac)
        if target and reported and target.sha256 != reported and not target._content_ok():
            log.error("Logger release %s is not offered to %s: its stored file is empty or damaged; "
                      "press Detect New Version to restore it", target.name, mac_name)
        elif target and reported and target.sha256 != reported:
            response['update'] = {
                'sha256': target.sha256,
                'size': target.size,
                'name': target.name,
            }
        if config_hash is not None:
            payload, digest = mac._logger_config_payload()
            if digest != config_hash:
                response['config'] = payload
                response['config_hash'] = digest
        return response

    @http.route('/mes/api/logger/download', type='json', auth='user', methods=['POST'], csrf=False)
    def logger_download(self, mac_name, sha256, **kw):
        mac = self._logger_machine(mac_name)
        if not mac:
            return {'status': 'error', 'msg': 'mac_not_found'}
        target = request.env['mes.logger.release'].sudo()._target_for_machine(mac)
        if not target or target.sha256 != sha256:
            log.warning("Logger download refused for %s: %s is not the version targeted at it",
                        mac_name, sha256)
            return {'status': 'error', 'msg': 'not_targeted'}
        if not target._content_ok():
            log.error("Logger download refused for %s: the stored file of %s is empty or damaged",
                      mac_name, target.name)
            return {'status': 'error', 'msg': 'content_damaged'}
        return {'status': 'ok', 'sha256': target.sha256, 'content': target._content_b64()}

    @http.route('/mes/api/snapshot', type='json', auth='user', methods=['POST'], csrf=False)
    def get_production_snapshot(self, **kwargs):
        if not request.env.user.has_group('mes_core.group_mes_api_read'):
            raise exceptions.AccessDenied()

        machine_numbers = kwargs.get('machine_numbers', [])
        requested_fields = [f.lower() for f in kwargs.get('fields', [])]
        target_time_str = kwargs.get('target_time')

        if not machine_numbers or not requested_fields or not target_time_str:
            return {'error': 'Missing required parameters'}

        try:
            target_utc = fields.Datetime.to_datetime(target_time_str.replace('T', ' ').replace('Z', '')[:19])
        except ValueError:
            return {'error': 'Invalid datetime format. Use YYYY-MM-DD HH:MM:SS'}

        workcenters = request.env['mrp.workcenter'].sudo().search([('machine_number', 'in', machine_numbers)])
        
        if not workcenters: 
            return {'error': 'Machines not found'}

        response_data = {'snapshot_time': target_utc.strftime('%Y-%m-%d %H:%M:%S UTC'), 'machines': []}

        for wc in workcenters:
            machine_set = wc.machine_settings_id
            
            if not machine_set: 
                continue

            company_tz = pytz.timezone(wc.company_id.tz or 'UTC')
            target_loc = pytz.utc.localize(target_utc).astimezone(company_tz).replace(tzinfo=None)
            curr_h = target_loc.hour + target_loc.minute / 60.0 + target_loc.second / 3600.0

            shifts = request.env['mes.shift'].sudo().search([('company_id', '=', wc.company_id.id)])
            val_shifts = shifts.filtered(lambda s: not s.workcenter_ids or wc.id in s.workcenter_ids.ids)

            active_shift = None
            shift_date = target_loc.date()

            for s in val_shifts:
                if s.start_hour < s.end_hour:
                    if s.start_hour <= curr_h < s.end_hour:
                        active_shift = s
                        break
                else:
                    if curr_h >= s.start_hour:
                        active_shift = s
                        break
                    elif curr_h < s.end_hour:
                        active_shift = s
                        shift_date -= timedelta(days=1)
                        break

            if not active_shift: 
                continue

            shift_start_loc = datetime.combine(shift_date, datetime.min.time()) + timedelta(hours=active_shift.start_hour)
            shift_end_loc = shift_start_loc + timedelta(hours=active_shift.duration)
            
            shift_start_utc = company_tz.localize(shift_start_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)
            shift_end_utc = company_tz.localize(shift_end_loc, is_dst=False).astimezone(pytz.utc).replace(tzinfo=None)

            if target_utc > shift_end_utc: 
                continue

            resolved_fields = []
            count_tags = []
            process_tags = []

            for field_name in requested_fields:
                matched = False
                for c_tag in machine_set.count_tag_ids:
                    if c_tag.count_id and c_tag.count_id.name.lower() == field_name:
                        resolved_fields.append({'requested_name': field_name, 'actual_name': c_tag.count_id.name, 'type': 'count', 'tag_name': c_tag.tag_name, 'is_cumulative': c_tag.is_cumulative})
                        count_tags.append(c_tag.tag_name)
                        matched = True
                        break
                
                if matched: 
                    continue

                for p_tag in machine_set.process_tag_ids:
                    if p_tag.process_id and p_tag.process_id.name.lower() == field_name:
                        resolved_fields.append({'requested_name': field_name, 'actual_name': p_tag.process_id.name, 'type': 'process', 'tag_name': p_tag.tag_name, 'is_bobbin': 'bobbin' in p_tag.process_id.name.lower()})
                        process_tags.append(p_tag.tag_name)
                        matched = True
                        break

            machine_result = {'machine_number': getattr(wc, 'machine_number', wc.id), 'machine_name': machine_set.name, 'shift': f"{shift_start_utc.strftime('%Y-%m-%d %H:%M:%S')} - {shift_end_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC", 'data': []}

            count_results = {}
            if count_tags:
                with request.env['mes.timescale.base']._connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT tag_name, COALESCE(SUM(value), 0), COALESCE(MAX(value) - MIN(value), 0)
                            FROM telemetry_count 
                            WHERE machine_name = %s AND tag_name = ANY(%s) AND time >= %s AND time <= %s 
                            GROUP BY tag_name
                        """, (machine_set.name, count_tags, shift_start_loc, target_loc))
                        for row in cur.fetchall():
                            count_results[row[0]] = {'sum': row[1], 'cum': row[2]}

            process_results = {}
            if process_tags:
                with request.env['mes.timescale.base']._connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            WITH first_vals AS (
                                SELECT tag_name, value, ROW_NUMBER() OVER(PARTITION BY tag_name ORDER BY time ASC) as rn
                                FROM telemetry_process 
                                WHERE machine_name = %s AND tag_name = ANY(%s) AND time >= %s AND time <= %s
                            ),
                            last_vals AS (
                                SELECT tag_name, value, ROW_NUMBER() OVER(PARTITION BY tag_name ORDER BY time DESC) as rn
                                FROM telemetry_process 
                                WHERE machine_name = %s AND tag_name = ANY(%s) AND time >= %s AND time <= %s
                            )
                            SELECT f.tag_name, f.value, l.value 
                            FROM first_vals f 
                            JOIN last_vals l ON f.tag_name = l.tag_name 
                            WHERE f.rn = 1 AND l.rn = 1
                        """, (machine_set.name, process_tags, shift_start_loc, target_loc, machine_set.name, process_tags, shift_start_loc, target_loc))
                        for row in cur.fetchall():
                            process_results[row[0]] = {'start': row[1], 'end': row[2]}

            for rf in resolved_fields:
                val = 0.0
                
                if rf['type'] == 'count':
                    dr = count_results.get(rf['tag_name'])
                    if dr: 
                        val = dr['cum'] if rf['is_cumulative'] else dr['sum']
                
                elif rf['type'] == 'process':
                    dr = process_results.get(rf['tag_name'])
                    if dr: 
                        val = abs(dr['end'] - dr['start']) if rf['is_bobbin'] else (dr['end'] - dr['start'])
                
                machine_result['data'].append({'field': rf['actual_name'], 'result': float(val)})

            response_data['machines'].append(machine_result)

        return response_data
