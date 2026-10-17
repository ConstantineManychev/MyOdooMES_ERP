import logging
from werkzeug.exceptions import NotFound, Forbidden

from odoo import http, fields, _
from odoo.http import request
from odoo.exceptions import UserError, AccessError

_logger = logging.getLogger(__name__)


class MesKioskController(http.Controller):

    @http.route('/mes/kiosk/auto', type='http', auth='public', website=False)
    def kiosk_autoresolve(self, wc=None, machine=None, **_kwargs):
        if wc:
            target = request.env['mrp.workcenter'].sudo().browse(self._safe_int(wc)).exists()
            if target:
                return self._redirect_to_kiosk(target)
        if machine:
            target = request.env['mrp.workcenter'].sudo().search([('name', '=', machine)], limit=1)
            if target:
                return self._redirect_to_kiosk(target)
        client_ip = request.env['res.users'].sudo()._get_real_client_ip()
        by_ip = request.env['res.users'].sudo()._resolve_workcenter_by_ip(client_ip)
        if by_ip:
            return self._redirect_to_kiosk(by_ip)
        return self._render_picker(client_ip)

    @http.route('/mes/kiosk/wc/<int:wc_id>', type='http', auth='public', website=False)
    def kiosk_by_id(self, wc_id, **_kwargs):
        target = request.env['mrp.workcenter'].sudo().browse(wc_id).exists()
        if not target:
            return self._render_picker(None)
        return self._redirect_to_kiosk(target)

    @http.route('/mes/kiosk/by-name/<string:machine_name>', type='http', auth='public', website=False)
    def kiosk_by_name(self, machine_name, **_kwargs):
        target = request.env['mrp.workcenter'].sudo().search([('name', '=', machine_name)], limit=1)
        if not target:
            return self._render_picker(None)
        return self._redirect_to_kiosk(target)

    @staticmethod
    def _safe_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    def _redirect_to_kiosk(self, workcenter):
        target = f"/web#action=mes_core.action_kiosk&workcenter_id={workcenter.id}"
        if not request.session.uid:
            return request.redirect(f'/web/login?redirect={target}')
        return request.redirect(target)

    def _render_picker(self, client_ip):
        workcenters = request.env['mrp.workcenter'].sudo().search([], order='name')
        return request.render('mes_core.kiosk_no_match', {
            'client_ip': client_ip or '(unknown)',
            'logged_in': bool(request.session.uid),
            'login': request.env.user.login if request.session.uid else None,
            'workcenters': workcenters,
        })

    @http.route('/mes/kiosk/auth/login', type='json', auth='user')
    def kiosk_login(self, workcenter_id, login=None, pin=None, password=None, fob_uid=None, force_role=None):
        Session = request.env['mes.kiosk.session'].sudo()
        wc = self._validate_workcenter_access(workcenter_id)
        pin_value = pin if pin is not None else password
        if fob_uid:
            employee = Session._resolve_employee_by_fob(fob_uid)
            method = 'fob'
        else:
            employee = Session._resolve_employee_by_pin(login, pin_value)
            method = 'manual'
        if not employee:
            return {'ok': False, 'error': 'Wrong PIN.'}
        force_role_clean = force_role if isinstance(force_role, str) else None
        try:
            session = Session.login(wc.id, employee, login_method=method, force_role=force_role_clean)
        except Exception as e:
            _logger.exception("kiosk_login failed wc=%s employee=%s", workcenter_id, employee.name)
            return {'ok': False, 'error': f'{type(e).__name__}: {e}'}
        return {
            'ok': True,
            'session_id': session.id,
            'employee_id': employee.id,
            'employee_name': employee.name,
            'job_title': employee.job_title or '',
            'role': session.role,
        }

    @http.route('/mes/kiosk/auth/fob', type='json', auth='user')
    def kiosk_fob_scan(self, workcenter_id, fob_uid, source=None, alternatives=None):
        Session = request.env['mes.kiosk.session'].sudo()
        wc = self._validate_workcenter_access(workcenter_id)
        employee = Session._resolve_employee_by_fob(fob_uid, alternatives)
        if not employee:
            Employee = request.env['hr.employee'].sudo()
            tried = []
            for candidate in [fob_uid] + list(alternatives or []):
                normalized = Employee._normalize_fob_uid(candidate)
                if normalized and normalized not in tried:
                    tried.append(normalized)
            return {
                'ok': False,
                'unknown_tag': True,
                'scanned_uid': str(fob_uid or ''),
                'candidates': tried,
                'error': 'This tag is not linked to any employee.',
            }
        method = 'fob'
        try:
            result = Session.toggle_fob_session(wc.id, employee, login_method=method)
        except Exception as exc:
            _logger.exception("kiosk_fob_scan failed wc=%s employee=%s", workcenter_id, employee.name)
            return {'ok': False, 'error': f'{type(exc).__name__}: {exc}'}
        session = result['session']
        return {
            'ok': True,
            'action': result['action'],
            'session_id': session.id if session else None,
            'employee_id': employee.id,
            'employee_name': employee.name,
            'job_title': employee.job_title or '',
            'role': result['role'],
            'source': source or 'reader',
        }

    @http.route('/mes/kiosk/auth/logout', type='json', auth='user')
    def kiosk_logout(self, session_id):
        session = request.env['mes.kiosk.session'].sudo().browse(session_id)
        if not session.exists():
            return {'ok': False, 'error': 'Unknown session.'}
        session.action_logout()
        return {'ok': True}

    @http.route('/mes/kiosk/chart_data', type='json', auth='user')
    def kiosk_chart_data(self, workcenter_id, selected_count_id=False, selected_process_id=False,
                         selected_text_process_id=False):
        wc = self._validate_workcenter_access(workcenter_id)
        result = request.env['mrp.workcenter'].sudo().get_live_chart_data(
            wc.id, selected_count_id, selected_process_id, selected_text_process_id
        )
        return result

    @http.route('/mes/kiosk/custom_chart_data', type='json', auth='user')
    def kiosk_custom_chart_data(self, workcenter_id, period_hours=2, bucket_minutes=5,
                                selected_count_id=False, selected_process_id=False,
                                selected_text_process_id=False):
        wc = self._validate_workcenter_access(workcenter_id)
        return request.env['mrp.workcenter'].sudo().get_custom_chart_data(
            wc.id, period_hours, bucket_minutes, selected_count_id, selected_process_id,
            selected_text_process_id
        )

    @http.route('/mes/kiosk/planned_downtime/toggle', type='json', auth='user')
    def kiosk_planned_downtime_toggle(self, workcenter_id, session_id=None, technician_session_id=None):
        wc = self._validate_workcenter_access(workcenter_id)
        Downtime = request.env['mes.planned.downtime'].sudo()
        session = wc.sudo()._kiosk_allowed_session(
            'planned_downtime', self._safe_int(session_id or technician_session_id))
        if not session:
            return {'ok': False, 'error': _("Log in with a role that may start or finish planned downtime.")}

        active = Downtime._active_pm_slots(wc)
        if active:
            try:
                Downtime._finish_now(active, session.employee_id.name)
            except (UserError, AccessError) as exc:
                return {'ok': False, 'error': str(exc)}
            return {'ok': True, 'active': False}
        try:
            Downtime.start_kiosk_downtime(wc, session.employee_id)
        except (UserError, AccessError) as exc:
            return {'ok': False, 'error': str(exc)}
        return {'ok': True, 'active': True}

    @http.route('/mes/kiosk/loss_breakdown', type='json', auth='user')
    def kiosk_loss_breakdown(self, workcenter_id, kind):
        wc = self._validate_workcenter_access(workcenter_id)
        mac = wc.sudo().machine_settings_id
        if not mac:
            return {'ok': False, 'error': 'No machine settings'}

        if kind == 'waste':
            Stat = request.env['mes.waste.loss.stat'].sudo()
            Stat.search([('machine_id', '=', mac.id)]).unlink()
            Stat._generate_stats(mac.id)
            rows = Stat.search([('machine_id', '=', mac.id)], order='waste_sum desc')
            counts_by_name = {c.name: c for c in request.env['mes.counts'].sudo().search([])}
            items = []
            for r in rows:
                count = counts_by_name.get(r.name)
                items.append({
                    'name': r.name,
                    'count_id': count.id if count else False,
                    'wheel': count.wheel if count else 0,
                    'module': count.module if count else 0,
                    'is_module_count': count.is_module_count if count else False,
                    'total': r.waste_sum,
                    'per_hour': r.waste_per_hour,
                })
            return {'ok': True, 'kind': 'waste', 'items': items}

        if kind == 'downtime':
            Stat = request.env['mes.downtime.loss.stat'].sudo()
            Stat.search([('machine_id', '=', mac.id)]).unlink()
            Stat._generate_stats(mac.id)
            rows = Stat.search([('machine_id', '=', mac.id)], order='total_time desc')
            return {
                'ok': True,
                'kind': 'downtime',
                'items': [{
                    'name': r.name,
                    'frequency': r.frequency,
                    'freq_per_hour': r.freq_per_hour,
                    'duration_min': r.total_time,
                    'duration_per_hour': r.time_per_hour,
                    'avg_duration_per_stop': r.avg_duration,
                } for r in rows],
            }

        return {'ok': False, 'error': f'Unknown kind: {kind}'}

    @http.route('/mes/kiosk/previous_shift_summary', type='json', auth='user')
    def kiosk_previous_shift_summary(self, workcenter_id):
        wc = self._validate_workcenter_access(workcenter_id)
        mac = wc.sudo().machine_settings_id
        if not mac:
            return {'ok': False, 'error': 'No machine settings'}
        Shift = request.env['mes.shift'].sudo()
        ps_loc, pe_loc = Shift.get_previous_worked_shift_window(wc)
        if not ps_loc:
            return {'ok': False, 'error': 'No previous shift with planned working time'}

        summary = mac._shift_kpi_summary(ps_loc, pe_loc)
        dt_list = mac._shift_downtime_list(ps_loc, pe_loc)
        waste_list = mac._shift_waste_list(ps_loc, pe_loc)

        dt_total_min = sum(d['duration_min'] for d in dt_list)
        waste_total = sum(w['total'] for w in waste_list)
        planned_minutes = mac._shift_planned_seconds(ps_loc, pe_loc) / 60.0
        dt_losses_pct = (dt_total_min / planned_minutes * 100.0) if planned_minutes > 0 else 0.0
        produced = summary.get('produced', 0.0)
        waste_pct = (waste_total / (produced + waste_total) * 100.0) if (produced + waste_total) > 0 else 0.0

        return {
            'ok': True,
            'oee': round(summary.get('oee', 0.0), 1),
            'dt_losses': round(dt_losses_pct, 1),
            'waste_losses': round(waste_pct, 1),
            'produced': produced,
            'produced_per_hour': round(summary.get('produced_per_hour', 0.0), 1),
            'produced_per_planned_hour': round(summary.get('produced_per_planned_hour', 0.0), 1),
            'runtime_hours': round(summary.get('runtime_hours', 0.0), 2),
            'planned_hours_elapsed': round(summary.get('planned_hours_elapsed', 0.0), 2),
            'shift_start': ps_loc.strftime('%Y-%m-%d %H:%M'),
            'shift_end': pe_loc.strftime('%Y-%m-%d %H:%M'),
            'planned_hours': round(planned_minutes / 60.0, 2),
        }

    @http.route('/mes/kiosk/shift_comparison', type='json', auth='user')
    def kiosk_shift_comparison(self, workcenter_id, kind):
        wc = self._validate_workcenter_access(workcenter_id)
        mac = wc.sudo().machine_settings_id
        if not mac:
            return {'ok': False, 'error': 'No machine settings'}
        Shift = request.env['mes.shift'].sudo()
        cs_loc, ce_loc = Shift.get_current_shift_window(wc)
        ps_loc, pe_loc = Shift.get_previous_worked_shift_window(wc)

        if kind == 'downtime':
            current = mac._shift_downtime_list(cs_loc, ce_loc) if cs_loc else []
            previous = mac._shift_downtime_list(ps_loc, pe_loc) if ps_loc else []
            return {'ok': True, 'kind': 'downtime', 'current': current, 'previous': previous}

        if kind == 'waste':
            current = mac._shift_waste_list(cs_loc, ce_loc) if cs_loc else []
            previous = mac._shift_waste_list(ps_loc, pe_loc) if ps_loc else []
            return {'ok': True, 'kind': 'waste', 'current': current, 'previous': previous}

        return {'ok': False, 'error': f'Unknown kind: {kind}'}

    @http.route('/mes/kiosk/state', type='json', auth='user')
    def kiosk_state(self, workcenter_id, include_pending=False):
        wc = self._validate_workcenter_access(workcenter_id)
        return self._serialize_workcenter_state(wc, include_pending=include_pending)

    @http.route('/mes/kiosk/help/prefill', type='json', auth='user')
    def kiosk_help_prefill(self, workcenter_id):
        wc = self._validate_workcenter_access(workcenter_id)
        prefill = request.env['mes.task'].sudo()._resolve_help_prefill(wc)
        prefill['ok'] = True
        return prefill

    @http.route('/mes/kiosk/help/stream_reasons', type='json', auth='user')
    def kiosk_help_stream_reasons(self, workcenter_id, stream_id):
        wc = self._validate_workcenter_access(workcenter_id)
        stream = request.env['mes.stream'].sudo().browse(self._safe_int(stream_id)).exists()
        if not stream:
            return {'ok': False, 'error': 'Stream not found'}
        reasons = request.env['mes.task'].sudo()._resolve_stream_reasons(wc, stream)
        return {'ok': True, 'stream': self._serialize_stream(stream), 'reasons': reasons}

    @http.route('/mes/kiosk/help/create', type='json', auth='user')
    def kiosk_help_create(self, workcenter_id, operator_session_id, payload):
        wc = self._validate_workcenter_access(workcenter_id)
        try:
            task = request.env['mes.task'].sudo().kiosk_create_help_request(
                wc.id, operator_session_id, payload or {}
            )
        except UserError as exc:
            return {'ok': False, 'error': str(exc)}
        return {'ok': True, 'task_id': task.id, 'state': task.state}

    @http.route('/mes/kiosk/help/<int:task_id>/supervisor_accept', type='json', auth='user')
    def kiosk_supervisor_accept(self, task_id, supervisor_session_id):
        return self._dispatch_action(task_id, 'action_supervisor_accept', supervisor_session_id)

    @http.route('/mes/kiosk/help/<int:task_id>/supervisor_close', type='json', auth='user')
    def kiosk_supervisor_close(self, task_id, comment=None):
        return self._dispatch_action(task_id, 'action_supervisor_close', comment)

    @http.route('/mes/kiosk/help/<int:task_id>/update_reason', type='json', auth='user')
    def kiosk_update_reason(self, task_id, payload=None, actor_role='supervisor'):
        return self._dispatch_action(task_id, 'action_update_reason', payload or {}, actor_role)

    @http.route('/mes/kiosk/help/<int:task_id>/request_technician', type='json', auth='user')
    def kiosk_request_technician(self, task_id, payload=None):
        return self._dispatch_action(task_id, 'action_supervisor_request_technician', payload or {})

    @http.route('/mes/kiosk/help/<int:task_id>/tech_arrive', type='json', auth='user')
    def kiosk_tech_arrive(self, task_id, technician_session_id):
        return self._dispatch_action(task_id, 'action_technician_arrive', technician_session_id)

    @http.route('/mes/kiosk/help/<int:task_id>/tech_pause', type='json', auth='user')
    def kiosk_tech_pause(self, task_id, comment=None, reason_overrides=None):
        return self._dispatch_action(task_id, 'action_technician_pause', comment, reason_overrides)

    @http.route('/mes/kiosk/help/<int:task_id>/tech_resume', type='json', auth='user')
    def kiosk_tech_resume(self, task_id, comment=None, reason_overrides=None):
        return self._dispatch_action(task_id, 'action_technician_resume', comment, reason_overrides)

    @http.route('/mes/kiosk/help/<int:task_id>/tech_reject', type='json', auth='user')
    def kiosk_tech_reject(self, task_id, comment=None, reason_overrides=None):
        return self._dispatch_action(task_id, 'action_technician_reject', comment, reason_overrides)

    @http.route('/mes/kiosk/help/<int:task_id>/tech_close', type='json', auth='user')
    def kiosk_tech_close(self, task_id, comment=None, reason_overrides=None):
        return self._dispatch_action(task_id, 'action_technician_close', comment, reason_overrides)

    @http.route('/mes/kiosk/help/<int:task_id>/supervisor_confirm', type='json', auth='user')
    def kiosk_supervisor_confirm(self, task_id):
        return self._dispatch_action(task_id, 'action_supervisor_confirm_subtask')

    @http.route('/mes/kiosk/help/<int:task_id>/operator_confirm', type='json', auth='user')
    def kiosk_operator_confirm(self, task_id, operator_session_id, confirmed, comment=None):
        return self._dispatch_action(task_id, 'action_operator_confirm', operator_session_id, confirmed, comment)

    def _validate_workcenter_access(self, workcenter_id):
        wc = request.env['mrp.workcenter'].sudo().browse(int(workcenter_id))
        if not wc.exists():
            raise NotFound()
        allowed_ids = request.env['res.users']._get_mes_accessible_workcenter_ids()
        if wc.id not in allowed_ids and not request.env.user.has_group('mes_core.group_mes_manager'):
            raise Forbidden()
        return wc

    def _dispatch_action(self, task_id, method, *args):
        task = request.env['mes.task'].sudo().browse(int(task_id))
        if not task.exists():
            return {'ok': False, 'error': 'Task not found.'}
        try:
            result = getattr(task, method)(*args)
        except (UserError, AccessError) as exc:
            return {'ok': False, 'error': str(exc)}
        except Exception as exc:
            _logger.exception("Kiosk action %s failed on task %s", method, task_id)
            return {'ok': False, 'error': f'{type(exc).__name__}: {exc}'}
        return {'ok': True, 'state': task.state, 'subtask_id': result.id if hasattr(result, 'id') else None}

    def _serialize_workcenter_state(self, wc, include_pending=False):
        Downtime = request.env['mes.planned.downtime'].sudo()
        open_requests = wc._get_open_help_requests()
        task = open_requests[:1]
        payload = {
            'workcenter': {
                'id': wc.id,
                'name': wc.name,
                'current_oee': wc.current_oee,
                'current_availability': wc.current_availability,
                'current_performance': wc.current_performance,
                'current_quality': wc.current_quality,
                'current_produced': wc.current_produced,
                'current_runtime_formatted': wc.current_runtime_formatted,
                'current_produced_per_hour': wc.current_produced_per_hour,
                'current_produced_per_planned_hour': wc.current_produced_per_planned_hour,
                'current_downtime_losses': wc.current_downtime_losses,
                'current_waste_losses': wc.current_waste_losses,
                'current_top_alarm': wc.current_top_alarm,
                'current_top_rejection': wc.current_top_rejection,
                'current_event': self._get_current_event(wc),
                'current_shift_key': self._current_shift_key(wc),
                'planned_downtime_active': bool(Downtime._active_pm_slots(wc)),
                'stream_layout': wc.get_stream_layout(),
            },
            'sessions': {
                'operator': self._serialize_session(wc.active_operator_session_id),
                'supervisor': self._serialize_session(wc.active_supervisor_session_id),
                'technician': self._serialize_session(wc.active_technician_session_id),
            },
            'permissions': wc.sudo()._kiosk_permissions(),
            'viewer_can_comment': bool(self._viewer_employee()),
            'help_request': self._serialize_task(task) if task else None,
            'help_requests': [self._serialize_task(t) for t in open_requests],
            'help_request_summaries': [self._serialize_task_summary(t) for t in open_requests],
            'supervisor_confirmation_required': (wc.company_id or request.env.company).mes_supervisor_confirmation_required,
            'header_badges': wc.sudo()._kiosk_header_badges(),
        }
        if include_pending:
            payload['pending_operator_stops'] = wc._get_pending_operator_stops()
        queued = request.env['mes.pending.comment'].sudo().search([
            ('machine_id', '=', wc.id),
            ('applied', '=', False),
        ], order='event_time desc', limit=1)
        if queued:
            payload['queued_comment'] = {
                'event_time': fields.Datetime.to_string(queued.event_time),
                'comment': queued.comment or '',
            }
        return payload

    def _current_shift_key(self, wc):
        s_loc, e_loc = request.env['mes.shift'].sudo().get_current_shift_window(wc)
        if not s_loc:
            return ''
        return f"{s_loc.strftime('%Y-%m-%dT%H:%M')}/{e_loc.strftime('%Y-%m-%dT%H:%M')}"

    def _serialize_stream(self, stream):
        return {
            'id': stream.id,
            'name': stream.name,
            'code': stream.code or '',
        }

    def _serialize_event_option(self, event):
        return {
            'id': event.id,
            'name': event.name,
            'complete_name': event.name,
            'color': event.color or '',
            'stream_id': event.stream_id.id or False,
            'part_id': event.part_id.id or False,
            'piece_id': event.piece_id.id or False,
        }

    def _get_current_event(self, wc):
        env = request.env
        status_by_model = {
            'mes.performance.alarm': ('alarm', 'Alarm', '#dc3545'),
            'mes.performance.slowing': ('slowing', 'Slowing', '#6c757d'),
            'mes.performance.running': ('running', 'Running', '#28a745'),
        }
        open_logs = []
        for model in status_by_model:
            open_logs.extend(env[model].sudo().search([
                ('performance_id.machine_id', '=', wc.id),
                ('end_time', '=', False),
            ]))
        if not open_logs:
            return None

        log = max(open_logs, key=lambda rec: rec.start_time)
        status, default_name, default_color = status_by_model[log._name]
        return {
            'id': log.id,
            'model': log._name,
            'status': status,
            'name': log.loss_id.name if log.loss_id else default_name,
            'color': log.loss_id.color or default_color,
            'start': fields.Datetime.to_string(log.start_time),
            'comment': log.comment or '',
            'provisional': bool(log._name == 'mes.performance.alarm' and log.reason_provisional),
            'is_manual_stop': bool(log.loss_id.is_manual_stop),
            'stream_id': log.loss_id.stream_id.id or None,
        }

    @http.route('/mes/kiosk/stream_manual_stop_events', type='json', auth='user')
    def kiosk_stream_manual_stop_events(self, workcenter_id, stream_id):
        wc = self._validate_workcenter_access(workcenter_id)
        stream = request.env['mes.stream'].sudo().browse(self._safe_int(stream_id)).exists()
        if not stream:
            return {'ok': False, 'error': 'Stream not found'}
        events = wc._get_manual_stop_events_for_stream(stream)
        parts = events.mapped('part_id')
        return {
            'ok': True,
            'stream': self._serialize_stream(stream),
            'events': [self._serialize_event_option(event) for event in events],
            'parts': [{
                'id': part.id,
                'name': part.name,
                'stream_id': part.stream_id.id or False,
                'pieces': [{'id': piece.id, 'name': piece.name}
                           for piece in part.piece_ids.sorted(lambda p: (p.sequence, p.name))],
            } for part in parts.sorted('name')],
        }

    @http.route('/mes/kiosk/status_event_replace', type='json', auth='user')
    def kiosk_status_event_replace(self, workcenter_id, model, event_id, new_event_id, comment=None, viewer=False):
        wc = self._validate_workcenter_access(workcenter_id)
        denied = self._comment_denied(wc, viewer)
        if denied:
            return denied
        rec = self._browse_performance_line(model, event_id, wc.id)
        if not rec:
            return {'ok': False, 'error': 'Event not found for this workcenter'}
        if not rec.loss_id.is_manual_stop:
            return {'ok': False, 'error': 'Current status is not a manual stop'}
        new_event = request.env['mes.event'].sudo().browse(self._safe_int(new_event_id)).exists()
        if not new_event or not new_event.is_manual_stop:
            return {'ok': False, 'error': 'Selected event is not a manual stop'}
        if not wc._is_manual_stop_available(new_event):
            return {'ok': False, 'error': 'Selected event is not available on this machine'}
        vals = {'loss_id': new_event.id}
        if comment is not None:
            vals['comment'] = (comment or '').strip()
        rec.write(vals)
        wc.sudo()._notify_kiosk_state_refresh()
        return {
            'ok': True,
            'event_name': new_event.name,
            'comment': rec.comment or '',
        }

    @http.route('/mes/kiosk/oee_breakdown', type='json', auth='user')
    def kiosk_oee_breakdown(self, workcenter_id):
        wc = self._validate_workcenter_access(workcenter_id)
        return request.env['mes.oee.breakdown'].build(wc)

    @http.route('/mes/kiosk/queue_comment', type='json', auth='user')
    def kiosk_queue_comment(self, workcenter_id, event_time, comment=None, new_event_id=None, viewer=False):
        wc = self._validate_workcenter_access(workcenter_id)
        denied = self._comment_denied(wc, viewer)
        if denied:
            return denied
        if not event_time:
            return {'ok': False, 'error': 'Missing status time'}

        new_event = None
        if new_event_id is not None:
            if new_event_id:
                new_event = request.env['mes.event'].sudo().browse(
                    self._safe_int(new_event_id)).exists()
                if not new_event or not new_event.is_manual_stop:
                    return {'ok': False, 'error': 'Selected event is not a manual stop'}
                if not wc._is_manual_stop_available(new_event):
                    return {'ok': False, 'error': 'Selected event is not available on this machine'}
            else:
                new_event = request.env['mes.event']

        record = request.env['mes.pending.comment'].queue_for_status(
            wc, event_time,
            comment=comment,
            new_event=new_event,
            employee=self._viewer_employee() if viewer else wc.active_operator_session_id.employee_id)
        return {
            'ok': True,
            'queued': bool(record),
            'cleared': not record,
            'comment': record.comment or '' if record else '',
            'new_event_id': record.new_event_id.id if record and record.new_event_id else False,
            'new_event_name': record.new_event_id.name if record and record.new_event_id else '',
        }

    @http.route('/mes/kiosk/pending_stops_bulk_update', type='json', auth='user')
    def kiosk_pending_stops_bulk_update(self, workcenter_id, entries,
                                        new_event_id=None, comment=None, viewer=False):
        wc = self._validate_workcenter_access(workcenter_id)
        denied = self._comment_denied(wc, viewer)
        if denied:
            return denied
        if not entries:
            return {'ok': False, 'error': 'Nothing selected'}

        new_event = request.env['mes.event']
        if new_event_id:
            new_event = new_event.sudo().browse(self._safe_int(new_event_id)).exists()
            if not new_event or not new_event.is_manual_stop:
                return {'ok': False, 'error': 'Selected event is not a manual stop'}
            if not wc._is_manual_stop_available(new_event):
                return {'ok': False, 'error': 'Selected event is not available on this machine'}

        cleaned_comment = (comment or '').strip()
        if not new_event and not cleaned_comment:
            return {'ok': False, 'error': 'Provide a comment or a stop reason'}

        updated = 0
        skipped = 0
        for entry in entries:
            rec = self._browse_performance_line(
                entry.get('model'), entry.get('id'), wc.id)
            if not rec or not rec.loss_id.is_manual_stop:
                skipped += 1
                continue
            vals = {}
            if new_event:
                vals['loss_id'] = new_event.id
            if cleaned_comment:
                vals['comment'] = cleaned_comment
            rec.write(vals)
            updated += 1
        if updated and new_event:
            wc.sudo()._notify_kiosk_state_refresh()
        return {'ok': True, 'updated': updated, 'skipped': skipped}

    @http.route('/mes/kiosk/event_comment', type='json', auth='user')
    def kiosk_event_comment(self, workcenter_id, model, event_id, comment, viewer=False):
        wc = self._validate_workcenter_access(workcenter_id)
        denied = self._comment_denied(wc, viewer)
        if denied:
            return denied
        rec = self._browse_performance_line(model, event_id, wc.id)
        if not rec:
            return {'ok': False, 'error': 'Event not found for this workcenter'}
        rec.write({'comment': (comment or '').strip()})
        return {'ok': True, 'comment': rec.comment or ''}

    def _viewer_employee(self):
        employee = request.env.user.sudo().employee_id
        return employee if employee.mes_kiosk_role else employee.browse()

    def _comment_denied(self, wc, viewer):
        if viewer:
            if self._viewer_employee():
                return None
            return {'ok': False,
                    'error': _("Comments need an employee with a kiosk role linked to your user.")}
        if not wc._active_kiosk_sessions():
            return {'ok': False, 'error': _("Log in to make changes.")}
        return None

    def _browse_performance_line(self, model, event_id, workcenter_id):
        allowed = ('mes.performance.alarm', 'mes.performance.slowing', 'mes.performance.running')
        if model not in allowed:
            return None
        rec = request.env[model].sudo().browse(self._safe_int(event_id))
        if not rec.exists() or rec.performance_id.machine_id.id != workcenter_id:
            return None
        return rec

    def _serialize_session(self, session):
        if not session:
            return None
        return {
            'id': session.id,
            'employee_id': session.employee_id.id,
            'employee_name': session.employee_id.name,
            'job_title': session.employee_id.job_title or '',
            'role': session.role,
            'login_at': fields.Datetime.to_string(session.login_at),
            'login_method': session.login_method,
        }

    def _serialize_task_summary(self, task):
        active_subtask = task.child_ids.filtered(
            lambda t: t.state not in ('done', 'cancel')
        ).sorted('id')[-1:]
        current = active_subtask or task
        return {
            'id': task.id,
            'name': task.name,
            'priority': task.priority,
            'priority_label': dict(task._fields['priority'].selection).get(task.priority, ''),
            'reason_label': task._format_reason_label(),
            'state': current.state,
            'state_label': dict(task._fields['state'].selection).get(current.state, current.state),
        }

    def _serialize_task(self, task):
        subtasks = task.child_ids.sorted('id')
        return {
            'id': task.id,
            'name': task.name,
            'state': task.state,
            'priority': task.priority,
            'reason_source': task.reason_source,
            'problem_area': task.problem_area,
            'reason_event_id': task.reason_event_id.id or None,
            'reason_count_id': task.reason_count_id.id or None,
            'reason_defect_id': task.reason_defect_id.id or None,
            'reason_label': task._format_reason_label(),
            'operator_description': task.operator_description,
            'supervisor_user_name': task.supervisor_session_id.employee_id.name if task.supervisor_session_id else None,
            'supervisor_accepted_at': fields.Datetime.to_string(task.supervisor_accepted_at) if task.supervisor_accepted_at else None,
            'closed_at': fields.Datetime.to_string(task.closed_at) if task.closed_at else None,
            'operator_confirmation_state': task.operator_confirmation_state,
            'operator_prompt_ready': task._is_operator_prompt_ready(),
            'last_closed_summary': task._last_closed_subtask_summary(),
            'confirmation_prompt_due_at': fields.Datetime.to_string(task.confirmation_prompt_due_at) if task.confirmation_prompt_due_at else None,
            'confirmation_prompt_remaining_sec': task._operator_prompt_remaining_sec(),
            'subtasks': [self._serialize_task(child) for child in subtasks],
            'technician_user_name': task.technician_session_id.employee_id.name if task.technician_session_id else None,
            'technician_arrived_at': fields.Datetime.to_string(task.technician_arrived_at) if task.technician_arrived_at else None,
        }
