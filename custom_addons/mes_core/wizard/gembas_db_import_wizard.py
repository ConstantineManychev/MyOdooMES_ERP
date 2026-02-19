import logging
from datetime import timedelta
import pyodbc

from odoo import models, fields, api
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

QUERY_SHIFTS = """
    SELECT 
        Shift.AssetShiftID, 
        Shift.ShiftDate,
        Shift.AssetID AS AssetCode,
        Shift.AssetID AS MachineName,
        Shift.StartTime AS ShiftStartTime,
        Shift.EndTime AS ShiftEndTime,
        Shift.ShiftID AS ShiftName
    FROM 
        dbo.tblDATAssetShift AS Shift
    WHERE 
        Shift.ShiftDate BETWEEN ? AND ?
"""

QUERY_EVENTS = """
    SELECT
        Ev.StartTime AS StartTime,
        ISNULL(Comm.Comment, '') AS Comment,
        EvReason.Code AS AlarmCode,
        EvReason.Description AS Alarm,
        Cat.Description AS AlarmType,
        Ev.AssetID AS AssetCode
    FROM
        dbo.tblDATRawEventAuto AS Ev
        INNER JOIN dbo.tblCFGSignalEventReason AS SigReason
            ON Ev.Value = SigReason.PLCValue AND Ev.SignalID = SigReason.SignalID
        INNER JOIN dbo.tblCFGEventReason AS EvReason
            ON SigReason.EventReasonID = EvReason.ID
        INNER JOIN dbo.tblCFGEventCategory AS Cat
            ON EvReason.EventCategoryID = Cat.ID
        LEFT JOIN dbo.tblDATRawEventAutoComments AS Comm
            ON Ev.ID = Comm.RMAID
    WHERE
        Ev.StartTime >= ? AND Ev.StartTime <= ?
    ORDER BY
        Ev.AssetID, Ev.StartTime
"""

QUERY_COUNTS = """
    SELECT
        Co.RecordTime AS EndTime,
        Co.Value AS Amount,
        SigType.Description AS Rejection,
        Cat.Description AS Category,
        Def.Code AS Code,
        Co.AssetID AS AssetCode
    FROM
        dbo.tblDATRawCount AS Co
        INNER JOIN dbo.tblCFGSignal AS Sig ON Co.SignalID = Sig.ID
        INNER JOIN dbo.tblCFGCountDefinition AS Def ON Sig.SignalTypeID = Def.SignalTypeID
        LEFT JOIN dbo.tblCFGSignalType AS SigType ON Def.SignalTypeID = SigType.ID
        LEFT JOIN dbo.tblCFGCountCategory AS Cat ON Def.CountCategoryID = Cat.ID
    WHERE
        Co.RecordTime >= ? AND Co.RecordTime <= ?
        AND Sig.Active = 1
    ORDER BY
        Co.AssetID, Co.RecordTime
"""

class ExternalImportWizard(models.TransientModel):
    _name = 'mes.external.import.wizard'
    _description = 'Import Events and Counts from Gemba'

    start_date = fields.Date(string='Start Date', default=fields.Date.context_today)
    end_date = fields.Date(string='End Date', default=fields.Date.context_today)
    clear_existing = fields.Boolean(
        string='Force Overwrite', 
        default=False, 
        help="If checked, existing alarms and rejections for these shifts will be wiped and re-imported."
    )

    def action_load_data(self):
        data = self._extract_and_transform_data()
        self._load_data_to_odoo(data)
        
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Import Completed',
                'message': f'Processed {len(data)} shift reports.',
                'type': 'success',
                'sticky': False,
            }
        }

    def _get_connection(self):
        params = self.env['ir.config_parameter'].sudo()
        server = params.get_param('gemba.sql_server')
        database = params.get_param('gemba.sql_database')
        username = params.get_param('gemba.sql_user')
        password = params.get_param('gemba.sql_password')
        
        if not all([server, database, username, password]):
             raise UserError("MS SQL Connection settings are missing. Check Configuration.")
        
        conn_str = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={server};DATABASE={database};'
            f'UID={username};PWD={password};TrustServerCertificate=yes;'
        )
        try:
            return pyodbc.connect(conn_str, timeout=15)
        except pyodbc.Error as e:
            raise UserError(f"Database connection failed: {e}")

    def _extract_and_transform_data(self):
        conn = self._get_connection()
        cursor = conn.cursor()

        cursor.execute(QUERY_SHIFTS, (self.start_date, self.end_date))
        shifts_raw = cursor.fetchall()
        
        if not shifts_raw:
            conn.close()
            return {}

        min_date = min(r.ShiftStartTime for r in shifts_raw)
        max_date = max(r.ShiftEndTime for r in shifts_raw)
        search_start = min_date - timedelta(days=1)

        reports = {}
        for row in shifts_raw:
            key = (row.AssetCode, row.ShiftDate, str(row.ShiftName))
            reports[key] = {
                'machine_name': row.MachineName,
                'asset_code': row.AssetCode,
                'doc_date': row.ShiftDate,
                'shift_name': str(row.ShiftName),
                'start_time': row.ShiftStartTime,
                'end_time': row.ShiftEndTime,
                'alarms': [],
                'rejections': []
            }

        cursor.execute(QUERY_EVENTS, (search_start, max_date))
        events_raw = cursor.fetchall()

        events_by_asset = {}
        for ev in events_raw:
            events_by_asset.setdefault(ev.AssetCode, []).append(ev)

        current_time = fields.Datetime.now()
        for asset, ev_list in events_by_asset.items():
            for i in range(len(ev_list)):
                ev = ev_list[i]
                next_ev = ev_list[i+1] if i < len(ev_list) - 1 else None
                raw_end = next_ev.StartTime if next_ev else None

                for r_key, r_data in reports.items():
                    if r_data['asset_code'] != asset:
                        continue
                    
                    calc_end = raw_end
                    if not calc_end:
                        calc_end = current_time if r_data['end_time'] > current_time else r_data['end_time']

                    if ev.StartTime < r_data['end_time'] and calc_end > r_data['start_time']:
                        start_val = max(ev.StartTime, r_data['start_time'])
                        end_val = min(calc_end, r_data['end_time'])
                        
                        if end_val > start_val:
                            r_data['alarms'].append({
                                'code': ev.AlarmCode,
                                'name': ev.Alarm,
                                'type': ev.AlarmType,
                                'start': start_val,
                                'end': end_val,
                                'comment': ev.Comment
                            })

        cursor.execute(QUERY_COUNTS, (min_date, max_date))
        counts_raw = cursor.fetchall()
        
        for count in counts_raw:
            for r_key, r_data in reports.items():
                if r_data['asset_code'] == count.AssetCode and r_data['start_time'] <= count.EndTime <= r_data['end_time']:
                    r_data['rejections'].append({
                        'code': count.Code,
                        'name': count.Rejection,
                        'category': count.Category,
                        'end_time': count.EndTime,
                        'amount': count.Amount
                    })
                    break

        conn.close()
        return reports

    def _load_data_to_odoo(self, data_dict):
        if not data_dict:
            return

        shift_map = self._sync_shifts(data_dict)
        machine_map = self._sync_machines(data_dict)
        loss_map = self._sync_alarm_reasons(data_dict)
        count_map = self._sync_count_reasons(data_dict)

#TODO: Change default product after VerifySystem sync
        default_product = self.env['product.product'].search([('detailed_type', '=', 'product')], limit=1)

        PerfObj = self.env['mes.machine.performance']
        
        existing_reports = PerfObj.search([
            ('date', '>=', self.start_date),
            ('date', '<=', self.end_date)
        ])
        rep_lookup = {
            (rep.machine_id.id, rep.date, rep.shift_id.id): rep 
            for rep in existing_reports
        }

        new_reports_vals = []
        reports_to_process = []

        for key, row in data_dict.items():
            machine_id = machine_map.get(row['machine_name'])
            shift_id = shift_map.get(row['shift_name'])
            if not machine_id or not shift_id:
                continue

            lookup_key = (machine_id, row['doc_date'], shift_id)
            report = rep_lookup.get(lookup_key)

            if not report:
                new_reports_vals.append({
                    'machine_id': machine_id,
                    'date': row['doc_date'],
                    'shift_id': shift_id,
                    'state': 'draft'
                })
            
            reports_to_process.append((lookup_key, row))

        if new_reports_vals:
            created_reports = PerfObj.create(new_reports_vals)
            for rep in created_reports:
                rep_lookup[(rep.machine_id.id, rep.date, rep.shift_id.id)] = rep

        for lookup_key, row in reports_to_process:
            report = rep_lookup.get(lookup_key)
            if not report:
                continue

            write_vals = {}
            
            if row['alarms']:
                current_len = len(report.alarm_ids)
                if self.clear_existing or current_len < len(row['alarms']):
                    commands = [(5, 0, 0)] 
                    for alarm in row['alarms']:
                        commands.append((0, 0, {
                            'loss_id': loss_map.get(alarm['code']),
                            'start_time': alarm['start'],
                            'end_time': alarm['end'],
                            'comment': alarm['comment']
                        }))
                    write_vals['alarm_ids'] = commands

            if row['rejections'] and default_product:
                current_len = len(report.rejection_ids)
                if self.clear_existing or current_len < len(row['rejections']):
                    commands = [(5, 0, 0)]
                    for rej in row['rejections']:
                        commands.append((0, 0, {
                            'product_id': default_product.id,
                            'reason_id': count_map.get(rej['code']),
                            'qty': rej['amount'],
                        }))
                    write_vals['rejection_ids'] = commands

            if write_vals:
                report.write(write_vals)

    def _sync_shifts(self, data_dict):
        unique_shifts = set(
            {'1. Mornings': 'Morning', 'Morning': 'Morning', 
             '2. Afternoons': 'Afternoon', 'Afternoon': 'Afternoon',
             '3. Nights': 'Night', 'Night': 'Night'}.get(v['shift_name'], 'Morning') 
            for v in data_dict.values()
        )
        
        Shift = self.env['mes.shift']
        existing = {s.name: s.id for s in Shift.search([('name', 'in', list(unique_shifts))])}
        
        new_vals = [{'name': name} for name in unique_shifts if name not in existing]
        if new_vals:
            created = Shift.create(new_vals)
            existing.update({s.name: s.id for s in created})
            
        return {
            v['shift_name']: existing.get(
                {'1. Mornings': 'Morning', 'Morning': 'Morning', 
                 '2. Afternoons': 'Afternoon', 'Afternoon': 'Afternoon',
                 '3. Nights': 'Night', 'Night': 'Night'}.get(v['shift_name'], 'Morning')
            ) for v in data_dict.values()
        }

    def _sync_machines(self, data_dict):
        unique_raw = set(v['machine_name'] for v in data_dict.values())
        parsed_data = {}
        
        for raw_name in unique_raw:
            parts = raw_name.split('-')
            imatec_name = parts[1].strip() if len(parts) > 1 else raw_name
            m_num = ''.join(filter(str.isdigit, parts[0])) if len(parts) > 1 else '0'
            parsed_data[raw_name] = {
                'name': raw_name,
                'code_imatec': imatec_name,
                'machine_number': int(m_num) if m_num else 0
            }

        Machine = self.env['mrp.workcenter']
        existing = Machine.search([
            '|', ('code_imatec', 'in', [p['code_imatec'] for p in parsed_data.values()]),
                 ('name', 'in', list(unique_raw))
        ])
        
        machine_map = {}
        for raw_name, p_data in parsed_data.items():
            match = next((m for m in existing if m.code_imatec == p_data['code_imatec'] or m.name == raw_name), None)
            if match:
                machine_map[raw_name] = match.id
            else:
                new_m = Machine.create(p_data)
                machine_map[raw_name] = new_m.id
                
        return machine_map

    def _sync_alarm_reasons(self, data_dict):
        data_list = []
        for row in data_dict.values():
            for a in row['alarms']:
                data_list.append({
                    'code': a['code'],
                    'name': a['name'][:100] if a['name'] else 'Unknown Alarm',
                    'parent_name': a['type'], 
                    'vals': {}
                })
                
        self.env['mes.event'].sync_batch(data_list)
        return {e.code: e.id for e in self.env['mes.event'].search([('code', '!=', False)])}

    def _sync_count_reasons(self, data_dict):
        data_list = []
        for row in data_dict.values():
            for r in row['rejections']:
                data_list.append({
                    'code': r['code'],
                    'name': r['name'][:100] if r['name'] else 'Unknown Defect',
                    'parent_name': r['category'],
                    'vals': {}
                })
                
        self.env['mes.counts'].sync_batch(data_list)
        return {c.code: c.id for c in self.env['mes.counts'].search([('code', '!=', False)])}