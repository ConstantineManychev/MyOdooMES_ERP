import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Any

import pyodbc
from odoo import models, fields, api
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

# --- SQL CONSTANTS ---
QUERY_SHIFTS = """
    SELECT 
        Shift.AssetShiftID, 
        Shift.ShiftDate,
        Shift.AssetID AS AssetCode,
        Shift.AssetID AS MachineName,
        Shift.StartTime AS ShiftStartTime,
        Shift.EndTime AS ShiftEndTime,
        Shift.ShiftID
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

class ExternalImportWizard(models.TransientModel):
    _name = 'mes.external.import.wizard'
    _description = 'Import Data from Gemba'

    start_date = fields.Date(string='Start Date', default=fields.Date.context_today)
    end_date = fields.Date(string='End Date', default=fields.Date.context_today)
    clear_existing = fields.Boolean(string='Clear Existing Data', default=False, 
                                  help="If checked, existing alarms for these shifts will be deleted and re-imported.")

    def action_load_data(self):
        """Main execution method triggered by button."""
        # 1. Fetch Data
        imported_data = self._get_data_from_external_db(self.start_date, self.end_date)
        
        # 2. Process & Save to Odoo
        self._create_performance_records(imported_data)
        
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Success',
                'message': f'Processed {len(imported_data)} shift records successfully.',
                'type': 'success',
                'sticky': False,
            }
        }

    def _get_connection_string(self) -> str:
        params = self.env['ir.config_parameter'].sudo()
        server = params.get_param('gemba.sql_server')
        database = params.get_param('gemba.sql_database')
        username = params.get_param('gemba.sql_user')
        password = params.get_param('gemba.sql_password')
        
        if not all([server, database, username, password]):
             raise UserError("SQL Connection settings are missing! Please configure them in Settings.")
        
        return (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password};'
            'TrustServerCertificate=yes;'
        )

    def _get_data_from_external_db(self, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        connection_string = self._get_connection_string()

        try:
            conn = pyodbc.connect(connection_string, timeout=10)
            cursor = conn.cursor()
            
            # --- 1. Fetch Shifts ---
            shifts: List[Dict[str, Any]] = []
            cursor.execute(QUERY_SHIFTS, (start_date, end_date))
            rows_shifts = cursor.fetchall()
            
            if not rows_shifts:
                conn.close()
                return []

            min_date = rows_shifts[0].ShiftStartTime
            max_date = rows_shifts[0].ShiftEndTime

            for row in rows_shifts:
                shifts.append({
                    'ShiftDate': row.ShiftDate,
                    'AssetCode': row.AssetCode,
                    'MachineName': row.MachineName, 
                    'ShiftStartTime': row.ShiftStartTime,
                    'ShiftEndTime': row.ShiftEndTime,
                    'ShiftName': str(row.ShiftID) # External Shift Name (e.g. "1. Mornings")
                })
                if row.ShiftStartTime < min_date: min_date = row.ShiftStartTime
                if row.ShiftEndTime > max_date: max_date = row.ShiftEndTime

            # --- 2. Fetch Events ---
            search_start = min_date - timedelta(days=1)
            cursor.execute(QUERY_EVENTS, (search_start, max_date))
            rows_events = cursor.fetchall()
            
            events: List[Dict[str, Any]] = []
            for row in rows_events:
                events.append({
                    'StartTime': row.StartTime,
                    'Comment': row.Comment,
                    'AlarmCode': row.AlarmCode,
                    'Alarm': row.Alarm,
                    'AssetCode': row.AssetCode,
                    'CalculatedEndTime': None 
                })

            conn.close()

            # --- 3. Logic Processing (Clamping & Merging) ---
            if not events:
                return []

            # 3.1. Fill Gaps (CalculatedEndTime)
            for i in range(len(events)):
                current_event = events[i]
                next_event = events[i+1] if i < len(events) - 1 else None
                
                if next_event and next_event['AssetCode'] == current_event['AssetCode']:
                    current_event['CalculatedEndTime'] = next_event['StartTime']
                else:
                    current_event['CalculatedEndTime'] = None

            # 3.2. Merge Logic
            result_data: List[Dict[str, Any]] = []
            current_time = fields.Datetime.now()

            for shift in shifts:
                machine_events = [e for e in events if e['AssetCode'] == shift['AssetCode']]
                valid_alarms: List[Dict[str, Any]] = []
                
                for event in machine_events:
                    raw_end_time = event['CalculatedEndTime']
                    
                    # Fix Open-Ended Events
                    if not raw_end_time:
                        if shift['ShiftEndTime'] < current_time:
                            raw_end_time = shift['ShiftEndTime']
                        else:
                            raw_end_time = current_time
                    
                    # Check Overlap
                    if (event['StartTime'] < shift['ShiftEndTime']) and (raw_end_time > shift['ShiftStartTime']):
                        start_val = max(event['StartTime'], shift['ShiftStartTime'])
                        end_val = raw_end_time
                        
                        # Clamp to shift end
                        if raw_end_time > shift['ShiftEndTime']:
                             end_val = shift['ShiftEndTime']

                        if end_val > start_val:
                            valid_alarms.append({
                                'AlarmCode': event['AlarmCode'],
                                'Alarm': event['Alarm'],
                                'Comment': event['Comment'],
                                'StartTime': start_val,
                                'EndTime': end_val
                            })

                result_data.append({
                    'MachineName': shift['MachineName'],
                    'DocDate': shift['ShiftDate'],
                    'ExternalShiftName': shift['ShiftName'], # Pass raw name to helper
                    'Alarms': valid_alarms
                })
                    
            return result_data

        except Exception as e:
            _logger.error(f"External DB Import Failed: {e}")
            raise UserError(f"Import Failed: {e}")

    def _create_performance_records(self, data: List[Dict[str, Any]]) -> None:
        """
        Creates or updates entries in mes.machine.performance.
        """
        PerformanceModel = self.env['mes.machine.performance']
        WorkcenterModel = self.env['mrp.workcenter']
        LossModel = self.env['mrp.workcenter.productivity.loss']
        AlarmModel = self.env['mes.performance.alarm']
        
        for row in data:
            # 1. Resolve Machine (using our custom method)
            machine = WorkcenterModel.get_or_create_from_external(row['MachineName'])
            
            # 2. Resolve Shift
            shift = self._find_shift_by_external_name(row['ExternalShiftName'])
            
            # 3. Find or Create Header Document
            performance_doc = PerformanceModel.search([
                ('machine_id', '=', machine.id),
                ('date', '=', row['DocDate']),
                ('shift_id', '=', shift.id)
            ], limit=1)
            
            if not performance_doc:
                performance_doc = PerformanceModel.create({
                    'machine_id': machine.id,
                    'date': row['DocDate'],
                    'shift_id': shift.id,
                    'state': 'draft'
                })

            # 4. Process Alarms
            existing_count = len(performance_doc.alarm_ids)
            incoming_count = len(row['Alarms'])
            
            should_update = True
            if existing_count > 0:
                if self.clear_existing or (existing_count < incoming_count):
                    performance_doc.alarm_ids.unlink() # Clean old lines
                else:
                    should_update = False # Do not overwrite if we have more data locally
            
            if should_update and row['Alarms']:
                alarms_to_create = []
                for alarm_data in row['Alarms']:
                    
                    # Resolve Alarm Reason (Loss)
                    loss_reason = LossModel.search([('alarm_code', '=', alarm_data['AlarmCode'])], limit=1)
                    if not loss_reason:
                        loss_reason = LossModel.create({
                            'name': alarm_data['Alarm'],
                            'alarm_code': alarm_data['AlarmCode'],
                            'category': 'availability',
                            'manual': True
                        })
                    
                    alarms_to_create.append({
                        'performance_id': performance_doc.id,
                        'loss_id': loss_reason.id,
                        'start_time': alarm_data['StartTime'],
                        'end_time': alarm_data['EndTime'],
                        'comment': alarm_data['Comment']
                    })
                
                if alarms_to_create:
                    AlarmModel.create(alarms_to_create)

    def _find_shift_by_external_name(self, external_name: str):
        """
        Maps Gemba strings to Odoo mes.shift records.
        """
        ShiftModel = self.env['mes.shift']
        
        # Mapping Logic: External String -> Odoo Search Term
        # Adjust these keys based on what exactly comes from SQL
        mapping = {
            '1. Mornings': 'Morning',
            'Morning': 'Morning',
            '2. Afternoons': 'Afternoon',
            'Afternoon': 'Afternoon',
            'Night': 'Night',
            '3. Nights': 'Night'
        }
        
        target_name = mapping.get(external_name, 'Morning') # Default to Morning if unknown
        
        shift = ShiftModel.search([('name', '=', target_name)], limit=1)
        if not shift:
            # Fallback: create if not exists to prevent crash
            shift = ShiftModel.create({'name': target_name})
            
        return shift