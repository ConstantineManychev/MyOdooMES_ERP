import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional

import pyodbc
from odoo import models, fields, api
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

class ExternalImportWizard(models.TransientModel):
    _name = 'mes.external.import.wizard'
    _description = 'Import Data from External DB'

    start_date = fields.Date(string='Start Date', default=fields.Date.context_today)
    end_date = fields.Date(string='End Date', default=fields.Date.context_today)
    clear_existing = fields.Boolean(string='Clear Existing Alarms', default=False)

    def action_load_data(self) -> Dict[str, Any]:
        """
        Main entry point (Action) triggered by the UI button.
        Coordinates fetching data and loading it into Odoo.
        """
        # 1. Get raw data from external DB
        imported_data: List[Dict[str, Any]] = self._get_data_from_external_db(
            self.start_date, 
            self.end_date
        )
        
        # 2. Process and save data
        self._load_merged_events(imported_data)
        
        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Success',
                'message': 'Data import completed successfully',
                'type': 'success',
                'sticky': False,
            }
        }

    def _get_connection_string(self) -> str:
        """Constructs ODBC connection string from System Parameters."""
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
        """
        Connects to MS SQL, fetches shifts and alarms, and merges them.
        Returns a structured list of documents ready for import.
        """
        connection_string = self._get_connection_string()

        try:
            conn = pyodbc.connect(connection_string, timeout=10)
            cursor = conn.cursor()
            
            # --- 1. Fetch Shifts ---
            # Using 'shifts' instead of 'vt_shifts' (PEP 8 clean naming)
            shifts: List[Dict[str, Any]] = []
            
            query_shifts = """
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
            cursor.execute(query_shifts, (start_date, end_date))
            rows_shifts = cursor.fetchall()
            
            if not rows_shifts:
                conn.close()
                return []

            # Initial bounds for event search
            min_date: datetime = rows_shifts[0].ShiftStartTime
            max_date: datetime = rows_shifts[0].ShiftEndTime

            for row in rows_shifts:
                shifts.append({
                    'AssetShiftID': row.AssetShiftID,
                    'ShiftDate': row.ShiftDate,
                    'AssetCode': row.AssetCode,
                    'MachineName': row.MachineName, 
                    'ShiftStartTime': row.ShiftStartTime,
                    'ShiftEndTime': row.ShiftEndTime,
                    'ShiftID': str(row.ShiftID)
                })
                
                # Expand time window
                if row.ShiftStartTime < min_date: 
                    min_date = row.ShiftStartTime
                if row.ShiftEndTime > max_date: 
                    max_date = row.ShiftEndTime

            # --- 2. Fetch Events (Alarms) ---
            search_start = min_date - timedelta(days=1)
            
            query_events = """
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
            
            cursor.execute(query_events, (search_start, max_date))
            rows_events = cursor.fetchall()
            
            events: List[Dict[str, Any]] = []
            for row in rows_events:
                events.append({
                    'StartTime': row.StartTime,
                    'Comment': row.Comment,
                    'AlarmCode': row.AlarmCode,
                    'Alarm': row.Alarm,
                    'AlarmType': row.AlarmType,
                    'AssetCode': row.AssetCode,
                    'CalculatedEndTime': None 
                })

            conn.close()

            # --- 3. Logic Processing ---
            if not events:
                return []

            # 3.1. Fill Gaps (CalculatedEndTime)
            # Using specific type hints for loop variables helps IDE
            for i in range(len(events)):
                current_event = events[i]
                next_event = events[i+1] if i < len(events) - 1 else None
                
                if next_event and next_event['AssetCode'] == current_event['AssetCode']:
                    current_event['CalculatedEndTime'] = next_event['StartTime']
                else:
                    current_event['CalculatedEndTime'] = None

            # 3.2. Merge Shifts and Events
            result_data: List[Dict[str, Any]] = []
            current_time = datetime.now()

            for shift in shifts:
                # Filter events for current machine
                machine_events = [e for e in events if e['AssetCode'] == shift['AssetCode']]
                
                valid_alarms: List[Dict[str, Any]] = []
                
                for event in machine_events:
                    raw_end_time = event['CalculatedEndTime']
                    
                    # Handle open-ended events
                    if not raw_end_time:
                        if shift['ShiftEndTime'] > current_time:
                            raw_end_time = current_time
                        else:
                            raw_end_time = shift['ShiftEndTime']
                    
                    # Check overlap: (StartA < EndB) and (EndA > StartB)
                    if (event['StartTime'] < shift['ShiftEndTime']) and (raw_end_time > shift['ShiftStartTime']):
                        
                        # Clamp time to shift boundaries
                        start_val = max(event['StartTime'], shift['ShiftStartTime'])
                        
                        # Complex logic for end time clamping
                        if raw_end_time > shift['ShiftEndTime']:
                            if shift['ShiftEndTime'] > current_time:
                                end_val = min(raw_end_time, current_time)
                            else:
                                end_val = shift['ShiftEndTime']
                        else:
                            end_val = raw_end_time
                            
                        if end_val > start_val:
                            valid_alarms.append({
                                'AlarmCode': event['AlarmCode'],
                                'AlarmType': event['AlarmType'],
                                'Alarm': event['Alarm'],
                                'Comment': event['Comment'],
                                'StartTime': start_val,
                                'EndTime': end_val
                            })

                if valid_alarms:
                    result_data.append({
                        'MachineName': shift['MachineName'],
                        'DocDate': shift['ShiftDate'],
                        'Shift': shift['ShiftID'],
                        'Alarms': valid_alarms
                    })
                    
            return result_data

        except Exception as e:
            _logger.error(f"External DB Import Failed: {e}")
            return []

    def _load_merged_events(self, data_table: List[Dict[str, Any]]) -> None:
        """
        Creates or updates Odoo documents based on prepared data.
        Optimized for batch creation of alarm lines.
        """
        ReportModel = self.env['mes.shift.report']
        WorkcenterModel = self.env['mrp.workcenter']
        LossModel = self.env['mrp.workcenter.productivity.loss']
        
        shift_map = {
            '1. Mornings': 'morning', 'Morning': 'morning',
            '2. Afternoons': 'afternoon', 'Afternoon': 'afternoon',
            'Night': 'night'
        }

        for doc_row in data_table:
            # 1. Resolve Shift
            shift_selection = shift_map.get(doc_row['Shift'], 'night')

            # 2. Resolve Machine
            machine_name = doc_row['MachineName']
            machine = WorkcenterModel.search([('name', '=', machine_name)], limit=1)
            
            if not machine and ' - ' in machine_name:
                imatec_name = machine_name.split(' - ')[1].strip()
                machine = WorkcenterModel.search([('code_imatec', '=', imatec_name)], limit=1)
            
            if not machine:
                vals = {'name': machine_name}
                if ' - ' in machine_name:
                     vals['code_imatec'] = machine_name.split(' - ')[1].strip()
                machine = WorkcenterModel.create(vals)

            # 3. Resolve Report Document
            report = ReportModel.search([
                ('workcenter_id', '=', machine.id),
                ('date', '=', doc_row['DocDate']),
                ('shift_type', '=', shift_selection)
            ], limit=1)
            
            if not report:
                report = ReportModel.create({
                    'workcenter_id': machine.id,
                    'date': doc_row['DocDate'],
                    'shift_type': shift_selection
                })

            # 4. Update Alarms
            current_count = len(report.alarm_ids)
            incoming_count = len(doc_row['Alarms'])
            
            # Logic: Update only if we have more data or forced clear
            should_update = True
            if current_count > 0:
                if self.clear_existing or (current_count < incoming_count):
                    report.alarm_ids.unlink()
                else:
                    should_update = False
            
            if should_update:
                alarms_to_create = []
                for alarm_data in doc_row['Alarms']:
                    
                    # Find or Create Alarm Reason
                    alarm_reason = LossModel.search([('alarm_code', '=', alarm_data['AlarmCode'])], limit=1)
                    if not alarm_reason:
                        alarm_reason = LossModel.create({
                            'name': alarm_data['Alarm'],
                            'alarm_code': alarm_data['AlarmCode'],
                            'category': 'availability',
                            'manual': True
                        })
                    
                    alarms_to_create.append({
                        'report_id': report.id,
                        'loss_id': alarm_reason.id,
                        'start_time': alarm_data['StartTime'],
                        'end_time': alarm_data['EndTime'],
                        'comment': alarm_data['Comment']
                    })
                
                # Batch create is much faster than creating in a loop
                if alarms_to_create:
                    self.env['mes.shift.alarm'].create(alarms_to_create)