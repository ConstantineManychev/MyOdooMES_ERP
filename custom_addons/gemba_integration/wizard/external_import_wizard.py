import logging
from datetime import date, datetime, timedelta
from typing import List, Dict, Any

import pyodbc
from odoo import models, fields
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

# --- SQL Constants ---
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
    _description = 'Import Data from External DB'

    start_date = fields.Date(string='Start Date', default=fields.Date.context_today)
    end_date = fields.Date(string='End Date', default=fields.Date.context_today)
    clear_existing = fields.Boolean(string='Clear Existing Alarms', default=False)

    def action_load_data(self) -> Dict[str, Any]:
        """Coordinator method."""
        imported_data = self._get_data_from_external_db(self.start_date, self.end_date)
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
            
            # 1. Fetch Shifts
            shifts: List[Dict[str, Any]] = []
            cursor.execute(QUERY_SHIFTS, (start_date, end_date))
            rows_shifts = cursor.fetchall()
            
            if not rows_shifts:
                conn.close()
                return []

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
                if row.ShiftStartTime < min_date: min_date = row.ShiftStartTime
                if row.ShiftEndTime > max_date: max_date = row.ShiftEndTime

            # 2. Fetch Events
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
                    'AlarmType': row.AlarmType,
                    'AssetCode': row.AssetCode,
                    'CalculatedEndTime': None 
                })

            conn.close()

            # 3. Logic Processing
            if not events:
                return []

            # 3.1. Fill Gaps
            for i in range(len(events)):
                current_event = events[i]
                next_event = events[i+1] if i < len(events) - 1 else None
                
                if next_event and next_event['AssetCode'] == current_event['AssetCode']:
                    current_event['CalculatedEndTime'] = next_event['StartTime']
                else:
                    current_event['CalculatedEndTime'] = None

            # 3.2. Merge
            result_data: List[Dict[str, Any]] = []
            
            # Use current Odoo Server time (UTC) for future clamping
            current_time = fields.Datetime.now() 

            for shift in shifts:
                machine_events = [e for e in events if e['AssetCode'] == shift['AssetCode']]
                valid_alarms: List[Dict[str, Any]] = []
                
                for event in machine_events:
                    raw_end_time = event['CalculatedEndTime']
                    
                    # --- FIXED TIME LOGIC ---
                    if not raw_end_time:
                        # If shift ended in the past, close event at shift end
                        if shift['ShiftEndTime'] < current_time:
                            raw_end_time = shift['ShiftEndTime']
                        else:
                            # If shift is still running, close at 'now'
                            raw_end_time = current_time
                    
                    if (event['StartTime'] < shift['ShiftEndTime']) and (raw_end_time > shift['ShiftStartTime']):
                        start_val = max(event['StartTime'], shift['ShiftStartTime'])
                        end_val = raw_end_time
                        
                        # Clamp end time to shift end if it spills over
                        if raw_end_time > shift['ShiftEndTime']:
                             end_val = shift['ShiftEndTime']

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
        Delegates processing to the Model.
        """
        ReportModel = self.env['mes.shift.report']
        
        for doc_row in data_table:
            ReportModel.process_external_batch(doc_row, self.clear_existing)