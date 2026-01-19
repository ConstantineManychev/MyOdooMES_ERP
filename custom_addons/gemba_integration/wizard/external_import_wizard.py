from odoo import models, fields, api
from datetime import timedelta
import logging

_logger = logging.getLogger(__name__)

class ShiftDataStruct:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class ExternalImportWizard(models.TransientModel):
    _name = 'mes.external.import.wizard'
    _description = 'Import Data from External DB'

    start_date = fields.Date(string='Start Date', default=fields.Date.context_today)
    end_date = fields.Date(string='End Date', default=fields.Date.context_today)
    clear_existing = fields.Boolean(string='Clear Existing Alarms', default=False)

    def action_load_data(self):
        # 1. Get data from external DB
        imported_data = self._get_data_from_external_db(self.start_date, self.end_date)
        
        # 2. Load data into Odoo
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

    def _get_data_from_external_db(self, start_date, end_date):
        import pyodbc
        from datetime import timedelta
        
        params = self.env['ir.config_parameter'].sudo()
        
        server = params.get_param('gemba.sql_server')
        database = params.get_param('gemba.sql_database')
        username = params.get_param('gemba.sql_user')
        password = params.get_param('gemba.sql_password')
        
        # Is settings filled up
        if not all([server, database, username, password]):
             raise UserError("SQL Connection settings are missing! Please configure them in Settings.")
        
        connection_string = (
            'DRIVER={ODBC Driver 17 for SQL Server};'
            f'SERVER={server};'
            f'DATABASE={database};'
            f'UID={username};'
            f'PWD={password};'
            'TrustServerCertificate=yes;'
        )

        try:
            conn = pyodbc.connect(connection_string, timeout=10)
            cursor = conn.cursor()
            
            # --- 1. Shifts ---
            query_shifts = """
                SELECT 
                    Shift.AssetShiftID, 
                    Shift.ShiftDate,
                    Shift.AssetID AS AssetCode,
                    Shift.AssetID AS MachineName, -- В 1С это было поле Code, берем ID пока
                    Shift.StartTime AS ShiftStartTime,
                    Shift.EndTime AS ShiftEndTime,
                    Shift.ShiftID -- Название смены
                FROM 
                    dbo.tblDATAssetShift AS Shift
                WHERE 
                    Shift.ShiftDate BETWEEN ? AND ?
            """
            cursor.execute(query_shifts, (start_date, end_date))
            rows_shifts = cursor.fetchall()
            
            vt_shifts = []
            if not rows_shifts:
                conn.close()
                return []

            min_date = rows_shifts[0].ShiftStartTime
            max_date = rows_shifts[0].ShiftEndTime

            for row in rows_shifts:
                # Shift struct
                vt_shifts.append({
                    'AssetShiftID': row.AssetShiftID,
                    'ShiftDate': row.ShiftDate,
                    'AssetCode': row.AssetCode,
                    'MachineName': row.MachineName, 
                    'ShiftStartTime': row.ShiftStartTime,
                    'ShiftEndTime': row.ShiftEndTime,
                    'ShiftID': str(row.ShiftID) # Приводим к строке
                })
                
                # Calculate min/max for events search
                if row.ShiftStartTime < min_date: min_date = row.ShiftStartTime
                if row.ShiftEndTime > max_date: max_date = row.ShiftEndTime

            # --- 2. Events ---
            # Extend search range by 1 day backward
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
            
            vt_events = []
            for row in rows_events:
                vt_events.append({
                    'StartTime': row.StartTime,
                    'Comment': row.Comment,
                    'AlarmCode': row.AlarmCode,
                    'Alarm': row.Alarm,
                    'AlarmType': row.AlarmType,
                    'AssetCode': row.AssetCode,
                    'CalculatedEndTime': None # Пока пусто
                })

            conn.close()

            # --- 3. Data Processing ---
            if not vt_events:
                return []

            # 3.1. CalculatedEndTime filling
            for i in range(len(vt_events)):
                row = vt_events[i]
                next_row = vt_events[i+1] if i < len(vt_events) - 1 else None
                
                if next_row and next_row['AssetCode'] == row['AssetCode']:
                    row['CalculatedEndTime'] = next_row['StartTime']
                else:
                    row['CalculatedEndTime'] = None

            # 3.2. Shift-Event Matching
            data_table = []
            current_time = fields.Datetime.now()

            for shift_row in vt_shifts:
                # Current machine events
                machine_events = [e for e in vt_events if e['AssetCode'] == shift_row['AssetCode']]
                
                alarms_struct = []
                
                for event_row in machine_events:
                    raw_end_time = event_row['CalculatedEndTime']
                    
                    # Undefined end time handling
                    if not raw_end_time:
                        if shift_row['ShiftEndTime'] > current_time:
                            raw_end_time = current_time
                        else:
                            raw_end_time = shift_row['ShiftEndTime']
                    
                    # Period overlap check
                    if event_row['StartTime'] < shift_row['ShiftEndTime'] and raw_end_time > shift_row['ShiftStartTime']:
                        
                        # Calculate final StartTime and EndTime
                        start_val = max(event_row['StartTime'], shift_row['ShiftStartTime'])
                        
                        if raw_end_time > shift_row['ShiftEndTime']:
                            if shift_row['ShiftEndTime'] > current_time:
                                end_val = min(raw_end_time, current_time)
                            else:
                                end_val = shift_row['ShiftEndTime']
                        else:
                            end_val = raw_end_time
                            
                        # Final check
                        if end_val > start_val:
                            alarms_struct.append({
                                'AlarmCode': event_row['AlarmCode'],
                                'AlarmType': event_row['AlarmType'],
                                'Alarm': event_row['Alarm'],
                                'Comment': event_row['Comment'],
                                'StartTime': start_val,
                                'EndTime': end_val
                            })

                # If there are alarms for this shift, add to data_table
                if alarms_struct:
                    data_table.append({
                        'MachineName': shift_row['MachineName'],
                        'DocDate': shift_row['ShiftDate'],
                        'Shift': shift_row['ShiftID'],
                        'Alarms': alarms_struct
                    })
                    
            return data_table

        except Exception as e:
            _logger.error(f"External DB Import Failed: {e}")
            return []

    def _load_merged_events(self, data_table):
        
        ReportModel = self.env['mes.shift.report']
        WorkcenterModel = self.env['mrp.workcenter']
        LossModel = self.env['mrp.workcenter.productivity.loss']
        
        for doc_row in data_table:
            # 1. Shift mapping
            shift_map = {
                '1. Mornings': 'morning', 'Morning': 'morning',
                '2. Afternoons': 'afternoon', 'Afternoon': 'afternoon',
                'Night': 'night'
            }
            shift_selection = shift_map.get(doc_row['Shift'], 'night')

            # 2. Search or Create Machine
            machine = WorkcenterModel.search([('name', '=', doc_row['MachineName'])], limit=1)
            
            if not machine:
                if ' - ' in doc_row['MachineName']:
                    parts = doc_row['MachineName'].split(' - ')
                    if len(parts) > 1:
                        imatec_name = parts[1].strip()
                        machine = WorkcenterModel.search([('code_imatec', '=', imatec_name)], limit=1)
            if not machine:
                # create new machine
                vals = {'name': doc_row['MachineName']}
                if ' - ' in doc_row['MachineName']:
                     vals['code_imatec'] = doc_row['MachineName'].split(' - ')[1].strip()
                machine = WorkcenterModel.create(vals)

            # 3. Search or Create Report
            domain = [
                ('workcenter_id', '=', machine.id),
                ('date', '=', doc_row['DocDate']),
                ('shift_type', '=', shift_selection)
            ]
            report = ReportModel.search(domain, limit=1)
            
            if not report:
                report = ReportModel.create({
                    'workcenter_id': machine.id,
                    'date': doc_row['DocDate'],
                    'shift_type': shift_selection
                })

            # 4. Table part: Alarms
            # Clear or update existing alarms
            current_alarm_count = len(report.alarm_ids)
            incoming_alarm_count = len(doc_row['Alarms'])
            
            need_write = True
            if current_alarm_count > 0:
                if self.clear_existing or (current_alarm_count < incoming_alarm_count):
                    report.alarm_ids.unlink() # Очистка
                else:
                    need_write = False
            
            if need_write:
                new_alarms = []
                for alarm_data in doc_row['Alarms']:
                    # Alarm search or create
                    alarm_reason = LossModel.search([('alarm_code', '=', alarm_data['AlarmCode'])], limit=1)
                    
                    if not alarm_reason:
                        # create new alarm reason
                        alarm_reason = LossModel.create({
                            'name': alarm_data['Alarm'], # Description
                            'alarm_code': alarm_data['AlarmCode'],
                            'category': 'availability', # Default category
                            'manual': True
                        })
                    
                    new_alarms.append({
                        'report_id': report.id,
                        'loss_id': alarm_reason.id,
                        'start_time': alarm_data['StartTime'],
                        'end_time': alarm_data['EndTime'],
                        'comment': alarm_data['Comment']
                    })
                
                # Create all alarms at once
                self.env['mes.shift.alarm'].create(new_alarms)