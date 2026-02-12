from odoo import models, fields, api
import pandas as pd
import io
import base64
from odoo.exceptions import UserError

class MesRawDataCsvImportWizard(models.TransientModel):
    _name = 'mes.raw.data.csv.import.wizard'
    _description = 'Import Telemetry CSV'

    file_data = fields.Binary('CSV File', required=True)
    filename = fields.Char('Filename')

    def do_import(self):
        self.ensure_one()
        csv_data = base64.b64decode(self.file_data)
        df = pd.read_csv(io.BytesIO(csv_data))
        
        df['time'] = pd.to_datetime(df['timestamp'])
        machine_name = self.filename.split(' - ')[0]
        df['machine_name'] = machine_name

        df['value_raw'] = df['value']
        df['value'] = pd.to_numeric(df['value'], errors='coerce')

        mask_bool = df['value_raw'].astype(str).str.lower().isin(['true', 'false'])
        df.loc[mask_bool, 'value'] = df.loc[mask_bool, 'value_raw'].astype(str).str.lower().map({'true': 1.0, 'false': 0.0})

        df = df.dropna(subset=['value'])

        signals = self.env['mes.signal.tag'].search([('machine_settings_id.name', '=', machine_name)])
        tag_map = {rec.tag_name: rec.signal_type for rec in signals}
        
        df['type'] = df['tag_name'].map(tag_map).fillna('process')

        df_count = df[df['type'] == 'count'].copy()
        df_event = df[df['type'] == 'event'].copy()
        df_process = df[df['type'] == 'process'].copy()

        if not df_count.empty:
            df_count['value'] = df_count['value'].astype('Int64')
        if not df_event.empty:
            df_event['value'] = df_event['value'].astype('Int64')
        
        try:
            manager = self.env['mes.timescale.db.manager']
        except KeyError:
             raise UserError("Model 'mes.timescale.db.manager' not found. "
                             "Please ensure the timescale integration module is installed.")

        try:
            with manager._cursor() as cur:
                def fast_insert(dataframe, table):
                    if dataframe.empty: 
                        return
                    
                    output = io.StringIO()
                    dataframe[['time', 'machine_name', 'tag_name', 'value']].to_csv(
                        output, 
                        sep='\t', 
                        header=False, 
                        index=False
                    )
                    output.seek(0)
                    
                    cur.copy_from(
                        output, 
                        table, 
                        columns=('time', 'machine_name', 'tag_name', 'value'),
                        null=''
                    )

                fast_insert(df_count, 'telemetry_count')
                fast_insert(df_event, 'telemetry_event')
                fast_insert(df_process, 'telemetry_process')
                
            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': 'Success',
                    'message': 'Telemetry data imported successfully.',
                    'type': 'success',
                    'sticky': False,
                }
            }
        except Exception as e:
            raise UserError(f"Import Failed: {e}")
