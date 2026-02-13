import io
import base64
import logging
import pandas as pd
from odoo import models, fields, _
from odoo.exceptions import UserError

_logger = logging.getLogger(__name__)

class MesRawDataCsvImportWizard(models.TransientModel):
    _name = 'mes.raw.data.csv.import.wizard'
    _description = 'Import Telemetry CSV'

    file_data = fields.Binary('CSV File', required=True)
    filename = fields.Char('Filename')

    def do_import(self):
        self.ensure_one()
        
        try:
            csv_data = base64.b64decode(self.file_data)
            df = pd.read_csv(io.BytesIO(csv_data))
        except Exception as e:
            raise UserError(_("Failed to read CSV file. Error: %s") % str(e))

        machine_name = self.filename.split(' - ')[0] if self.filename else 'Unknown'
        df['machine_name'] = machine_name
        
        if 'timestamp' in df.columns:
            df['time'] = pd.to_datetime(df['timestamp'])
        else:
             raise UserError(_("CSV must contain a 'timestamp' column."))

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

        manager = self.env['mes.timescale.db.manager']
        columns = ('time', 'machine_name', 'tag_name', 'value')

        def _prepare_and_push(dataframe, table_name):
            if dataframe.empty:
                return 0
            
            buffer = io.StringIO()
            
            dataframe[['time', 'machine_name', 'tag_name', 'value']].to_csv(
                buffer, 
                sep='\t', 
                header=False, 
                index=False
            )
            buffer.seek(0)
            
            manager.bulk_copy_from_buffer(table_name, buffer, columns)
            return len(dataframe)

        try:
            total_rows = 0
            total_rows += _prepare_and_push(df_count, 'telemetry_count')
            total_rows += _prepare_and_push(df_event, 'telemetry_event')
            total_rows += _prepare_and_push(df_process, 'telemetry_process')

            return {
                'type': 'ir.actions.client',
                'tag': 'display_notification',
                'params': {
                    'title': _('Import Success'),
                    'message': _('Successfully imported %s telemetry records.') % total_rows,
                    'type': 'success',
                    'sticky': False,
                    'next': {'type': 'ir.actions.act_window_close'},
                }
            }

        except Exception as e:
            _logger.exception("Telemetry CSV Import Failed")
            raise UserError(_("Database Import Failed: %s") % str(e))