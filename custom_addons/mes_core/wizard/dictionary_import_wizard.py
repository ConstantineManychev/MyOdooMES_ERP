import base64
import csv
import io
from odoo import models, fields, _
from odoo.exceptions import UserError

class MesDictionaryImportWizard(models.TransientModel):
    _name = 'mes.dictionary.import.wizard'
    _description = 'Import Dictionaries from CSV'

    import_type = fields.Selection([
        ('event', 'Events / Alarms (mes.event)'),
        ('count', 'Counts / Rejections (mes.counts)')
    ], string='Import Type', required=True, default='event')
    
    file = fields.Binary(string='CSV File', required=True)
    filename = fields.Char(string='Filename')

    def do_import(self):
        if not self.file:
            raise UserError(_("Please upload a file."))
        try:
            csv_data = base64.b64decode(self.file)
            data_file = io.StringIO(csv_data.decode("utf-8-sig"))
            file_reader = csv.DictReader(data_file, delimiter=';')
        except Exception as e:
            raise UserError(f"Invalid file format: {e}")

        is_event = self.import_type == 'event'
        model_name = 'mes.event' if is_event else 'mes.counts'
        
        # Динамический маппинг колонок под модель
        mapping = {
            'DefaultOPCTag': 'default_OPCTag',
            'DefaultPLCValue': 'default_PLCValue' if is_event else None,
            'IsModuleCount': None if is_event else 'is_module_count',
            'Wheel': None if is_event else 'wheel',
            'Module': None if is_event else 'module'
        }

        data_list = []
        for row in file_reader:
            vals = {}
            for csv_col, db_field in mapping.items():
                if db_field and csv_col in row:
                    val = row[csv_col].strip()
                    if db_field in ['default_PLCValue', 'wheel', 'module']:
                        vals[db_field] = int(val) if val.isdigit() else 0
                    elif db_field == 'is_module_count':
                        vals[db_field] = val.lower() in ['1', 'true', 'yes']
                    else:
                        vals[db_field] = val

            data_list.append({
                'code': row.get('Code', '').strip(),
                'name': row.get('Name', '').strip(),
                'parent_name': row.get('ParentName', '').strip(),
                'vals': vals
            })

        self.env[model_name].sync_batch(data_list)

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': 'Import Success',
                'message': f'Successfully imported {len(data_list)} records into {model_name}.',
                'type': 'success',
                'sticky': False,
                'next': {'type': 'ir.actions.act_window_close'},
            }
        }