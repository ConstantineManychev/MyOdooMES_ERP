import base64
import csv
import io
from odoo import models, fields, _
from odoo.exceptions import UserError

class MesImportEventsWizard(models.TransientModel):
    _name = 'mes.events.import.wizard'
    _description = 'Import Events from CSV'

    file = fields.Binary(string='CSV File', required=True)
    filename = fields.Char(string='Filename')

    def do_import(self):
        if not self.file:
            raise UserError(_("Please upload a file."))
        try:
            csv_data = base64.b64decode(self.file)
            data_file = io.StringIO(csv_data.decode("utf-8-sig"))
            data_file.seek(0)
            file_reader = csv.DictReader(data_file, delimiter=';')
        except Exception as e:
            raise UserError(_("Invalid file format. Ensure it is a CSV with ';' delimiter. Error: %s") % e)

        expected_headers = ['AlarmCode', 'Description', 'DefaultOPCTag', 'DefaultPLCValue', 'ParentName']
        if not set(expected_headers).issubset(set(file_reader.fieldnames)):
             raise UserError(_("Missing columns! Expected: %s") % ";".join(expected_headers))

        MesEvent = self.env['mes.event']
        
        count = 0
        
        for row in file_reader:
            code = row.get('AlarmCode', '').strip()
            name = row.get('Description', '').strip()
            opc_tag = row.get('DefaultOPCTag', '').strip()
            plc_val = row.get('DefaultPLCValue', '').strip()
            parent_name = row.get('ParentName', '').strip()

            if not name:
                continue

            parent_id = False
            if parent_name:
                parent = MesEvent.search([('name', '=', parent_name)], limit=1)
                if parent:
                    parent_id = parent.id
                else:
                    parent = MesEvent.create({'name': parent_name})
                    parent_id = parent.id

            domain = []
            if code:
                domain = [('code', '=', code)]
            else:
                domain = [('name', '=', name)]
                
            event = MesEvent.search(domain, limit=1)

            vals = {
                'name': name,
                'code': code,
                'default_OPCTag': opc_tag,
                'default_PLCValue': plc_val,
                'parent_id': parent_id,
            }

            if event:
                event.write(vals)
            else:
                MesEvent.create(vals)
            
            count += 1

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'title': _('Import Success'),
                'message': _('%s events imported successfully.') % count,
                'type': 'success',
                'sticky': False,
                'next': {'type': 'ir.actions.act_window_close'},
            }
        }