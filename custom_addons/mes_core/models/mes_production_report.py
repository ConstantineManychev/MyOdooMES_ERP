from odoo import models, fields, api

class MesProductionReport(models.Model):
    _name = 'mes.production.report'
    _description = 'Shift Production Report (Packing)'
    _inherit = ['mail.thread', 'mail.activity.mixin']

    name = fields.Char(string='Report #', default='New', readonly=True)
    date = fields.Date(string='Date', required=True, default=fields.Date.context_today)
    
    machine_id = fields.Many2one('mrp.workcenter', string='Machine', required=True)
    shift_id = fields.Many2one('mes.shift', string='Shift', required=True)
    product_id = fields.Many2one('product.product', string='Main Product')
    
    start_time = fields.Datetime(string='Job Start')
    end_time = fields.Datetime(string='Job End')
    job_number = fields.Char(string='Job Number')

    # Tables
    packer_ids = fields.One2many('mes.production.packer', 'report_id', string='Packers')
    shipper_ids = fields.One2many('mes.production.shipper', 'report_id', string='Shippers')
    qc_ids = fields.One2many('mes.production.qc', 'report_id', string='QC Checks')
    ingredient_ids = fields.One2many('mes.production.ingredient', 'report_id', string='Ingredients')

    @api.model
    def create(self, vals):
        if vals.get('name', 'New') == 'New':
            vals['name'] = self.env['ir.sequence'].next_by_code('mes.production.report') or 'New'
        return super().create(vals)

# --- Sub-Tables ---

class MesPacker(models.Model):
    _name = 'mes.production.packer'
    _description = 'Packer Activity'

    report_id = fields.Many2one('mes.production.report')
    employee_id = fields.Many2one('hr.employee', string='Packer')
    start_time = fields.Datetime(string='Start')
    end_time = fields.Datetime(string='End')

class MesShipper(models.Model):
    _name = 'mes.production.shipper'
    _description = 'Shipper (Pallet/Box)'

    report_id = fields.Many2one('mes.production.report')
    name = fields.Char(string='Shipper ID/Barcode')
    product_id = fields.Many2one('product.product', string='Product')
    qty = fields.Float(string='Quantity')
    
    # Table Outers
    outer_ids = fields.One2many('mes.production.outer', 'shipper_id', string='Outers')

class MesOuter(models.Model):
    _name = 'mes.production.outer'
    _description = 'Outer Package'

    shipper_id = fields.Many2one('mes.production.shipper')
    name = fields.Char(string='Outer ID')
    qty = fields.Float(string='Qty per Outer')

class MesQC(models.Model):
    _name = 'mes.production.qc'
    _description = 'Quality Control Check'

    report_id = fields.Many2one('mes.production.report')
    check_time = fields.Datetime(string='Check Time', default=fields.Datetime.now)
    checked_by = fields.Many2one('res.users', string='Inspector', default=lambda self: self.env.user)
    result = fields.Selection([('pass', 'Pass'), ('fail', 'Fail')], string='Result')
    
    # Table Defects
    defect_ids = fields.One2many('mes.production.qc.defect', 'qc_id', string='Defects Found')

class MesQCDefect(models.Model):
    _name = 'mes.production.qc.defect'
    _description = 'Quality Control Defect'
    
    qc_id = fields.Many2one('mes.production.qc')
    defect_id = fields.Many2one('mes.defect', string='Defect Type')
    qty = fields.Float(string='Affected Qty')
    comment = fields.Char(string='Note')

class MesIngredient(models.Model):
    _name = 'mes.production.ingredient'
    _description = 'Production Ingredient/Material Used'
    
    report_id = fields.Many2one('mes.production.report')
    product_id = fields.Many2one('product.product', string='Material', domain=[('detailed_type', '=', 'product')])
    lot_number = fields.Char(string='Batch/Lot Number')
    qty_used = fields.Float(string='Quantity Used')