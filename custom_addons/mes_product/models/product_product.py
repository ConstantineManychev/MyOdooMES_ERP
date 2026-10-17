from odoo import models, api


class ProductProduct(models.Model):
    _inherit = 'product.product'

    @api.model_create_multi
    def create(self, vals_list):
        products = super().create(vals_list)
        products._mes_output_variants_changed()
        return products

    def write(self, vals):
        result = super().write(vals)
        if 'active' in vals or 'product_template_attribute_value_ids' in vals:
            self._mes_output_variants_changed()
        return result

    def _mes_output_variants_changed(self):
        templates = self.mapped('product_tmpl_id')
        if not templates:
            return
        used = self.env['mes.workcenter.product.config'].sudo().search_count(
            [('product_tmpl_id', 'in', templates.ids)])
        if used:
            self.env['mrp.workcenter']._mark_all_product_dirty()
