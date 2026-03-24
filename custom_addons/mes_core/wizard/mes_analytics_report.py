from odoo import models, fields, api
import pytz

class MesAnalyticsWizard(models.TransientModel):
    _name = 'mes.analytics.wizard'
    _inherit = 'mes.report.base.wizard'
    _description = 'Shift Analytics Matrix Wizard'

    show_oee = fields.Boolean("OEE (%)", default=True)
    show_availability = fields.Boolean("Availability (%)", default=True)
    show_performance = fields.Boolean("Performance (%)", default=True)
    show_quality = fields.Boolean("Quality (%)", default=True)
    show_produced = fields.Boolean("Produced Qty", default=True)
    show_waste = fields.Boolean("Waste Loss (%)", default=True)
    show_downtime = fields.Boolean("Downtime Loss (%)", default=True)

    limit_by = fields.Selection(
        selection='_get_limit_by_options',
        default='oee',
        required=True
    )

    @api.model
    def _get_limit_by_options(self):
        return [
            ('oee', 'OEE (%)'),
            ('produced', 'Produced Qty'),
            ('availability', 'Availability (%)'),
            ('performance', 'Performance (%)'),
            ('quality', 'Quality (%)')
        ]

    def action_generate_report(self):
        self.env['mes.analytics.report.line'].search([('user_id', '=', self.env.user.id)]).unlink()

        machines = self._get_filtered_machines()
        if not machines:
            return

        lines_to_create = []

        for machine in machines:
            workcenter = self.env['mrp.workcenter'].search([('machine_settings_id', '=', machine.id)], limit=1)
            if not workcenter:
                continue
            
            # Получаем индивидуальные смены и периоды для машины в ее часовом поясе
            tz_name = workcenter.company_id.tz or 'UTC'
            shifts = self.env['mes.shift'].search([('company_id', '=', workcenter.company_id.id)], order='start_hour asc')
            periods_dict = self._get_logical_periods(self.start_datetime, self.end_datetime, shifts, tz_name)

            state_sig = machine.event_tag_ids.filtered(lambda x: x.event_id == workcenter.runtime_event_id) if workcenter else None

            for p_name, time_blocks in periods_dict.items():
                if not time_blocks:
                    continue
                
                p_start = min(t[0] for t in time_blocks)
                p_end = max(t[1] for t in time_blocks)

                kpi = machine._calculate_kpi_for_window(workcenter, p_start, p_end)
                
                if kpi and (kpi.get('oee') or kpi.get('produced')):
                    
                    def build_label(by_mac, by_per):
                        parts = []
                        if by_mac: parts.append(machine.name)
                        if by_per: parts.append(p_name)
                        return " | ".join(parts) if parts else "All Data"

                    r_label = build_label(self.row_by_machine, self.row_by_period)
                    c_label = build_label(self.col_by_machine, self.col_by_period)

                    lines_to_create.append({
                        'user_id': self.env.user.id,
                        'machine_id': machine.id,
                        'period_name': p_name,
                        'row_group_label': r_label,
                        'col_group_label': c_label,
                        'first_running_time': kpi.get('first_running_time', False),
                        'oee': kpi.get('oee', 0),
                        'availability': kpi.get('availability', 0),
                        'performance': kpi.get('performance', 0),
                        'quality': kpi.get('quality', 0),
                        'produced': kpi.get('produced', 0),
                        'waste_losses': kpi.get('waste_losses', 0),
                        'downtime_losses': kpi.get('downtime_losses', 0),
                    })

        if lines_to_create:
            lines_to_create.sort(key=lambda x: x.get(self.limit_by, 0), reverse=True)
            if self.record_limit > 0:
                lines_to_create = lines_to_create[:self.record_limit]
            self.env['mes.analytics.report.line'].create(lines_to_create)

        measures = []
        if self.show_oee: measures.append('oee')
        if self.show_availability: measures.append('availability')
        if self.show_performance: measures.append('performance')
        if self.show_quality: measures.append('quality')
        if self.show_produced: measures.append('produced')
        if self.show_waste: measures.append('waste_losses')
        if self.show_downtime: measures.append('downtime_losses')
        
        if not measures:
            measures = ['oee']

        ctx = self._build_skd_context(measures)

        return {
            'name': 'Shift Analytics Matrix',
            'type': 'ir.actions.act_window',
            'res_model': 'mes.analytics.report.line',
            'view_mode': 'pivot,tree',
            'domain': [('user_id', '=', self.env.user.id)],
            'context': ctx
        }

class MesAnalyticsReportLine(models.Model):
    _name = 'mes.analytics.report.line'
    _description = 'Analytics Report Matrix Line'

    user_id = fields.Many2one('res.users', string="User")
    machine_id = fields.Many2one('mes.machine.settings', string="Machine")
    period_name = fields.Char(string="Period")

    row_group_label = fields.Char(string="Rows Level")
    col_group_label = fields.Char(string="Columns Level")

    first_running_time = fields.Datetime(string="First Start")
    oee = fields.Float("OEE (%)", group_operator="avg")
    availability = fields.Float("Availability (%)", group_operator="avg")
    performance = fields.Float("Performance (%)", group_operator="avg")
    quality = fields.Float("Quality (%)", group_operator="avg")
    produced = fields.Float("Produced", group_operator="sum")
    waste_losses = fields.Float("Waste Loss (%)", group_operator="avg")
    downtime_losses = fields.Float("Downtime Loss (%)", group_operator="avg")