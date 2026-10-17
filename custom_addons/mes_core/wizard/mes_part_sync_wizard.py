import logging
from odoo import models, fields, api, _
from odoo.exceptions import UserError
from odoo.addons.mes_core.tools.maintainx_api import MaintainXClient

_logger = logging.getLogger(__name__)


class MesPartSyncWizard(models.TransientModel):
    _name = 'mes.part.sync.wizard'
    _description = 'Sync Work Center Parts With MaintainX'

    workcenter_id = fields.Many2one('mrp.workcenter', string='Work Center', required=True)
    line_ids = fields.One2many('mes.part.sync.line', 'wizard_id', string='Differences')
    fetched_count = fields.Integer(string='Assets Received', readonly=True)
    in_sync = fields.Boolean(string='Everything In Sync', readonly=True)

    @api.model
    def open_for_workcenter(self, workcenter):
        company = workcenter.company_id or self.env.company
        if not company.maintainx_enabled or not company.maintainx_api_token:
            raise UserError(_("Enable MaintainX and set API Token first."))
        asset_id = workcenter.sudo().maintainx_id
        if not asset_id:
            raise UserError(_("This work center has no MaintainX ID."))

        client = MaintainXClient(company.maintainx_api_token)
        all_assets = client.get_assets()
        if not all_assets:
            raise UserError(_(
                "MaintainX returned no assets at all. Check the API token and that the "
                "account has access to assets."))

        target = str(asset_id)
        assets = [a for a in all_assets if client.asset_parent_id(a) == target]
        if not assets:
            raise UserError(_(
                "MaintainX returned %(total)s assets, but none of them has this work "
                "center (asset %(asset)s) as parent. Check the asset hierarchy in "
                "MaintainX.", total=len(all_assets), asset=target))

        wizard = self.create({'workcenter_id': workcenter.id, 'fetched_count': len(assets)})
        wizard._build_lines(assets)
        wizard.in_sync = not wizard.line_ids
        return wizard

    def _build_lines(self, assets):
        self.ensure_one()
        Part = self.env['mes.part'].sudo()
        existing = {line.part_id.id: line for line in self.workcenter_id.part_line_ids}
        by_name = {part.name.strip().lower(): part for part in Part.search([])}

        seen_part_ids = set()
        values = []

        for asset in assets:
            asset_id = str(asset.get('id') or '').strip()
            asset_name = (asset.get('name') or '').strip()
            if not asset_id or not asset_name:
                continue

            part = by_name.get(asset_name.lower())
            if not part:
                values.append({
                    'kind': 'unmatched',
                    'asset_name': asset_name,
                    'maintainx_id': asset_id,
                })
                continue

            seen_part_ids.add(part.id)
            current = existing.get(part.id)
            if not current:
                values.append({
                    'kind': 'missing',
                    'asset_name': asset_name,
                    'part_id': part.id,
                    'maintainx_id': asset_id,
                })
            elif (current.maintainx_id or '').strip() != asset_id:
                values.append({
                    'kind': 'changed',
                    'asset_name': asset_name,
                    'part_id': part.id,
                    'maintainx_id': asset_id,
                    'current_maintainx_id': current.maintainx_id or '',
                })

        for part_id, line in existing.items():
            if part_id not in seen_part_ids:
                values.append({
                    'kind': 'extra',
                    'asset_name': line.part_id.name,
                    'part_id': part_id,
                    'current_maintainx_id': line.maintainx_id or '',
                })

        order = {'missing': 0, 'changed': 1, 'unmatched': 2, 'extra': 3}
        values.sort(key=lambda v: (order[v['kind']], v['asset_name']))
        self.line_ids = [(0, 0, v) for v in values]

    def action_apply(self):
        self.ensure_one()
        Link = self.env['mes.workcenter.part'].sudo()
        applied = {'added': 0, 'updated': 0, 'removed': 0, 'created': 0}

        for line in self.line_ids:
            if line.kind == 'extra':
                link = Link.search([
                    ('workcenter_id', '=', self.workcenter_id.id),
                    ('part_id', '=', line.part_id.id),
                ], limit=1)
                if link:
                    link.unlink()
                    applied['removed'] += 1
                continue

            part = line.part_id
            if line.kind == 'unmatched':
                if not line.create_part and not part:
                    raise UserError(_(
                        "Asset '%(asset)s' is not matched to a part. Pick an existing part, "
                        "tick 'Create Part', or remove the row.", asset=line.asset_name))
                if not part:
                    part = self.env['mes.part'].sudo().create({'name': line.asset_name})
                    applied['created'] += 1

            if not part:
                continue

            link = Link.search([
                ('workcenter_id', '=', self.workcenter_id.id),
                ('part_id', '=', part.id),
            ], limit=1)
            if link:
                if (link.maintainx_id or '') != line.maintainx_id:
                    link.maintainx_id = line.maintainx_id
                    applied['updated'] += 1
            else:
                Link.create({
                    'workcenter_id': self.workcenter_id.id,
                    'part_id': part.id,
                    'maintainx_id': line.maintainx_id,
                })
                applied['added'] += 1

        return {
            'type': 'ir.actions.client',
            'tag': 'display_notification',
            'params': {
                'type': 'success',
                'title': _("Parts synchronised"),
                'message': _("Added: %(added)s, updated: %(updated)s, removed: %(removed)s, "
                             "new parts created: %(created)s", **applied),
                'next': {'type': 'ir.actions.act_window_close'},
            },
        }


class MesPartSyncLine(models.TransientModel):
    _name = 'mes.part.sync.line'
    _description = 'Work Center Part Sync Difference'
    _order = 'kind, asset_name'

    wizard_id = fields.Many2one('mes.part.sync.wizard', required=True, ondelete='cascade')
    kind = fields.Selection([
        ('missing', 'Missing on the work center'),
        ('changed', 'MaintainX ID differs'),
        ('unmatched', 'No matching part'),
        ('extra', 'Not in MaintainX'),
    ], string='Difference', required=True)
    asset_name = fields.Char(string='MaintainX Asset', required=True)
    part_id = fields.Many2one('mes.part', string='Part')
    create_part = fields.Boolean(string='Create Part')
    maintainx_id = fields.Char(string='New MaintainX ID')
    current_maintainx_id = fields.Char(string='Current MaintainX ID', readonly=True)

    @api.onchange('part_id')
    def _onchange_part_id(self):
        for line in self:
            if line.part_id:
                line.create_part = False
