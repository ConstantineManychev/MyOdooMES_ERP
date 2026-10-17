from odoo import models


class MesAnomalyReport(models.Model):
    _inherit = 'mes.anomaly.report'

    def _anomaly_slices(self, docs, windows, now):
        result = super()._anomaly_slices(docs, windows, now)
        if not docs:
            return result
        earliest = min(windows[doc.id][0] for doc in docs)
        runs = self.env['mes.production.run'].sudo().search([
            ('workcenter_id', 'in', docs.mapped('machine_id').ids),
            ('date_start', '<', now),
            '|', ('date_end', '=', False), ('date_end', '>', earliest),
        ], order='date_start')
        by_machine = {}
        for run in runs:
            by_machine.setdefault(run.workcenter_id.id, []).append(
                (run.date_start, run.date_end or now, run.product_id.id or False))
        for doc in docs:
            start, end = windows[doc.id][0], min(windows[doc.id][1], now)
            pieces, cursor = [], start
            for run_start, run_end, product_id in by_machine.get(doc.machine_id.id, []):
                piece_start, piece_end = max(run_start, start), min(run_end, end)
                if piece_end <= piece_start:
                    continue
                if piece_start > cursor:
                    pieces.append((False, cursor, piece_start))
                pieces.append((product_id, piece_start, piece_end))
                cursor = max(cursor, piece_end)
            if cursor < end:
                pieces.append((False, cursor, end))
            result[doc.id] = pieces
        return result
