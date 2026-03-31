/** @odoo-module **/

import { deserializeDateTime } from "@web/core/l10n/dates";

export class MesTimeUtil {
    static toMillis(val) {
        if (!val) return NaN;
        if (typeof val === 'number') return val;
        const dt = typeof val === 'string' ? deserializeDateTime(val) : val;
        return dt ? dt.toMillis() : NaN;
    }

    static fmtDisplay(ms) {
        if (isNaN(ms)) return '';
        const dt = luxon.DateTime.fromMillis(ms);
        return dt.toFormat("dd.MM.yyyy HH:mm");
    }
}