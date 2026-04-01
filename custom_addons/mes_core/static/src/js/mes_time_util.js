/** @odoo-module **/

export class MesTimeUtil {
    static toMillis(val) {
        if (!val) return NaN;
        if (typeof val === 'number') return val;
        const isoStr = String(val).replace(' ', 'T') + "Z";
        return new Date(isoStr).getTime();
    }

    static fmtDisplay(ms) {
        if (isNaN(ms)) return '';
        const dt = new Date(ms);
        const iso = dt.toISOString();
        const yyyy = iso.substring(0, 4);
        const mm = iso.substring(5, 7);
        const dd = iso.substring(8, 10);
        const hhmm = iso.substring(11, 16);
        return `${dd}.${mm}.${yyyy} ${hhmm}`;
    }
}