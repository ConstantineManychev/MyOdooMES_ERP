/** @odoo-module **/

import { useState } from "@odoo/owl";
import { patch } from "@web/core/utils/patch";
import { _t } from "@web/core/l10n/translation";
import { KioskAction } from "@mes_core/js/kiosk/kiosk_action";
import { meshRpc } from "@mes_core/js/kiosk/mesh_rpc";
import { activeSessions, findTemplate, matchVariant, replaceLineValue } from "./kiosk_product_logic";

patch(KioskAction.prototype, {
    setup() {
        super.setup(...arguments);
        this.productUi = useState({
            open: false,
            loading: false,
            saving: false,
            options: null,
            templateId: null,
            valueIds: [],
            moment: "",
            sessionId: null,
        });
    },

    get productTemplate() {
        return findTemplate(this.productUi.options, this.productUi.templateId);
    },

    get productVariant() {
        return matchVariant(this.productTemplate, this.productUi.valueIds);
    },

    get productSessions() {
        return activeSessions(this.state.data?.sessions);
    },

    get canApplyProduct() {
        return Boolean(this.productVariant && this.productUi.moment && this.productUi.sessionId
            && !this.productUi.saving && !this.productUi.loading);
    },

    async openProductEditor() {
        if (!this.requireSession()) return;
        Object.assign(this.productUi, {
            open: true,
            loading: true,
            saving: false,
            options: null,
            sessionId: this.productSessions[0]?.id || null,
        });
        try {
            const res = await meshRpc("/mes/kiosk/product/options", {
                workcenter_id: this.state.workcenterId,
            });
            if (!res.ok) {
                this.notification.add(res.error || _t("Cannot load products."), { type: "warning" });
                this.closeProductEditor();
                return;
            }
            this.productUi.options = res;
            this.productUi.moment = res.now;
            this.selectProductTemplate(res.current_template_id);
            const current = res.templates
                .flatMap((template) => template.variants)
                .find((variant) => variant.id === res.current_product_id);
            if (current) {
                this.productUi.valueIds = [...current.value_ids];
            }
        } catch (err) {
            this.notification.add(_t("Error: ") + err.message, { type: "danger" });
            this.closeProductEditor();
            return;
        }
        this.productUi.loading = false;
    },

    closeProductEditor() {
        Object.assign(this.productUi, { open: false, loading: false, saving: false, options: null });
    },

    selectProductTemplate(templateId) {
        const template = findTemplate(this.productUi.options, templateId);
        if (!template) return;
        this.productUi.templateId = template.id;
        this.productUi.valueIds = [...template.default_value_ids];
    },

    selectProductValue(lineId, valueId) {
        this.productUi.valueIds = replaceLineValue(this.productTemplate, this.productUi.valueIds, lineId, valueId);
    },

    selectProductSession(sessionId) {
        this.productUi.sessionId = sessionId;
    },

    setProductMomentNow() {
        this.productUi.moment = this.productUi.options.now;
    },

    setProductMomentShiftStart() {
        this.productUi.moment = this.productUi.options.shift_start;
    },

    async applyProductChange() {
        if (!this.canApplyProduct) return;
        this.productUi.saving = true;
        try {
            const res = await meshRpc("/mes/kiosk/product/set", {
                workcenter_id: this.state.workcenterId,
                session_id: this.productUi.sessionId,
                template_id: this.productUi.templateId,
                value_ids: this.productUi.valueIds,
                moment: this.productUi.moment,
            });
            if (!res.ok) {
                this.notification.add(res.error || _t("Product change failed."), { type: "danger" });
                this.productUi.saving = false;
                return;
            }
            this.notification.add(
                res.applied
                    ? _t("Product changed to %s.", res.product_name)
                    : _t("Product change to %s saved; the history updates within 5 minutes.", res.product_name),
                { type: "success" }
            );
            this.closeProductEditor();
            await this.refresh();
        } catch (err) {
            this.notification.add(_t("Error: ") + err.message, { type: "danger" });
            this.productUi.saving = false;
        }
    },
});
