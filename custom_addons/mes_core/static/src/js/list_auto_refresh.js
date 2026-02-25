/** @odoo-module **/

import { ListController } from "@web/views/list/list_controller";
import { patch } from "@web/core/utils/patch";
import { useService } from "@web/core/utils/hooks";
import { onWillStart, onWillDestroy } from "@odoo/owl";

patch(ListController.prototype, "mes_core.TreeAutoRefresh", {
    setup() {
        this._super(...arguments);
        
        if (this.props.resModel === 'mrp.workcenter') {
            this.orm = useService("orm");
            this.refreshTimer = null;

            onWillStart(async () => {
                const intervalStr = await this.orm.call(
                    "ir.config_parameter",
                    "get_param",
                    ["mes_core.dashboard_refresh_interval", "60"]
                );
                const intervalMs = parseInt(intervalStr) * 1000;

                if (intervalMs > 0) {
                    this.refreshTimer = setInterval(() => {
                        this.model.load();
                    }, intervalMs);
                }
            });

            onWillDestroy(() => {
                if (this.refreshTimer) {
                    clearInterval(this.refreshTimer);
                }
            });
        }
    }
});