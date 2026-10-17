/** @odoo-module **/

import { ListController } from "@web/views/list/list_controller";
import { patch } from "@web/core/utils/patch";
import { useService } from "@web/core/utils/hooks";
import { onWillStart, onWillDestroy } from "@odoo/owl";

patch(ListController.prototype, {
    setup() {
        super.setup(); 
        
        const isWorkcenter = this.props.resModel === 'mrp.workcenter';
        const isLoginEntry = this.props.resModel === 'mes.performance.login.entry';
        if (isWorkcenter || isLoginEntry) {
            this.orm = useService("orm");
            this.refreshTimer = null;

            onWillStart(async () => {
                let intervalMs = isLoginEntry ? 10000 : 60000;

                if (isWorkcenter) {
                    try {
                        const intervalStr = await this.orm.call(
                            "ir.config_parameter",
                            "get_param",
                            ["mes_core.dashboard_refresh_interval", "60"]
                        );
                        intervalMs = parseInt(intervalStr || "60") * 1000;
                    } catch (error) {
                        console.warn("MES Dashboard: using default interval", error);
                    }
                }

                if (intervalMs > 0) {
                    this.refreshTimer = setInterval(() => {
                        if (this.model && typeof this.model.load === 'function') {
                            this.model.load();
                        }
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