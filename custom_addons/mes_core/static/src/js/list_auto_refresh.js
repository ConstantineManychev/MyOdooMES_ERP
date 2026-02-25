/** @odoo-module **/

import { ListController } from "@web/views/list/list_controller";
import { patch } from "@web/core/utils/patch";
import { useService } from "@web/core/utils/hooks";
import { onWillStart, onWillDestroy } from "@odoo/owl";

patch(ListController.prototype, {
    setup() {
        super.setup(); 
        
        if (this.props.resModel === 'mrp.workcenter') {
            this.orm = useService("orm");
            this.refreshTimer = null;

            onWillStart(async () => {
                let intervalMs = 60000; 
                
                try {
                    const intervalStr = await this.orm.call(
                        "ir.config_parameter",
                        "get_param",
                        ["mes_core.dashboard_refresh_interval", "60"]
                    );
                    intervalMs = parseInt(intervalStr || "60") * 1000;
                } catch (error) {
                    console.warn("MES Dashboard: No permission to read settings, using default 60 sec", error);
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