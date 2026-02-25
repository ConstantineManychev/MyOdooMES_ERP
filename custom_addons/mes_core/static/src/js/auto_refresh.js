/** @odoo-module **/

import { IntegerField } from "@web/views/fields/integer/integer_field";
import { registry } from "@web/core/registry";
import { onMounted, onWillUnmount, onWillUpdateProps } from "@odoo/owl";

export class AutoRefreshWidget extends IntegerField {
    setup() {
        super.setup();
        this.timer = null;

        onMounted(() => {
            this._startTimer(this.props.record.data[this.props.name]);
        });

        onWillUpdateProps((nextProps) => {
            const newFreq = nextProps.record.data[nextProps.name];
            if (newFreq !== this.props.record.data[this.props.name]) {
                this._startTimer(newFreq);
            }
        });

        onWillUnmount(() => {
            this._stopTimer();
        });
    }

    _startTimer(freq) {
        this._stopTimer();
        
        if (freq && freq >= 10) {
            this.timer = setInterval(() => {
                if (!this.props.record.isDirty && !this.props.record.isSaving) {
                    this.props.record.load(); 
                }
            }, freq * 1000);
        }
    }

    _stopTimer() {
        if (this.timer) {
            clearInterval(this.timer);
            this.timer = null;
        }
    }
}

registry.category("fields").add("auto_refresh", {
    component: AutoRefreshWidget,
    supportedTypes: ["integer"],
});