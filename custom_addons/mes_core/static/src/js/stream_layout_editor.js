/** @odoo-module **/

import { registry } from "@web/core/registry";
import { Component, useState, useRef } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";

const _t = (() => { try { return require("@web/core/l10n/translation")._t; } catch (_) {} return (str) => str; })();

export class StreamLayoutEditor extends Component {
    static template = "mes_core.StreamLayoutEditor";
    static props = { "*": true };

    setup() {
        this.orm = useService("orm");
        this.notification = useService("notification");
        this.canvasRef = useRef("canvas");
        this.state = useState({
            open: false,
            loading: false,
            saving: false,
            canvasWidth: 0,
            canvasHeight: 0,
            configured: false,
            items: [],
        });
        this.dragging = null;
    }

    get workcenterId() {
        return this.props.record?.resId || null;
    }

    async openEditor() {
        const record = this.props.record;
        if (record?.save) {
            const saved = await record.save();
            if (!saved) return;
        }
        if (!this.workcenterId) {
            this.notification.add(_t("Save the work center before configuring the stream layout."), { type: "warning" });
            return;
        }
        this.state.open = true;
        this.state.loading = true;
        try {
            const layout = await this.orm.call("mrp.workcenter", "get_stream_layout", [[this.workcenterId]]);
            this.state.canvasWidth = layout.canvas_width;
            this.state.canvasHeight = layout.canvas_height;
            this.state.configured = !!layout.configured;
            this.state.items = layout.items.map((item) => ({ ...item }));
        } catch (err) {
            this.notification.add(_t("Cannot load the stream layout."), { type: "danger" });
            this.state.open = false;
        }
        this.state.loading = false;
    }

    closeEditor() {
        this.state.open = false;
        this.dragging = null;
    }

    startDrag(item, event) {
        event.preventDefault();
        const canvas = this.canvasRef.el;
        if (!canvas) return;
        const bounds = canvas.getBoundingClientRect();
        const pointer = this.pointerPosition(event);
        this.dragging = {
            item,
            offsetX: pointer.clientX - (bounds.left + (item.pos_x / 100) * bounds.width),
            offsetY: pointer.clientY - (bounds.top + (item.pos_y / 100) * bounds.height),
        };
        this.bindDragListeners();
    }

    pointerPosition(event) {
        const touch = event.touches?.[0] || event.changedTouches?.[0];
        return touch ? { clientX: touch.clientX, clientY: touch.clientY }
                     : { clientX: event.clientX, clientY: event.clientY };
    }

    bindDragListeners() {
        this.onMove = (event) => this.moveDrag(event);
        this.onUp = () => this.endDrag();
        window.addEventListener("mousemove", this.onMove);
        window.addEventListener("mouseup", this.onUp);
        window.addEventListener("touchmove", this.onMove, { passive: false });
        window.addEventListener("touchend", this.onUp);
    }

    unbindDragListeners() {
        window.removeEventListener("mousemove", this.onMove);
        window.removeEventListener("mouseup", this.onUp);
        window.removeEventListener("touchmove", this.onMove);
        window.removeEventListener("touchend", this.onUp);
    }

    moveDrag(event) {
        if (!this.dragging) return;
        event.preventDefault();
        const canvas = this.canvasRef.el;
        if (!canvas) return;
        const bounds = canvas.getBoundingClientRect();
        const pointer = this.pointerPosition(event);
        const rawX = pointer.clientX - this.dragging.offsetX - bounds.left;
        const rawY = pointer.clientY - this.dragging.offsetY - bounds.top;
        const maxX = Math.max(1, bounds.width - this.dragging.item.width);
        const maxY = Math.max(1, bounds.height - this.dragging.item.height);
        this.dragging.item.pos_x = (Math.min(Math.max(0, rawX), maxX) / bounds.width) * 100;
        this.dragging.item.pos_y = (Math.min(Math.max(0, rawY), maxY) / bounds.height) * 100;
    }

    endDrag() {
        this.unbindDragListeners();
        this.dragging = null;
    }

    onCanvasSizeChange(axis, ev) {
        const value = Math.max(0, Math.min(4000, parseInt(ev.target.value, 10) || 0));
        if (axis === "width") {
            this.state.canvasWidth = value;
        } else {
            this.state.canvasHeight = value;
        }
        this.state.configured = !!(this.state.canvasWidth && this.state.canvasHeight);
    }

    resetCanvasSize() {
        this.state.canvasWidth = 0;
        this.state.canvasHeight = 0;
        this.state.configured = false;
    }

    async saveLayout() {
        this.state.saving = true;
        try {
            await this.orm.call("mrp.workcenter", "save_stream_canvas", [
                [this.workcenterId],
                this.state.canvasWidth,
                this.state.canvasHeight,
            ]);
            await this.orm.call("mrp.workcenter", "save_stream_layout", [
                [this.workcenterId],
                this.state.items.map((item) => ({
                    stream_id: item.stream_id,
                    pos_x: item.pos_x,
                    pos_y: item.pos_y,
                })),
            ]);
            this.notification.add(_t("Stream layout saved."), { type: "success" });
            this.closeEditor();
        } catch (err) {
            this.notification.add(_t("Cannot save the stream layout."), { type: "danger" });
        }
        this.state.saving = false;
    }
}

export const streamLayoutEditor = {
    component: StreamLayoutEditor,
};

registry.category("view_widgets").add("mes_stream_layout_editor", streamLayoutEditor);
