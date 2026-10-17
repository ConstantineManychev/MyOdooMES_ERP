/** @odoo-module **/

import { registry } from "@web/core/registry";
import { Component, onWillUnmount, useState } from "@odoo/owl";
import { standardFieldProps } from "@web/views/fields/standard_field_props";
import { serializeDateTime } from "@web/core/l10n/dates";
import { acceptLiveStatus, newerSavedStatus, savedStatusIsCurrent } from "./live_status";

const STATUS_CHANNEL = "mes.workcenter.status";
const LIVE_EVENT = "workcenter_status_color";
const SAVED_EVENT = "workcenter_status_saved";

const HEX_COLOR_REGEX = /^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/;

function statusKey(status) {
    return [status.since, status.label, status.color, status.provisional].join("|");
}

function relativeLuminance(hex) {
    let value = hex.slice(1);
    if (value.length === 3) {
        value = value.split("").map((char) => char + char).join("");
    }
    const channels = [0, 2, 4].map((offset) => {
        const raw = parseInt(value.slice(offset, offset + 2), 16) / 255;
        return raw <= 0.03928 ? raw / 12.92 : Math.pow((raw + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
}

export class StatusColoredName extends Component {
    static template = "mes_core.StatusColoredName";
    static props = { ...standardFieldProps, colorField: { type: String, optional: true } };

    setup() {
        this.status = useState({ live: null, saved: null });
        this.busService = this.env.services && this.env.services.bus_service
            ? this.env.services.bus_service : null;
        if (!this.busService) return;
        this.onNotification = ({ detail }) => {
            for (const { type, payload } of detail || []) {
                this.handleStatus(type, payload);
            }
        };
        try {
            this.busService.addChannel(STATUS_CHANNEL);
            this.busService.addEventListener("notification", this.onNotification);
            onWillUnmount(() => this.busService.removeEventListener("notification", this.onNotification));
        } catch (_) {
            this.busService = null;
        }
    }

    handleStatus(type, payload) {
        if (!payload || payload.workcenter_id !== this.props.record.resId) return;
        if (type === LIVE_EVENT && acceptLiveStatus(this.status.live, payload)) {
            this.status.live = {
                label: payload.label || "",
                color: payload.color || "",
                state_since: payload.state_since,
                has_reason: Boolean(payload.has_reason),
            };
        } else if (type === SAVED_EVENT) {
            this.status.saved = {
                label: payload.label || "",
                color: payload.color || "",
                since: payload.since || "",
                provisional: Boolean(payload.provisional),
                recordKey: statusKey(this.recordStatus),
            };
        }
    }

    get recordStatus() {
        const data = this.props.record.data;
        const since = data.mes_status_since;
        return {
            label: data.mes_status_label || "",
            color: data[this.props.colorField || "mes_status_color"] || "",
            since: since ? serializeDateTime(since) : "",
            provisional: Boolean(data.mes_status_provisional),
        };
    }

    get savedStatus() {
        const fromRecord = this.recordStatus;
        const pushed = this.status.saved;
        if (!pushed) return fromRecord;
        const reloaded = pushed.recordKey !== statusKey(fromRecord);
        return (reloaded ? newerSavedStatus(pushed, fromRecord) : newerSavedStatus(fromRecord, pushed)) || fromRecord;
    }

    get shownStatus() {
        const saved = this.savedStatus;
        const live = this.status.live;
        return savedStatusIsCurrent(saved, live) ? saved : live;
    }

    get label() {
        return this.props.record.data[this.props.name] || "";
    }

    get backgroundColor() {
        const color = this.shownStatus.color;
        return HEX_COLOR_REGEX.test(color || "") ? color : null;
    }

    get textColor() {
        const background = this.backgroundColor;
        if (!background) return null;
        return relativeLuminance(background) > 0.55 ? "#1b1b1b" : "#ffffff";
    }

    get title() {
        return this.shownStatus.label || "";
    }
}

export const statusColoredName = {
    component: StatusColoredName,
    displayName: "Status Coloured Name",
    supportedTypes: ["char"],
    extractProps: ({ options }) => ({ colorField: options.color_field }),
    fieldDependencies: [
        { name: "mes_status_color", type: "char" },
        { name: "mes_status_label", type: "char" },
        { name: "mes_status_since", type: "datetime" },
        { name: "mes_status_provisional", type: "boolean" },
    ],
};

registry.category("fields").add("mes_status_colored_name", statusColoredName);
