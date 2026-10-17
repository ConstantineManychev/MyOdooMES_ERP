/** @odoo-module **/

import { computeStreamCanvasSize } from "./stream_canvas";

import { Component, useState, onWillStart } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";

import { meshRpc } from "./mesh_rpc";

const _t = (() => { try { return require("@web/core/l10n/translation")._t; } catch (_) {} return (str) => str; })();

export class KioskHelpForm extends Component {
    static template = "mes_core.KioskHelpForm";
    static props = {
        workcenterId: Number,
        operatorSessionId: Number,
        onCreated: Function,
        onClose: Function,
    };

    setup() {
        this.notification = useService("notification");
        this.state = useState({
            prefill: null,
            area: "manual",
            description: "",
            reason_event_id: null,
            reason_count_id: null,
            reason_defect_id: null,
            part_id: null,
            piece_ids: [],
            streamPartId: null,
            streamPieceIds: [],
            streamSearch: "",
            pickedReasonLabel: null,
            streamPicker: null,
            streamReasons: null,
            streamTab: null,
            streamLoading: false,
            busy: false,
        });

        onWillStart(async () => {
            try {
                const data = await meshRpc("/mes/kiosk/help/prefill", {
                    workcenter_id: this.props.workcenterId,
                });
                if (data.ok) {
                    this.state.prefill = data;
                    this.state.area = data.default_area || "manual";
                    this.state.reason_event_id = data.alarm?.suggested_event_id || null;
                    this.state.reason_count_id = data.rejection?.suggested_count_id || null;
                    const defects = data.defect?.options || [];
                    this.state.reason_defect_id = defects.length ? defects[0].id : null;
                }
            } catch (_) {}
        });
    }

    get currentReasonLabel() {
        if (this.state.pickedReasonLabel) return this.state.pickedReasonLabel;
        const prefill = this.state.prefill;
        if (this.state.area === "alarm" && this.state.reason_event_id) {
            const opt = (prefill?.alarm?.options || []).find((o) => o.id === this.state.reason_event_id);
            return opt?.label || prefill?.alarm?.suggested_label || _t("Not set");
        }
        if (this.state.area === "rejection" && this.state.reason_count_id) {
            const opt = (prefill?.rejection?.options || []).find((o) => o.id === this.state.reason_count_id);
            return opt?.label || prefill?.rejection?.suggested_label || _t("Not set");
        }
        if (this.state.area === "defect" && this.state.reason_defect_id) {
            const opt = (prefill?.defect?.options || []).find((o) => o.id === this.state.reason_defect_id);
            return opt?.label || _t("Not set");
        }
        return prefill?.alarm?.suggested_label || prefill?.rejection?.suggested_label || _t("Not set");
    }

    setArea(area) {
        this.state.area = area;
    }

    get streamLayout() {
        return this.state.prefill?.stream_layout || null;
    }

    get availableStreams() {
        return this.streamLayout?.items || [];
    }

    get streamCanvasSize() {
        return computeStreamCanvasSize(this.streamLayout);
    }

    get streamReasonTabs() {
        const reasons = this.state.streamReasons;
        if (!reasons) return [];
        return [
            { key: "alarm", label: this.areaLabels.alarm, items: reasons.alarm || [] },
            { key: "rejection", label: this.areaLabels.rejection, items: reasons.rejection || [] },
            { key: "defect", label: this.areaLabels.defect, items: reasons.defect || [] },
            { key: "manual", label: this.areaLabels.manual, items: reasons.manual || [] },
        ].filter((tab) => tab.items.length);
    }

    get streamTabParts() {
        const parts = this.state.streamReasons?.parts || {};
        return parts[this.state.streamTab] || [];
    }

    get activeStreamPart() {
        return this.streamTabParts.find((p) => p.id === this.state.streamPartId) || null;
    }

    selectStreamPart(partId, ev) {
        if (ev?.currentTarget?.blur) ev.currentTarget.blur();
        this.state.streamPartId = this.state.streamPartId === partId ? null : partId;
        this.state.streamPieceIds = [];
    }

    toggleStreamPiece(pieceId, ev) {
        if (ev?.currentTarget?.blur) ev.currentTarget.blur();
        const next = this.state.streamPieceIds.filter((id) => id !== pieceId);
        if (next.length === this.state.streamPieceIds.length) next.push(pieceId);
        this.state.streamPieceIds = next;
    }

    get filteredStreamItems() {
        const tab = this.streamReasonTabs.find((t) => t.key === this.state.streamTab);
        if (!tab) return [];
        const partId = this.state.streamPartId;
        const pieceIds = this.state.streamPieceIds;
        const needle = (this.state.streamSearch || "").trim().toLowerCase();
        return tab.items.filter((item) => {
            if (partId && item.part_id && item.part_id !== partId) return false;
            if (pieceIds.length && item.piece_id && !pieceIds.includes(item.piece_id)) return false;
            if (needle && !(item.label || "").toLowerCase().includes(needle)) return false;
            return true;
        });
    }

    clearStreamSearch() {
        this.state.streamSearch = "";
    }

    get selectedPieceNames() {
        const part = this.activeStreamPart;
        if (!part) return [];
        return part.pieces
            .filter((piece) => this.state.streamPieceIds.includes(piece.id))
            .map((piece) => piece.name);
    }

    async selectStream(stream) {
        this.state.streamPicker = stream;
        this.state.streamReasons = null;
        this.state.streamLoading = true;
        try {
            const res = await meshRpc("/mes/kiosk/help/stream_reasons", {
                workcenter_id: this.props.workcenterId,
                stream_id: stream.stream_id,
            });
            if (res.ok) {
                this.state.streamReasons = res.reasons;
                const tabs = this.streamReasonTabs;
                this.state.streamTab = tabs.length ? tabs[0].key : null;
            } else {
                this.notification.add(res.error || _t("Cannot load stream reasons."), { type: "warning" });
                this.closeStreamPicker();
            }
        } catch (err) {
            this.notification.add(_t("Error: ") + err.message, { type: "danger" });
            this.closeStreamPicker();
        }
        this.state.streamLoading = false;
    }

    setStreamTab(key) {
        this.state.streamTab = key;
        this.state.streamPartId = null;
        this.state.streamPieceIds = [];
        this.state.streamSearch = "";
    }

    closeStreamPicker() {
        this.state.streamPicker = null;
        this.state.streamReasons = null;
        this.state.streamTab = null;
        this.state.streamLoading = false;
        this.state.streamPartId = null;
        this.state.streamPieceIds = [];
        this.state.streamSearch = "";
    }

    selectStreamReason(tabKey, item) {
        this.state.reason_event_id = null;
        this.state.reason_count_id = null;
        this.state.reason_defect_id = null;

        if (tabKey === "alarm") {
            this.state.area = "alarm";
            this.state.reason_event_id = item.id;
        } else if (tabKey === "rejection") {
            this.state.area = "rejection";
            this.state.reason_count_id = item.id;
        } else if (tabKey === "defect") {
            this.state.area = "defect";
            this.state.reason_defect_id = item.id;
        } else {
            this.state.area = "manual";
            this.state.reason_event_id = item.id;
        }

        const pieceNames = this.selectedPieceNames;
        this.state.part_id = this.state.streamPartId || item.part_id || null;
        this.state.piece_ids = [...this.state.streamPieceIds];
        this.state.description = this.composeDescription(item.label, pieceNames);
        this.state.pickedReasonLabel = item.label;
        this.closeStreamPicker();
    }

    composeDescription(reasonLabel, pieceNames) {
        const segments = [];
        if (this.state.streamPicker?.name) segments.push(this.state.streamPicker.name);
        const part = this.activeStreamPart;
        if (part) segments.push(part.name);
        if (reasonLabel) segments.push(reasonLabel);
        if (pieceNames.length) segments.push(pieceNames.join(", "));
        return segments.join(" / ");
    }

    get areaLabels() {
        return {
            alarm: _t("Alarm"),
            rejection: _t("Rejection"),
            defect: _t("Defect"),
            manual: _t("Manual"),
        };
    }

    validate() {
        const a = this.state.area;
        if (a === "alarm" && !this.state.reason_event_id) {
            return _t("Select a stoppage reason.");
        }
        if (a === "rejection" && !this.state.reason_count_id) {
            return _t("Select a reject count.");
        }
        if (a === "defect" && !this.state.reason_defect_id) {
            return _t("Select a defect type.");
        }
        if (a === "manual" && !this.state.description.trim()) {
            return _t("Describe the problem.");
        }
        return null;
    }

    async submit() {
        const err = this.validate();
        if (err) {
            this.notification.add(err, { type: "warning" });
            return;
        }
        const a = this.state.area;
        const pf = this.state.prefill || {};
        let reasonSource = "manual";
        if (a === "alarm" && this.state.reason_event_id === pf.alarm?.suggested_event_id) {
            reasonSource = "auto_event";
        } else if (a === "rejection" && this.state.reason_count_id === pf.rejection?.suggested_count_id) {
            reasonSource = "auto_count";
        }
        this.state.busy = true;
        try {
            const result = await meshRpc("/mes/kiosk/help/create", {
                workcenter_id: this.props.workcenterId,
                operator_session_id: this.props.operatorSessionId,
                payload: {
                    problem_area: a,
                    reason_source: reasonSource,
                    reason_event_id: a === "alarm" ? this.state.reason_event_id : null,
                    reason_count_id: a === "rejection" ? this.state.reason_count_id : null,
                    reason_defect_id: a === "defect" ? this.state.reason_defect_id : null,
                    reason_manual_event_id: a === "manual" ? this.state.reason_event_id : null,
                    part_id: this.state.part_id || null,
                    piece_ids: this.state.piece_ids || [],
                    operator_description: this.state.description,
                    reason_label: this.state.pickedReasonLabel || this.currentReasonLabel,
                },
            });
            if (!result.ok) {
                this.notification.add(result.error || _t("Failed to create help request."), { type: "danger" });
                return;
            }
            this.notification.add(_t("Help request created. Awaiting supervisor."), { type: "success" });
            this.props.onCreated();
        } catch (err) {
            this.notification.add(_t("Error: ") + err.message, { type: "danger" });
        } finally {
            this.state.busy = false;
        }
    }
}
