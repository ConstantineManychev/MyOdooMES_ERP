/** @odoo-module **/

import { Component, useState } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";

import { meshRpc } from "./mesh_rpc";

const _t = (() => { try { return require("@web/core/l10n/translation")._t; } catch (_) {} return (str) => str; })();

export class KioskHelpFlow extends Component {
    static template = "mes_core.KioskHelpFlow";
    static props = {
        helpRequest: Object,
        sessions: Object,
        workcenterId: { type: Number, optional: true },
        supervisorConfirmationRequired: { type: Boolean, optional: true },
        readonly: { type: Boolean, optional: true },
        onRefresh: Function,
    };

    setup() {
        this.notification = useService("notification");
        this.state = useState({
            comment: "",
            supComment: "",
            confirmComment: "",
            busy: false,
            editingReason: false,
            prefill: null,
            editArea: "manual",
            editEventId: null,
            editCountId: null,
            editDefectId: null,
            editComment: "",
        });
    }

    get task() { return this.props.helpRequest; }
    get supervisorSession() { return this.props.sessions?.supervisor; }
    get technicianSession() { return this.props.sessions?.technician; }
    get operatorSession() { return this.props.sessions?.operator; }

    get activeSubtask() {
        return (this.task?.subtasks || []).find(
            (s) => !['help_supervisor_confirmed', 'help_completed', 'cancel'].includes(s.state)
        ) || null;
    }

    get canEditReason() {
        if (this.props.readonly) return false;
        const editableStates = [
            'help_open', 'help_supervisor_accepted', 'help_tech_requested',
            'new', 'assigned', 'on_hold',
        ];
        if (!editableStates.includes(this.task?.state)) return false;
        const sub = this.activeSubtask;
        const techActive = sub && this.technicianSession &&
            ['assigned', 'on_hold'].includes(sub.state);
        return Boolean(this.supervisorSession || techActive);
    }

    get editActorRole() {
        const sub = this.activeSubtask;
        if (sub && this.technicianSession &&
            ['assigned', 'on_hold'].includes(sub.state)) {
            return "technician";
        }
        return "supervisor";
    }

    get areaLabels() {
        return {
            alarm: _t("Alarm"),
            rejection: _t("Rejection"),
            defect: _t("Defect"),
            manual: _t("Manual"),
        };
    }

    async call(endpoint, payload = {}) {
        if (this.props.readonly || this.state.busy) return null;
        this.state.busy = true;
        try {
            const result = await meshRpc(endpoint, payload);
            if (!result.ok) {
                this.notification.add(result.error || _t("Action failed."), { type: "danger" });
                return null;
            }
            await this.props.onRefresh();
            return result;
        } catch (err) {
            this.notification.add(_t("Error: ") + err.message, { type: "danger" });
            return null;
        } finally {
            this.state.busy = false;
        }
    }

    onSupervisorAccept() {
        if (!this.supervisorSession) {
            this.notification.add(_t("Supervisor must scan FOB or log in first."), { type: "warning" });
            return;
        }
        this.call(`/mes/kiosk/help/${this.task.id}/supervisor_accept`, {
            supervisor_session_id: this.supervisorSession.id,
        });
    }

    async onSupervisorClose() {
        if (!this.state.supComment.trim()) {
            this.notification.add(_t("A comment is required to close the request."), { type: "warning" });
            return;
        }
        const res = await this.call(`/mes/kiosk/help/${this.task.id}/supervisor_close`, {
            comment: this.state.supComment,
        });
        if (res) this.state.supComment = "";
    }

    onRequestTechnician() {
        this.call(`/mes/kiosk/help/${this.task.id}/request_technician`, { payload: {} });
    }

    onTechnicianArrive() {
        const sub = this.activeSubtask;
        if (!sub || !this.technicianSession) {
            this.notification.add(_t("Technician must scan FOB first."), { type: "warning" });
            return;
        }
        this.call(`/mes/kiosk/help/${sub.id}/tech_arrive`, {
            technician_session_id: this.technicianSession.id,
        });
    }

    get isSubtaskPaused() {
        return this.activeSubtask?.state === 'on_hold';
    }

    get canSupervisorConfirmSubtask() {
        return Boolean(
            this.props.supervisorConfirmationRequired &&
            this.activeSubtask?.state === 'done' &&
            this.supervisorSession
        );
    }

    async onTechnicianAction(action) {
        const sub = this.activeSubtask;
        if (!sub) return;
        if (!this.state.comment.trim()) {
            this.notification.add(_t("A comment is required for pause, reject and close."), { type: "warning" });
            return;
        }
        const res = await this.call(`/mes/kiosk/help/${sub.id}/tech_${action}`, { comment: this.state.comment });
        if (res) this.state.comment = "";
    }

    async onTechnicianResume() {
        const sub = this.activeSubtask;
        if (!sub) return;
        const res = await this.call(`/mes/kiosk/help/${sub.id}/tech_resume`, { comment: this.state.comment });
        if (res) this.state.comment = "";
    }

    onSupervisorConfirmSubtask() {
        const sub = this.activeSubtask;
        if (!sub) return;
        this.call(`/mes/kiosk/help/${sub.id}/supervisor_confirm`, {});
    }

    onOperatorConfirm(confirmed) {
        if (!this.operatorSession) {
            this.notification.add(_t("Operator session is required."), { type: "warning" });
            return;
        }
        this.call(`/mes/kiosk/help/${this.task.id}/operator_confirm`, {
            operator_session_id: this.operatorSession.id,
            confirmed,
            comment: this.state.confirmComment,
        });
    }

    formatRemaining(totalSec) {
        const s = Math.max(0, Math.floor(totalSec || 0));
        const m = Math.floor(s / 60);
        const r = s % 60;
        return m > 0 ? `${m}m ${r}s` : `${r}s`;
    }

    async toggleEditReason() {
        if (this.state.editingReason) {
            this.state.editingReason = false;
            return;
        }
        if (!this.state.prefill) {
            try {
                const data = await meshRpc("/mes/kiosk/help/prefill", { workcenter_id: this.props.workcenterId });
                if (data.ok) this.state.prefill = data;
            } catch (_) {}
        }
        this.state.editArea = this.task.problem_area || "manual";
        this.state.editEventId = this.task.reason_event_id || null;
        this.state.editCountId = this.task.reason_count_id || null;
        this.state.editDefectId = this.task.reason_defect_id || null;
        this.state.editComment = "";
        this.state.editingReason = true;
    }

    setEditArea(area) {
        this.state.editArea = area;
    }

    async submitReasonUpdate() {
        const a = this.state.editArea;
        if (a === "alarm" && !this.state.editEventId) {
            this.notification.add(_t("Select a stoppage reason."), { type: "warning" });
            return;
        }
        if (a === "rejection" && !this.state.editCountId) {
            this.notification.add(_t("Select a reject count."), { type: "warning" });
            return;
        }
        if (a === "defect" && !this.state.editDefectId) {
            this.notification.add(_t("Select a defect type."), { type: "warning" });
            return;
        }
        if (!this.state.editComment.trim()) {
            this.notification.add(_t("A comment is required to change the reason."), { type: "warning" });
            return;
        }
        const res = await this.call(`/mes/kiosk/help/${this.task.id}/update_reason`, {
            actor_role: this.editActorRole,
            payload: {
                problem_area: a,
                reason_event_id: a === "alarm" ? this.state.editEventId : null,
                reason_count_id: a === "rejection" ? this.state.editCountId : null,
                reason_defect_id: a === "defect" ? this.state.editDefectId : null,
                comment: this.state.editComment,
            },
        });
        if (res) {
            this.state.editingReason = false;
            this.state.editComment = "";
        }
    }
}
