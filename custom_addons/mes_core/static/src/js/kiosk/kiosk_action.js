/** @odoo-module **/

import { Component, useState, onWillStart, onMounted, onWillUnmount } from "@odoo/owl";
import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { meshRpc } from "./mesh_rpc";
import { computeStreamCanvasSize } from "./stream_canvas";
import { KioskHelpForm } from "./help_request_form";
import { KioskHelpFlow } from "./help_flow";
import { KioskCharts } from "./kiosk_charts";
import { useFobReader } from "./fob_listener";
import { acceptLiveStatus, savedStatusIsCurrent } from "../live_status";

const _t = (() => {
    try {
        return require("@web/core/l10n/translation")._t;
    } catch (_) {}
    return (str) => str;
})();

const POLL_INTERVAL_MS = 5000;
const POLL_INTERVAL_NO_BUS_MS = 1000;
const PENDING_STOPS_REFRESH_MS = 60000;
const WORKCENTER_STORAGE_KEY = "mes_kiosk_workcenter_id";
const VIEWER_FLAGS = new Set([true, 1, "1", "true"]);
const SUPERVISOR_BOARD_ACTION = "mes_core.action_mes_supervisor_board";

export class KioskAction extends Component {
    static template = "mes_core.KioskAction";
    static components = { KioskHelpForm, KioskHelpFlow, KioskCharts };
    static props = ["*"];

    setup() {
        this.notification = useService("notification");
        this.actionService = useService("action");
        this.router = useService("router");
        this.viewerMode = this._resolveViewerMode();

        this.state = useState({
            workcenterId: this._resolveWorkcenterId(),
            data: null,
            loading: true,
            modal: null,
            modalContext: {},
            loginTab: "pin",
            loginField: "",
            pinField: "",
            loginError: null,
            loginBusy: false,
            liveStatus: null,
            oeeModal: false,
            oeeLoading: false,
            oeeError: false,
            oeeData: null,
            oeeOpen: [],
            oeeDetail: null,
            queuedComment: null,
            pendingStops: [],
            pendingStopsModal: false,
            pendingStopsSelected: [],
            pendingStopsComment: '',
            pendingStopsSaving: false,
            forceCountId: false,
            lossModal: null,
            lossLoading: false,
            lossItems: [],
            lossActiveTab: 'all',
            lossSortField: null,
            lossSortDir: 'desc',
            timelineModalOpen: false,
            timelineItems: [],
            timelineSortField: 'start',
            timelineSortDir: 'desc',
            helpListModal: false,
            selectedHelpRequestId: null,
            customChartModal: false,
            plannedDowntimeBusy: false,
            unknownTagModal: null,
            collapsedTimelineGroups: {},
            commentModal: null,
            commentDraft: '',
            commentSaving: false,
            streamPicker: null,
            streamEvents: [],
            streamEventsLoading: false,
            streamParts: [],
            streamPartId: null,
            streamPieceIds: [],
            prevShift: null,
            compareModalOpen: false,
            compareTab: 'downtime',
            compareLoading: false,
            compareData: { downtime: null, waste: null },
            compareWasteTab: 'all',
            compareSortField: null,
            compareSortDir: 'desc',
        });

        this.loadedShiftKey = null;
        this.busChannel = null;
        this.busSubscribed = false;
        this.busService = this.env.services && this.env.services.bus_service
            ? this.env.services.bus_service : null;
        const fobReader = useFobReader((uid, source, alternatives) =>
            this.onFobScan(uid, source, alternatives), { enabled: !this.viewerMode });
        this.nfc = fobReader.nfc;
        this.startNfc = fobReader.startNfc;
        this.setPassiveScan = fobReader.setPassive;
        this.setFobOverride = fobReader.setOverride;
        this.scanFobOnce = fobReader.scanOnce;
        this.toggleNfcDiagnostics = fobReader.toggleDiagnostics;

        onWillStart(async () => {
            if (!this.state.workcenterId) {
                this.state.workcenterId = await this._waitForWorkcenterId();
            }
            if (!this.state.workcenterId && !this.viewerMode) {
                this.state.workcenterId = this._loadPersistedWorkcenterId();
            }
            if (!this.state.workcenterId) {
                if (this.viewerMode) {
                    this.state.loading = false;
                    return;
                }
                window.location.href = "/mes/kiosk/auto";
                return;
            }
            if (!this.viewerMode) {
                this._persistWorkcenterId(this.state.workcenterId);
            }
            await this.refresh();
        });

        onMounted(() => {
            const pollInterval = this.busService ? POLL_INTERVAL_MS : POLL_INTERVAL_NO_BUS_MS;
            this.pollHandle = window.setInterval(() => this.refresh(), pollInterval);
            if (this.viewerMode && this.state.workcenterId) {
                this.router.pushState({ workcenter_id: this.state.workcenterId, viewer: 1 });
            }
        });

        onWillUnmount(() => {
            window.clearInterval(this.pollHandle);
        });
    }

    _resolveViewerMode() {
        const action = this.props.action || {};
        const hash = new URLSearchParams(window.location.hash.replace(/^#/, ''));
        return [
            action.params?.viewer,
            action.context?.viewer,
            action.context?.params?.viewer,
            hash.get('viewer'),
        ].some((flag) => VIEWER_FLAGS.has(flag));
    }

    async leaveViewer() {
        try {
            await this.actionService.restore();
        } catch (_) {
            await this.actionService.doAction(SUPERVISOR_BOARD_ACTION, { clearBreadcrumbs: true });
        }
    }

    _resolveWorkcenterId() {
        const fromAction =
            this.props.action?.context?.workcenter_id ||
            this.props.action?.params?.workcenter_id ||
            this.props.action?.context?.params?.workcenter_id;
        if (fromAction) return parseInt(fromAction);

        const hash = window.location.hash.replace(/^#/, '');
        const fromHash = new URLSearchParams(hash).get('workcenter_id');
        if (fromHash) return parseInt(fromHash);

        const fromQuery = new URLSearchParams(window.location.search).get('workcenter_id');
        if (fromQuery) return parseInt(fromQuery);

        return null;
    }

    async _waitForWorkcenterId(retries = 10, delayMs = 100) {
        for (let i = 0; i < retries; i++) {
            const id = this._resolveWorkcenterId();
            if (id) return id;
            await new Promise((r) => setTimeout(r, delayMs));
        }
        return null;
    }

    _loadPersistedWorkcenterId() {
        try {
            const stored = window.localStorage.getItem(WORKCENTER_STORAGE_KEY);
            const id = stored ? parseInt(stored) : null;
            return id && !Number.isNaN(id) ? id : null;
        } catch (_) {
            return null;
        }
    }

    _persistWorkcenterId(id) {
        try {
            window.localStorage.setItem(WORKCENTER_STORAGE_KEY, String(id));
        } catch (_) {
        }
    }

    _subscribeToWorkcenterBus() {
        if (!this.busService) return;
        const workcenterId = this.state.workcenterId;
        if (!workcenterId || this.busChannel === workcenterId) return;

        try {
            if (this.busChannel) {
                this.busService.deleteChannel(`mes.kiosk.workcenter.${this.busChannel}`);
            }
        } catch (_) {
            this.busService = null;
            return;
        }
        try {
            this.busService.addChannel(`mes.kiosk.workcenter.${workcenterId}`);
            this.busChannel = workcenterId;

            if (this.busSubscribed) return;
            for (const eventType of ["session_changed", "help_request_state", "help_request_created",
                                     "machine_state_changed"]) {
                this.busService.subscribe(eventType, () => this.refresh());
            }
            this.busService.subscribe("machine_live_state", (payload) => this.applyLiveStatus(payload));
            this.busSubscribed = true;
        } catch (_) {
            this.busService = null;
        }
    }

    async openOeeModal() {
        this.state.oeeModal = true;
        this.state.oeeLoading = true;
        this.state.oeeError = false;
        this.state.oeeData = null;
        this.state.oeeOpen = [];
        try {
            const res = await meshRpc("/mes/kiosk/oee_breakdown", {
                workcenter_id: this.state.workcenterId,
            });
            if (res.ok) {
                this.state.oeeData = res;
            } else {
                this.state.oeeError = res.error || _t("No data for current shift.");
            }
        } catch (err) {
            this.state.oeeError = err.message;
        }
        this.state.oeeLoading = false;
    }

    closeOeeModal() {
        this.state.oeeModal = false;
        this.state.oeeData = null;
        this.state.oeeOpen = [];
    }

    openOeeDetail(node, kind, ev) {
        if (ev?.stopPropagation) ev.stopPropagation();
        this.state.oeeDetail = {
            title: `${kind}: ${node.name}`,
            items: node.items || [],
        };
    }

    closeOeeDetail() {
        this.state.oeeDetail = null;
    }

    toggleOeeNode(key) {
        const next = this.state.oeeOpen.filter((k) => k !== key);
        if (next.length === this.state.oeeOpen.length) next.push(key);
        this.state.oeeOpen = next;
    }

    applyLiveStatus(payload) {
        if (!payload || payload.workcenter_id !== this.state.workcenterId) return;
        if (!acceptLiveStatus(this.state.liveStatus, payload)) return;
        this.state.liveStatus = payload;
    }

    _savedEventIsCurrent(fsmEvent, live) {
        const saved = fsmEvent ? { since: fsmEvent.start, provisional: fsmEvent.provisional } : null;
        return savedStatusIsCurrent(saved, live);
    }

    get displayedStatus() {
        const fsmEvent = this.state.data?.workcenter?.current_event || null;
        const live = this.state.liveStatus;
        if (!live) return fsmEvent;
        return this._savedEventIsCurrent(fsmEvent, live) ? fsmEvent : live;
    }

    get queuedCommentForCurrent() {
        const live = this.state.liveStatus;
        const queued = this.state.queuedComment;
        if (!live || !queued) return '';
        if (queued.event_time && queued.event_time !== live.state_since) return '';
        return queued.comment || '';
    }

    get displayedStatusIsLive() {
        const shown = this.displayedStatus;
        return !!shown && !shown.model;
    }

    async refresh() {
        if (!this.state.workcenterId) return;
        try {
            this._subscribeToWorkcenterBus();
            const includePending = this._shouldIncludePending();
            this.state.data = await meshRpc("/mes/kiosk/state", {
                workcenter_id: this.state.workcenterId,
                include_pending: includePending,
            });
            if (includePending) {
                this.state.pendingStops = this.state.data?.pending_operator_stops || [];
                this.lastPendingFetch = Date.now();
                this.forcePendingRefresh = false;
            }
            this._dropStaleLiveStatus();
            this.state.queuedComment = this.state.data?.queued_comment || null;
            const pending = this.viewerMode ? null : this.pendingOperatorConfirmation;
            if (pending && this.announcedConfirmationId !== pending.id) {
                this.announcedConfirmationId = pending.id;
                this.notification.add(
                    _t("Please confirm completion of the open help request."),
                    { type: "warning", sticky: false }
                );
            } else if (!pending) {
                this.announcedConfirmationId = null;
            }
            if (this.state.helpListModal && !this.helpRequestSummaries.length) {
                this.state.helpListModal = false;
                this.state.selectedHelpRequestId = null;
            }
        } catch (_) {
            this.notification.add(_t("Failed to refresh kiosk state."), { type: "danger" });
        } finally {
            this.state.loading = false;
        }
        const shiftKey = this.state.data?.workcenter?.current_shift_key || '';
        if (this.state.prevShift === null || (shiftKey && shiftKey !== this.loadedShiftKey)) {
            const shiftChanged = !!this.loadedShiftKey && shiftKey !== this.loadedShiftKey;
            this.loadedShiftKey = shiftKey;
            this.loadPrevShiftSummary();
            if (shiftChanged) {
                this.state.pendingStops = [];
                this.forcePendingRefresh = true;
            }
        }
    }

    async loadPrevShiftSummary() {
        try {
            const res = await meshRpc("/mes/kiosk/previous_shift_summary", {
                workcenter_id: this.state.workcenterId,
            });
            this.state.prevShift = res.ok ? res : false;
        } catch (_) {
            this.state.prevShift = false;
        }
    }

    async openCompareModal() {
        this.state.compareModalOpen = true;
        this.state.compareTab = 'downtime';
        this.state.compareWasteTab = 'all';
        this.state.compareLoading = true;
        try {
            const [dt, waste] = await Promise.all([
                meshRpc("/mes/kiosk/shift_comparison", { workcenter_id: this.state.workcenterId, kind: 'downtime' }),
                meshRpc("/mes/kiosk/shift_comparison", { workcenter_id: this.state.workcenterId, kind: 'waste' }),
            ]);
            this.state.compareData = {
                downtime: dt.ok ? dt : { current: [], previous: [] },
                waste: waste.ok ? waste : { current: [], previous: [] },
            };
        } catch (err) {
            this.notification.add(_t("Error: ") + err.message, { type: "danger" });
        }
        this.state.compareLoading = false;
    }

    closeCompareModal() {
        this.state.compareModalOpen = false;
    }

    setCompareTab(tab) {
        this.state.compareTab = tab;
        this.state.compareSortField = null;
        this.state.compareSortDir = 'desc';
    }

    setCompareWasteTab(tab) {
        this.state.compareWasteTab = tab;
    }

    sortCompareBy(field) {
        if (this.state.compareSortField === field) {
            this.state.compareSortDir = this.state.compareSortDir === 'asc' ? 'desc' : 'asc';
        } else {
            this.state.compareSortField = field;
            this.state.compareSortDir = 'desc';
        }
    }

    _applyCompareSort(rows) {
        const field = this.state.compareSortField;
        if (!field) return rows;
        const dir = this.state.compareSortDir === 'asc' ? 1 : -1;
        const val = (row) => {
            if (field === 'name') return row.name;
            const [side, metric] = field.split('.');
            return row[side]?.[metric] || 0;
        };
        return rows.slice().sort((a, b) => {
            const va = val(a), vb = val(b);
            if (typeof va === 'string') return va.localeCompare(vb) * dir;
            return (va - vb) * dir;
        });
    }

    _mergeComparison(currentList, previousList, keyField, metricFields) {
        const map = {};
        const blank = () => {
            const o = { name: '', current: {}, previous: {} };
            for (const f of metricFields) { o.current[f] = 0; o.previous[f] = 0; }
            return o;
        };
        for (const it of (currentList || [])) {
            if (!map[it[keyField]]) { map[it[keyField]] = blank(); map[it[keyField]].name = it[keyField]; }
            for (const f of metricFields) map[it[keyField]].current[f] = it[f] || 0;
        }
        for (const it of (previousList || [])) {
            if (!map[it[keyField]]) { map[it[keyField]] = blank(); map[it[keyField]].name = it[keyField]; }
            for (const f of metricFields) map[it[keyField]].previous[f] = it[f] || 0;
        }
        const primary = metricFields[0];
        return Object.values(map).sort((a, b) => (b.current[primary] + b.previous[primary]) - (a.current[primary] + a.previous[primary]));
    }

    get compareDowntimeRows() {
        const d = this.state.compareData.downtime;
        if (!d) return [];
        return this._applyCompareSort(
            this._mergeComparison(d.current, d.previous, 'name', ['freq_per_hour', 'avg_duration_per_stop'])
        );
    }

    get compareWasteWheelTabs() {
        const d = this.state.compareData.waste;
        if (!d) return [];
        const wheels = new Set();
        for (const it of [...(d.current || []), ...(d.previous || [])]) {
            if (it.wheel) wheels.add(it.wheel);
        }
        return Array.from(wheels).sort((a, b) => a - b);
    }

    get compareWasteRows() {
        const d = this.state.compareData.waste;
        if (!d) return [];
        const filt = (list) => {
            if (this.state.compareWasteTab === 'no_wheel') return (list || []).filter(it => !it.wheel);
            if (this.state.compareWasteTab !== 'all') {
                const w = parseInt(this.state.compareWasteTab.replace('wheel_', ''));
                return (list || []).filter(it => it.wheel === w && it.module);
            }
            return list || [];
        };
        return this._applyCompareSort(
            this._mergeComparison(filt(d.current), filt(d.previous), 'name', ['per_hour'])
        );
    }

    get hasOperator() { return !!this.state.data?.sessions?.operator; }
    get hasSupervisor() { return !!this.state.data?.sessions?.supervisor; }
    get hasTechnician() { return !!this.state.data?.sessions?.technician; }
    get hasActiveSession() { return this.hasOperator || this.hasSupervisor || this.hasTechnician; }

    requireSession() {
        if (this.viewerMode) {
            this.notification.add(_t("This screen is view only."), { type: "warning" });
            return false;
        }
        if (this.hasActiveSession) {
            return true;
        }
        this.notification.add(_t("Log in to make changes."), { type: "warning" });
        return false;
    }

    get canComment() {
        if (this.viewerMode) {
            return Boolean(this.state.data?.viewer_can_comment);
        }
        return this.hasActiveSession;
    }

    requireCommentRights() {
        if (this.canComment) {
            return true;
        }
        this.notification.add(
            this.viewerMode
                ? _t("Comments need an employee with a kiosk role linked to your user.")
                : _t("Log in to make changes."),
            { type: "warning" }
        );
        return false;
    }
    get helpRequest() { return this.state.data?.help_request || null; }

    _sessionAllowedFor(feature, preference) {
        const allowed = this.state.data?.permissions?.[feature] || [];
        const sessions = this.state.data?.sessions || {};
        const role = preference.find((code) => allowed.includes(code) && sessions[code]);
        return role ? sessions[role] : null;
    }

    get helpSession() {
        return this._sessionAllowedFor("help", ["operator", "supervisor", "technician"]);
    }

    get downtimeSession() {
        return this._sessionAllowedFor("planned_downtime", ["technician", "supervisor", "operator"]);
    }

    get helpButtonDisabled() { return !this.helpSession || !!this.helpRequest; }

    openLoginModal(targetRole) {
        if (this.viewerMode) return;
        const role = (typeof targetRole === "string") ? targetRole : null;
        this.state.modalContext = { targetRole: role };
        this.state.loginTab = "pin";
        this.state.loginField = "";
        this.state.pinField = "";
        this.state.loginError = null;
        this.state.loginBusy = false;
        this.state.modal = "login";
    }

    closeModal() {
        this.state.modal = null;
        this.state.modalContext = {};
        this.state.lossModal = null;
        this.state.lossItems = [];
        this.setFobOverride(false);
    }

    async openLossModal(kind) {
        this.state.lossModal = kind;
        this.state.lossLoading = true;
        this.state.lossItems = [];
        this.state.lossActiveTab = 'all';
        this.state.lossSortField = kind === 'waste' ? 'total' : 'duration_min';
        this.state.lossSortDir = 'desc';
        try {
            const result = await meshRpc("/mes/kiosk/loss_breakdown", {
                workcenter_id: this.state.workcenterId,
                kind,
            });
            if (result.ok) {
                this.state.lossItems = result.items || [];
            } else {
                this.notification.add(result.error || "Failed to load", { type: "warning" });
            }
        } catch (err) {
            this.notification.add(_t("Error: ") + err.message, { type: "danger" });
        }
        this.state.lossLoading = false;
    }

    selectLossCount(countId) {
        if (countId) {
            this.state.forceCountId = countId;
        }
        this.state.lossModal = null;
        this.state.lossItems = [];
    }

    clearForceCount() {
        this.state.forceCountId = false;
    }

    selectLossTab(tab) {
        this.state.lossActiveTab = tab;
    }

    sortLossBy(field) {
        if (this.state.lossSortField === field) {
            this.state.lossSortDir = this.state.lossSortDir === 'asc' ? 'desc' : 'asc';
        } else {
            this.state.lossSortField = field;
            this.state.lossSortDir = 'desc';
        }
    }

    get lossWheelTabs() {
        if (this.state.lossModal !== 'waste') return [];
        const wheels = new Set();
        for (const it of this.state.lossItems) {
            if (it.wheel) wheels.add(it.wheel);
        }
        return Array.from(wheels).sort((a, b) => a - b);
    }

    get lossWheelTotals() {
        const totals = {};
        if (this.state.lossModal !== 'waste') return totals;
        for (const it of this.state.lossItems) {
            if (!it.wheel) continue;
            if (!it.module) {
                totals[it.wheel] = (totals[it.wheel] || 0) + (it.total || 0);
            }
        }
        for (const w of this.lossWheelTabs) {
            if (totals[w] !== undefined) continue;
            let s = 0;
            for (const it of this.state.lossItems) {
                if (it.wheel === w && it.module) s += it.total || 0;
            }
            totals[w] = s;
        }
        return totals;
    }

    get visibleLossItems() {
        let items = this.state.lossItems.slice();
        if (this.state.lossModal === 'waste') {
            if (this.state.lossActiveTab === 'no_wheel') {
                items = items.filter(it => !it.wheel);
            } else if (this.state.lossActiveTab !== 'all') {
                const w = parseInt(this.state.lossActiveTab.replace('wheel_', ''));
                items = items.filter(it => it.wheel === w && it.module);
            }
        }
        const field = this.state.lossSortField;
        const dir = this.state.lossSortDir === 'asc' ? 1 : -1;
        if (field) {
            items.sort((a, b) => {
                const va = a[field], vb = b[field];
                if (typeof va === 'string') return va.localeCompare(vb) * dir;
                return ((va || 0) - (vb || 0)) * dir;
            });
        }
        return items;
    }

    async selectTopRejection() {
        const raw = this.state.data?.workcenter?.current_top_rejection;
        if (!raw || raw === 'None' || raw === '--') return;
        const countName = raw.replace(/\s*\(\d+\)\s*$/, '').trim();
        try {
            const result = await meshRpc("/mes/kiosk/loss_breakdown", {
                workcenter_id: this.state.workcenterId,
                kind: 'waste',
            });
            if (!result.ok) {
                this.notification.add(result.error || "Cannot load rejections", { type: "warning" });
                return;
            }
            const match = (result.items || []).find(it => it.name === countName);
            if (match && match.count_id) {
                this.state.forceCountId = match.count_id;
            } else {
                this.notification.add(`Count "${countName}" not found`, { type: "warning" });
            }
        } catch (err) {
            this.notification.add(_t("Error: ") + err.message, { type: "danger" });
        }
    }

    formatDurationHms(seconds) {
        const total = Math.max(0, Math.floor(seconds || 0));
        const h = Math.floor(total / 3600);
        const m = Math.floor((total % 3600) / 60);
        const s = total % 60;
        return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
    }

    formatDatetime(iso) {
        if (!iso) return '';
        try {
            return new Date(iso.includes('T') ? iso : iso.replace(' ', 'T')).toLocaleString();
        } catch {
            return iso;
        }
    }

    async openTimelineModal() {
        this.state.timelineModalOpen = true;
        this.state.timelineItems = [];
        try {
            const res = await meshRpc("/mes/kiosk/chart_data", {
                workcenter_id: this.state.workcenterId,
                selected_count_id: false,
                selected_process_id: false,
            });
            const items = (res?.timeline || []).map((b) => ({
                ...b,
                durationSec: (new Date(b.end.includes('T') ? b.end : b.end.replace(' ', 'T')) - new Date(b.start.includes('T') ? b.start : b.start.replace(' ', 'T'))) / 1000,
            }));
            this.state.timelineItems = items;
            this.state.timelineSortField = 'start';
            this.state.timelineSortDir = 'desc';
        } catch (err) {
            this.notification.add(_t("Error loading timeline: ") + err.message, { type: "danger" });
        }
    }

    closeTimelineModal() {
        this.state.timelineModalOpen = false;
    }

    sortTimelineBy(field) {
        if (this.state.timelineSortField === field) {
            this.state.timelineSortDir = this.state.timelineSortDir === 'asc' ? 'desc' : 'asc';
        } else {
            this.state.timelineSortField = field;
            this.state.timelineSortDir = 'desc';
        }
    }

    get sortedTimelineItems() {
        const items = this.state.timelineItems.slice();
        const field = this.state.timelineSortField;
        const dir = this.state.timelineSortDir === 'asc' ? 1 : -1;
        items.sort((a, b) => {
            const va = a[field], vb = b[field];
            if (typeof va === 'string') return va.localeCompare(vb) * dir;
            return ((va || 0) - (vb || 0)) * dir;
        });
        return items;
    }

    compareTimelineValues(a, b) {
        const field = this.state.timelineSortField;
        const dir = this.state.timelineSortDir === 'asc' ? 1 : -1;
        const va = a[field], vb = b[field];
        if (typeof va === 'string' || typeof vb === 'string') {
            return String(va ?? '').localeCompare(String(vb ?? '')) * dir;
        }
        return ((va || 0) - (vb || 0)) * dir;
    }

    get groupedTimelineItems() {
        const parseMs = (value) => new Date(String(value).replace(' ', 'T')).getTime();
        const items = this.state.timelineItems.slice();
        const overlays = items.filter((it) => it.overlay);
        const statuses = items.filter((it) => !it.overlay);
        const claimed = new Set();

        const groups = overlays.map((overlay) => {
            const from = parseMs(overlay.start);
            const to = parseMs(overlay.end);
            const children = statuses
                .filter((it) => {
                    const mid = (parseMs(it.start) + parseMs(it.end || it.start)) / 2;
                    return mid >= from && mid <= to;
                })
                .sort((a, b) => this.compareTimelineValues(a, b));
            children.forEach((child) => claimed.add(child));
            return {
                type: 'group',
                key: `${overlay.overlay}-${overlay.model}-${overlay.id}`,
                overlay,
                sortSource: overlay,
                children,
            };
        });

        const singles = statuses
            .filter((it) => !claimed.has(it))
            .map((event, index) => ({
                type: 'event',
                key: `e-${event.model || 'x'}-${event.id || index}-${event.start}`,
                event,
                sortSource: event,
            }));

        const rows = groups.concat(singles);
        rows.sort((a, b) => this.compareTimelineValues(a.sortSource, b.sortSource));
        return rows;
    }

    toggleTimelineGroup(key) {
        this.state.collapsedTimelineGroups = {
            ...this.state.collapsedTimelineGroups,
            [key]: !this.state.collapsedTimelineGroups[key],
        };
    }

    isTimelineGroupCollapsed(key) {
        return Boolean(this.state.collapsedTimelineGroups[key]);
    }

    get pendingOperatorStops() {
        return this.state.pendingStops || [];
    }

    _shouldIncludePending() {
        if (this.forcePendingRefresh) return true;
        if (!this.lastPendingFetch) return true;
        return Date.now() - this.lastPendingFetch >= PENDING_STOPS_REFRESH_MS;
    }

    pendingStopKey(item) {
        return `${item.model}:${item.id}`;
    }

    isPendingStopSelected(item) {
        return this.state.pendingStopsSelected.includes(this.pendingStopKey(item));
    }

    togglePendingStop(item) {
        const key = this.pendingStopKey(item);
        const next = this.state.pendingStopsSelected.filter((k) => k !== key);
        if (next.length === this.state.pendingStopsSelected.length) next.push(key);
        this.state.pendingStopsSelected = next;
    }

    get allPendingStopsSelected() {
        const items = this.pendingOperatorStops;
        return items.length > 0 && items.every((i) => this.isPendingStopSelected(i));
    }

    toggleAllPendingStops() {
        this.state.pendingStopsSelected = this.allPendingStopsSelected
            ? []
            : this.pendingOperatorStops.map((i) => this.pendingStopKey(i));
    }

    openPendingStopsModal() {
        if (!this.requireCommentRights()) return;
        this.state.pendingStopsModal = true;
        this.state.pendingStopsSelected = [];
        this.state.pendingStopsComment = '';
        this.closeStreamPicker();
    }

    closePendingStopsModal() {
        this.state.pendingStopsModal = false;
        this.state.pendingStopsSelected = [];
        this.state.pendingStopsComment = '';
        this.closeStreamPicker();
    }

    editPendingStop(item) {
        this.state.pendingStopsModal = false;
        this.openCommentModalForEvent(item);
    }

    get selectedPendingStopEntries() {
        const selected = new Set(this.state.pendingStopsSelected);
        return this.pendingOperatorStops
            .filter((i) => selected.has(this.pendingStopKey(i)))
            .map((i) => ({ model: i.model, id: i.id }));
    }

    async applyPendingStops(event) {
        const entries = this.selectedPendingStopEntries;
        if (!entries.length) {
            this.notification.add(_t("Select at least one stop."), { type: "warning" });
            return;
        }
        const typed = (this.state.pendingStopsComment || '').trim();
        const comment = [typed, event ? this.manualStopSuffix(event) : ''].filter((segment) => segment).join(' ');
        if (!event && !comment) {
            this.notification.add(_t("Provide a comment or pick a stop reason."), { type: "warning" });
            return;
        }
        this.state.pendingStopsSaving = true;
        try {
            const res = await meshRpc("/mes/kiosk/pending_stops_bulk_update", {
                workcenter_id: this.state.workcenterId,
                entries: entries,
                new_event_id: event ? event.id : null,
                comment: comment || null,
                viewer: this.viewerMode,
            });
            if (res.ok) {
                this.notification.add(
                    _t("Updated: ") + res.updated, { type: "success" });
                this.closePendingStopsModal();
                this.forcePendingRefresh = true;
                await this.refresh();
            } else {
                this.notification.add(res.error || _t("Update failed."), { type: "warning" });
            }
        } catch (err) {
            this.notification.add(_t("Error: ") + err.message, { type: "danger" });
        }
        this.state.pendingStopsSaving = false;
    }

    async applyPendingStopsCommentOnly() {
        await this.applyPendingStops(null);
    }

    openCommentModalForEvent(event) {
        if (!event || !event.id || !event.model) return;
        if (!this.requireCommentRights()) return;
        this.state.commentModal = {
            id: event.id,
            model: event.model,
            name: event.name || '',
            isManualStop: !!event.is_manual_stop,
        };
        this.state.commentDraft = event.comment || '';
        this.closeStreamPicker();
    }

    openCommentModalForCurrent() {
        const shown = this.displayedStatus;
        if (!shown) return;
        if (!this.requireCommentRights()) return;
        if (shown.model) {
            this.openCommentModalForEvent(shown);
            return;
        }
        this.state.commentModal = {
            model: null,
            id: null,
            queuedFor: shown.state_since,
            name: shown.name || '',
            isManualStop: true,
        };
        this.state.commentDraft = this.state.queuedComment?.comment || '';
        this.closeStreamPicker();
    }

    _dropStaleLiveStatus() {
        const live = this.state.liveStatus;
        if (!live) return;
        const fsmEvent = this.state.data?.workcenter?.current_event;
        if (fsmEvent && this._savedEventIsCurrent(fsmEvent, live)) {
            this.state.liveStatus = null;
        }
    }

    closeCommentModal() {
        this.state.commentModal = null;
        this.state.commentDraft = '';
        this.closeStreamPicker();
    }

    get streamLayout() {
        return this.state.data?.workcenter?.stream_layout || null;
    }

    get availableStreams() {
        return this.streamLayout?.items || [];
    }

    get streamCanvasSize() {
        return computeStreamCanvasSize(this.streamLayout);
    }

    closeStreamPicker() {
        this.state.streamPicker = null;
        this.state.streamEvents = [];
        this.state.streamEventsLoading = false;
        this.state.streamParts = [];
        this.state.streamPartId = null;
        this.state.streamPieceIds = [];
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

    get activeStreamPart() {
        return this.state.streamParts.find((p) => p.id === this.state.streamPartId) || null;
    }

    get selectedPieceNames() {
        const part = this.activeStreamPart;
        if (!part) return [];
        return part.pieces
            .filter((piece) => this.state.streamPieceIds.includes(piece.id))
            .map((piece) => piece.name);
    }

    get filteredStreamEvents() {
        const partId = this.state.streamPartId;
        const pieceIds = this.state.streamPieceIds;
        return this.state.streamEvents.filter((ev) => {
            if (partId && ev.part_id && ev.part_id !== partId) return false;
            if (pieceIds.length && ev.piece_id && !pieceIds.includes(ev.piece_id)) return false;
            return true;
        });
    }

    async toggleStreamPicker(stream) {
        if (this.state.streamPicker?.stream_id === stream.stream_id) {
            this.closeStreamPicker();
            return;
        }
        this.state.streamPicker = stream;
        this.state.streamEvents = [];
        this.state.streamParts = [];
        this.state.streamPartId = null;
        this.state.streamPieceIds = [];
        this.state.streamEventsLoading = true;
        try {
            const res = await meshRpc("/mes/kiosk/stream_manual_stop_events", {
                workcenter_id: this.state.workcenterId,
                stream_id: stream.stream_id,
            });
            if (res.ok) {
                this.state.streamEvents = res.events || [];
                this.state.streamParts = res.parts || [];
            } else {
                this.notification.add(res.error || _t("Cannot load stream events."), { type: "warning" });
                this.closeStreamPicker();
            }
        } catch (err) {
            this.notification.add(_t("Error: ") + err.message, { type: "danger" });
            this.closeStreamPicker();
        }
        this.state.streamEventsLoading = false;
    }

    async queueStreamEvent(event) {
        this.state.commentSaving = true;
        try {
            const params = {
                workcenter_id: this.state.workcenterId,
                event_time: this.state.commentModal.queuedFor,
                new_event_id: event.id,
                viewer: this.viewerMode,
            };
            const suffix = this.manualStopSuffix(event);
            if (suffix) {
                this.appendTextToComment(suffix);
                params.comment = this.state.commentDraft;
            }
            const res = await meshRpc("/mes/kiosk/queue_comment", params);
            if (res.ok) {
                this.state.queuedComment = res.cleared ? null : {
                    event_time: this.state.commentModal.queuedFor,
                    comment: res.comment,
                    new_event_id: res.new_event_id,
                    new_event_name: res.new_event_name,
                };
                this.notification.add(_t("Queued: ") + event.name, { type: "success" });
                this.closeCommentModal();
            } else {
                this.notification.add(res.error || _t("Save failed"), { type: "danger" });
            }
        } catch (err) {
            this.notification.add(_t("Error: ") + err.message, { type: "danger" });
        }
        this.state.commentSaving = false;
    }

    async selectStreamEvent(event) {
        if (this.state.pendingStopsModal) {
            await this.applyPendingStops(event);
            return;
        }
        if (!this.state.commentModal) return;
        if (this.state.commentModal.queuedFor) {
            await this.queueStreamEvent(event);
            return;
        }
        const pieces = this.selectedPieceNames;
        if (this.state.commentModal.isManualStop) {
            const suffix = this.manualStopSuffix(event);
            if (suffix) {
                this.appendTextToComment(suffix);
            }
            await this.replaceStatusWithEvent(event);
        } else {
            const partName = this.activeStreamPart?.name;
            this.state.commentDraft = [event.name, partName, pieces.join(', ')]
                .filter((segment) => segment)
                .join(' / ');
            await this.saveComment();
        }
        this.closeCommentModal();
    }

    manualStopSuffix(event) {
        const pieces = this.selectedPieceNames.join(', ');
        if (event.part_id) {
            return pieces;
        }
        const stream = this.state.streamPicker;
        const part = this.activeStreamPart;
        let place = '';
        if (part) {
            place = !stream || part.stream_id === stream.stream_id ? part.name : `${stream.name}/${part.name}`;
        } else if (stream && event.stream_id !== stream.stream_id) {
            place = stream.name;
        }
        return [place, pieces].filter((segment) => segment).join(' ');
    }

    appendTextToComment(text) {
        const draft = (this.state.commentDraft || '').trim();
        this.state.commentDraft = draft ? `${draft} ${text}` : text;
    }

    appendEventNameToComment(event) {
        const draft = (this.state.commentDraft || '').trim();
        this.state.commentDraft = draft ? `${draft} ${event.name}` : event.name;
    }

    async replaceStatusWithEvent(event) {
        this.state.commentSaving = true;
        try {
            const res = await meshRpc("/mes/kiosk/status_event_replace", {
                workcenter_id: this.state.workcenterId,
                model: this.state.commentModal.model,
                event_id: this.state.commentModal.id,
                new_event_id: event.id,
                comment: this.state.commentDraft,
                viewer: this.viewerMode,
            });
            if (!res.ok) {
                this.notification.add(res.error || _t("Status change failed."), { type: "danger" });
            } else {
                const changedId = this.state.commentModal.id;
                const changedModel = this.state.commentModal.model;
                this.state.timelineItems = this.state.timelineItems.map(it =>
                    (it.id === changedId && it.model === changedModel)
                        ? { ...it, name: res.event_name, comment: res.comment }
                        : it
                );
                this.state.commentModal.name = res.event_name;
                this.state.commentDraft = res.comment;
                this.notification.add(
                    _t("Status changed to %s.", res.event_name),
                    { type: "success" }
                );
                await this.refresh();
            }
        } catch (err) {
            this.notification.add(_t("Error: ") + err.message, { type: "danger" });
        }
        this.state.commentSaving = false;
    }

    async saveComment() {
        if (!this.state.commentModal) return;
        this.state.commentSaving = true;
        if (this.state.commentModal.queuedFor) {
            try {
                const res = await meshRpc("/mes/kiosk/queue_comment", {
                    workcenter_id: this.state.workcenterId,
                    event_time: this.state.commentModal.queuedFor,
                    comment: this.state.commentDraft,
                    viewer: this.viewerMode,
                });
                if (res.ok) {
                    this.state.queuedComment = res.cleared ? null : {
                        event_time: this.state.commentModal.queuedFor,
                        comment: res.comment,
                        new_event_id: res.new_event_id,
                        new_event_name: res.new_event_name,
                    };
                    this.closeCommentModal();
                } else {
                    this.notification.add(res.error || _t("Save failed"), { type: "danger" });
                }
            } catch (err) {
                this.notification.add(_t("Error: ") + err.message, { type: "danger" });
            }
            this.state.commentSaving = false;
            return;
        }
        try {
            const res = await meshRpc("/mes/kiosk/event_comment", {
                workcenter_id: this.state.workcenterId,
                model: this.state.commentModal.model,
                event_id: this.state.commentModal.id,
                comment: this.state.commentDraft,
                viewer: this.viewerMode,
            });
            if (!res.ok) {
                this.notification.add(res.error || _t("Save failed"), { type: "danger" });
            } else {
                const savedId = this.state.commentModal.id;
                const savedModel = this.state.commentModal.model;
                this.state.timelineItems = this.state.timelineItems.map(it =>
                    (it.id === savedId && it.model === savedModel) ? { ...it, comment: res.comment } : it
                );
                this.closeCommentModal();
                this.forcePendingRefresh = true;
                await this.refresh();
            }
        } catch (err) {
            this.notification.add(_t("Error: ") + err.message, { type: "danger" });
        }
        this.state.commentSaving = false;
    }

    setLoginTabPin() { this.state.loginTab = "pin"; this.state.loginError = null; }
    setLoginTabFob() {
        this.state.loginTab = "fob";
        this.state.loginError = null;
        this.setFobOverride(true);
    }

    onPinKeydown(ev) {
        if (ev.key === "Enter") this.submitPin();
    }

    async submitPin() {
        if (!this.state.pinField) {
            this.state.loginError = "PIN is required.";
            return;
        }
        this.state.loginBusy = true;
        const err = await this.onCredentialsLogin({
            pin: this.state.pinField,
            role: this.state.modalContext.targetRole || null,
        });
        this.state.loginBusy = false;
        if (err) this.state.loginError = err;
    }

    get helpRequestSummaries() {
        return this.state.data?.help_request_summaries || [];
    }

    get helpRequestsByPriority() {
        const groups = new Map();
        for (const item of this.helpRequestSummaries) {
            const key = item.priority_label || "Other";
            if (!groups.has(key)) groups.set(key, []);
            groups.get(key).push(item);
        }
        return [...groups.entries()]
            .map(([label, items]) => ({ label, items }))
            .sort((a, b) => (b.items[0]?.priority || "0").localeCompare(a.items[0]?.priority || "0"));
    }

    openCustomChartModal() {
        this.state.customChartModal = true;
    }

    closeCustomChartModal() {
        this.state.customChartModal = false;
    }

    get operatorConfirmationQueue() {
        const all = this.state.data?.help_requests || [];
        return all.filter((task) =>
            task.state === 'help_awaiting_operator' &&
            task.operator_confirmation_state === 'pending' &&
            task.operator_prompt_ready
        );
    }

    get pendingOperatorConfirmation() {
        return this.operatorConfirmationQueue[0] || null;
    }

    get selectedHelpRequest() {
        const id = this.state.selectedHelpRequestId;
        if (!id) return null;
        const all = this.state.data?.help_requests || [];
        return all.find((t) => t.id === id) || null;
    }

    selectHelpRequest(taskId) {
        this.state.selectedHelpRequestId = taskId;
    }

    openHelpListModal() {
        if (!this.helpRequestSummaries.length) return;
        this.state.helpListModal = true;
        if (this.helpRequestSummaries.length === 1) {
            this.state.selectedHelpRequestId = this.helpRequestSummaries[0].id;
        }
    }

    closeHelpListModal() {
        this.state.helpListModal = false;
        this.state.selectedHelpRequestId = null;
    }

    clearHelpSelection() {
        this.state.selectedHelpRequestId = null;
    }

    get plannedDowntimeActive() {
        return Boolean(this.state.data?.workcenter?.planned_downtime_active);
    }


    async togglePlannedDowntime() {
        if (this.viewerMode || this.state.plannedDowntimeBusy) return;
        this.state.plannedDowntimeBusy = true;
        try {
            const res = await meshRpc("/mes/kiosk/planned_downtime/toggle", {
                workcenter_id: this.state.workcenterId,
                session_id: this.downtimeSession?.id || null,
            });
            if (!res.ok) {
                this.notification.add(res.error || _t("Cannot change the planned downtime."),
                                      { type: "danger" });
            } else {
                this.notification.add(
                    res.active ? _t("Planned downtime started.") : _t("Planned downtime finished."),
                    { type: "success" }
                );
                await this.refresh();
            }
        } catch (err) {
            this.notification.add(_t("Error: ") + err.message, { type: "danger" });
        }
        this.state.plannedDowntimeBusy = false;
    }

    closeUnknownTagModal() {
        this.state.unknownTagModal = null;
    }

    async togglePassiveScan() {
        if (this.nfc.passive && this.nfc.supported && !this.nfc.active) {
            const started = await this.startNfc();
            this.notification.add(
                started ? _t("NFC reader activated.") : (this.nfc.status || _t("NFC could not start.")),
                { type: started ? "success" : "warning" }
            );
            return;
        }
        const enabled = !this.nfc.passive;
        this.setPassiveScan(enabled);
        this.notification.add(
            enabled ? _t("Badge scanning activated.") : _t("Badge scanning deactivated."),
            { type: enabled ? "success" : "info" }
        );
    }

    async onFobScan(uid, source, alternatives) {
        if (this.viewerMode) return;
        try {
            const result = await meshRpc("/mes/kiosk/auth/fob", {
                workcenter_id: this.state.workcenterId,
                fob_uid: uid,
                alternatives: alternatives || [],
                source: source || "reader",
            });
            if (!result.ok) {
                if (result.unknown_tag) {
                    this.state.unknownTagModal = {
                        scanned: result.scanned_uid || uid,
                        candidates: result.candidates || [],
                    };
                } else {
                    this.notification.add(result.error || _t("FOB not recognized."), { type: "danger" });
                }
                return;
            }
            if (result.action === "logout") {
                this.notification.add(
                    _t("Goodbye, %s.", result.employee_name),
                    { type: "info" }
                );
            } else {
                this.notification.add(
                    _t("Welcome, %s (%s).", result.employee_name, result.role),
                    { type: "success" }
                );
            }
            if (this.state.modal === "login") {
                this.closeModal();
            }
            await this.refresh();
        } catch (err) {
            this.notification.add(_t("FOB error: ") + err.message, { type: "danger" });
        }
    }

    async onCredentialsLogin(payload) {
        try {
            const result = await meshRpc("/mes/kiosk/auth/login", {
                workcenter_id: this.state.workcenterId,
                login: payload.login,
                pin: payload.pin,
                fob_uid: payload.fob_uid,
                force_role: payload.role || null,
            });
            if (!result.ok) {
                const msg = result.error || _t("Login failed.");
                this.notification.add(msg, { type: "danger" });
                return msg;
            }
            this.notification.add(
                _t("Welcome, %s (%s).", result.employee_name, result.role),
                { type: "success" }
            );
            this.closeModal();
            await this.refresh();
            return null;
        } catch (err) {
            const msg = _t("Login error: ") + err.message;
            this.notification.add(msg, { type: "danger" });
            return msg;
        }
    }

    async logoutRole(role) {
        const session = this.state.data?.sessions?.[role];
        if (!session) return;
        try {
            await meshRpc("/mes/kiosk/auth/logout", { session_id: session.id });
        } catch (_) {}
        await this.refresh();
    }

    openHelpForm() {
        if (this.viewerMode || this.helpButtonDisabled) return;
        this.state.modal = "help_form";
    }

    onHelpCreated() {
        this.closeModal();
        this.refresh();
    }
}

registry.category("actions").add("mes_core.kiosk_action", KioskAction);
