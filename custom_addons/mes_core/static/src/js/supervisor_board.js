/** @odoo-module **/

import { Component, onMounted, onWillStart, onWillUnmount, useState } from "@odoo/owl";
import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { _t } from "@web/core/l10n/translation";
import { acceptLiveStatus, newerSavedStatus, savedStatusIsCurrent } from "./live_status";

const VIEW_STORAGE_KEY = "mes_supervisor_board_view";
const BUS_REFRESH_DELAY_MS = 1500;
const BUS_EVENTS = new Set([
    "session_changed",
    "help_request_state",
    "help_request_created",
    "machine_state_changed",
]);
const STATUS_CHANNEL = "mes.workcenter.status";
const LIVE_EVENT = "workcenter_status_color";
const SAVED_EVENT = "workcenter_status_saved";
const HEX_COLOR = /^#(?:[0-9a-fA-F]{3}|[0-9a-fA-F]{6})$/;
const ROLE_SHORT = { operator: "OP", supervisor: "SV", technician: "TECH" };
const subscribedChannels = new Set();

function readStoredView()
{
    try
    {
        const stored = window.localStorage.getItem(VIEW_STORAGE_KEY);
        return stored === "list" ? "list" : "tiles";
    }
    catch (error)
    {
        return "tiles";
    }
}

function storeView(view)
{
    try
    {
        window.localStorage.setItem(VIEW_STORAGE_KEY, view);
    }
    catch (error)
    {
        return;
    }
}

function luminance(hex)
{
    let value = hex.slice(1);
    if (value.length === 3)
    {
        value = value.split("").map((char) => char + char).join("");
    }
    const channels = [0, 2, 4].map((offset) =>
    {
        const raw = parseInt(value.slice(offset, offset + 2), 16) / 255;
        return raw <= 0.03928 ? raw / 12.92 : Math.pow((raw + 0.055) / 1.055, 2.4);
    });
    return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
}

export function readableTextColor(background)
{
    if (!background || !HEX_COLOR.test(background))
    {
        return "#212529";
    }
    return luminance(background) > 0.45 ? "#212529" : "#ffffff";
}

export function oeeTone(value)
{
    if (value >= 85)
    {
        return "good";
    }
    if (value >= 67)
    {
        return "fair";
    }
    return "poor";
}

export class MesSupervisorBoard extends Component
{
    static template = "mes_core.SupervisorBoard";
    static props = ["*"];

    setup()
    {
        this.orm = useService("orm");
        this.actionService = useService("action");
        this.notification = useService("notification");
        this.busService = this.env.services.bus_service || null;
        this.refreshTimer = null;
        this.busTimer = null;
        this.loading = false;

        this.state = useState({
            ready: false,
            error: "",
            machines: [],
            canTogglePm: false,
            refreshSec: 30,
            view: readStoredView(),
            search: "",
            pmBusy: {},
            updatedAt: "",
            live: {},
            saved: {},
        });

        this.onBusNotification = ({ detail }) => this.handleNotifications(detail || []);

        onWillStart(() => this.load());

        onMounted(() =>
        {
            this.scheduleRefresh();
            this.listenToBus();
        });

        onWillUnmount(() =>
        {
            window.clearTimeout(this.refreshTimer);
            window.clearTimeout(this.busTimer);
            this.stopListening();
        });
    }

    async load()
    {
        if (this.loading)
        {
            return;
        }
        this.loading = true;
        try
        {
            const board = await this.orm.call("mes.supervisor.board", "get_board", []);
            this.state.machines = board.machines || [];
            this.state.canTogglePm = Boolean(board.can_toggle_pm);
            this.state.refreshSec = board.refresh_sec || 30;
            this.state.error = "";
            this.state.updatedAt = new Date().toLocaleTimeString();
            this.state.saved = {};
            this.forgetCaughtUpLive();
            this.syncChannels();
        }
        catch (error)
        {
            this.state.error = error?.data?.message || error?.message || _t("The dashboard could not be loaded.");
        }
        finally
        {
            this.loading = false;
            this.state.ready = true;
        }
    }

    scheduleRefresh()
    {
        window.clearTimeout(this.refreshTimer);
        this.refreshTimer = window.setTimeout(async () =>
        {
            await this.load();
            this.scheduleRefresh();
        }, this.state.refreshSec * 1000);
    }

    async refreshNow()
    {
        await this.load();
        this.scheduleRefresh();
    }

    listenToBus()
    {
        if (!this.busService)
        {
            return;
        }
        try
        {
            this.busService.addEventListener("notification", this.onBusNotification);
            this.busService.addChannel(STATUS_CHANNEL);
            this.syncChannels();
        }
        catch (error)
        {
            this.busService = null;
        }
    }

    stopListening()
    {
        if (!this.busService)
        {
            return;
        }
        try
        {
            this.busService.removeEventListener("notification", this.onBusNotification);
        }
        catch (error)
        {
            this.busService = null;
        }
    }

    syncChannels()
    {
        if (!this.busService)
        {
            return;
        }
        try
        {
            for (const machine of this.state.machines)
            {
                const channel = `mes.kiosk.workcenter.${machine.id}`;
                if (!subscribedChannels.has(channel))
                {
                    this.busService.addChannel(channel);
                    subscribedChannels.add(channel);
                }
            }
        }
        catch (error)
        {
            this.busService = null;
        }
    }

    handleNotifications(notifications)
    {
        let needsReload = false;
        for (const { type, payload } of notifications)
        {
            if (type === LIVE_EVENT)
            {
                this.applyLive(payload);
            }
            else if (type === SAVED_EVENT)
            {
                this.applySaved(payload);
            }
            else if (BUS_EVENTS.has(type))
            {
                needsReload = true;
            }
        }
        if (needsReload)
        {
            window.clearTimeout(this.busTimer);
            this.busTimer = window.setTimeout(() => this.refreshNow(), BUS_REFRESH_DELAY_MS);
        }
    }

    applyLive(payload)
    {
        const machineId = payload?.workcenter_id;
        if (!machineId || !acceptLiveStatus(this.state.live[machineId], payload))
        {
            return;
        }
        this.state.live[machineId] = {
            label: payload.label || "",
            color: payload.color || "",
            state_since: payload.state_since,
            has_reason: Boolean(payload.has_reason),
        };
    }

    applySaved(payload)
    {
        const machineId = payload?.workcenter_id;
        if (!machineId)
        {
            return;
        }
        this.state.saved[machineId] = newerSavedStatus(this.state.saved[machineId], {
            label: payload.label || "",
            color: payload.color || "",
            since: payload.since || "",
            provisional: Boolean(payload.provisional),
        });
    }

    savedStatus(machine)
    {
        const loaded = {
            label: machine.status_label || "",
            color: machine.status_color || "",
            since: machine.status_since || "",
            provisional: Boolean(machine.status_provisional),
        };
        return newerSavedStatus(loaded, this.state.saved[machine.id]) || loaded;
    }

    shownStatus(machine)
    {
        const saved = this.savedStatus(machine);
        const live = this.state.live[machine.id];
        return savedStatusIsCurrent(saved, live) ? saved : live;
    }

    forgetCaughtUpLive()
    {
        for (const machine of this.state.machines)
        {
            if (this.state.live[machine.id] && savedStatusIsCurrent(this.savedStatus(machine), this.state.live[machine.id]))
            {
                delete this.state.live[machine.id];
            }
        }
    }

    get visibleMachines()
    {
        const term = this.state.search.trim().toLowerCase();
        if (!term)
        {
            return this.state.machines;
        }
        return this.state.machines.filter((machine) => machine.name.toLowerCase().includes(term));
    }

    get summary()
    {
        const machines = this.state.machines;
        return {
            total: machines.length,
            help: machines.filter((machine) => machine.help).length,
            pm: machines.filter((machine) => machine.pm_active).length,
        };
    }

    setView(view)
    {
        this.state.view = view;
        storeView(view);
    }

    onSearch(ev)
    {
        this.state.search = ev.target.value || "";
    }

    statusStyle(machine)
    {
        const color = this.shownStatus(machine).color;
        if (!color)
        {
            return "";
        }
        return `background:${color};color:${readableTextColor(color)};`;
    }

    tileStyle(machine)
    {
        const color = this.shownStatus(machine).color;
        return color ? `--mes-board-status:${color};` : "";
    }

    statusLabel(machine)
    {
        return this.shownStatus(machine).label;
    }

    oeeClass(machine)
    {
        return `o_mes_board_oee_${oeeTone(machine.oee || 0)}`;
    }

    helpClass(help)
    {
        return `o_mes_board_hr_${help.tone}` + (help.blink ? " o_mes_board_hr_blink" : "");
    }

    roleShort(session)
    {
        return ROLE_SHORT[session.role] || session.role_label;
    }

    formatPercent(value)
    {
        return `${(Math.round((value || 0) * 10) / 10).toLocaleString()}%`;
    }

    formatCount(value)
    {
        return Math.round(value || 0).toLocaleString();
    }

    displayText(value)
    {
        return value && value !== "None" ? value : "—";
    }

    pmTitle(machine)
    {
        if (machine.pm_active)
        {
            return machine.pm_title || _t("Planned downtime is on. Switch off to end it now.");
        }
        return _t("Switch on to start planned downtime now.");
    }

    pmDisabled(machine)
    {
        return !this.state.canTogglePm || Boolean(this.state.pmBusy[machine.id]);
    }

    async togglePm(machine, ev)
    {
        const wanted = Boolean(ev.target.checked);
        if (this.pmDisabled(machine))
        {
            ev.target.checked = machine.pm_active;
            return;
        }
        this.state.pmBusy[machine.id] = true;
        try
        {
            const result = await this.orm.call("mes.supervisor.board", "set_pm", [machine.id, wanted]);
            if (!result.ok)
            {
                ev.target.checked = machine.pm_active;
                this.notification.add(result.error || _t("Planned downtime could not be changed."), { type: "danger" });
                return;
            }
            machine.pm_active = Boolean(result.active);
            this.notification.add(
                machine.pm_active
                    ? _t("Planned downtime started on %s.", machine.name)
                    : _t("Planned downtime finished on %s.", machine.name),
                { type: "success" }
            );
        }
        catch (error)
        {
            ev.target.checked = machine.pm_active;
            throw error;
        }
        finally
        {
            delete this.state.pmBusy[machine.id];
        }
        await this.refreshNow();
    }

    openKiosk(machine)
    {
        this.actionService.doAction({
            type: "ir.actions.client",
            tag: "mes_core.kiosk_action",
            name: machine.name,
            target: "fullscreen",
            params: { workcenter_id: machine.id, viewer: 1 },
        });
    }

    openTask(help)
    {
        this.actionService.doAction({
            type: "ir.actions.act_window",
            res_model: "mes.task",
            res_id: help.open_id,
            views: [[false, "form"]],
            target: "current",
        });
    }

    async openLosses(machine, kind)
    {
        await this.actionService.doActionButton({
            type: "object",
            name: kind === "waste" ? "action_open_waste_losses" : "action_open_downtime_losses",
            resModel: "mrp.workcenter",
            resId: machine.id,
            context: {},
        });
    }
}

registry.category("actions").add("mes_core.supervisor_board", MesSupervisorBoard);
