/** @odoo-module **/

import { Component, onMounted, onPatched, onWillStart, onWillUnmount, useRef, useState } from "@odoo/owl";
import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { loadJS } from "@web/core/assets";
import { _t } from "@web/core/l10n/translation";

const REFRESH_INTERVAL_MS = 60000;
const SERIES_COLORS = [
    "#0d6efd", "#dc3545", "#198754", "#fd7e14", "#6f42c1",
    "#20c997", "#e83e8c", "#0dcaf0", "#ffc107", "#6c757d",
];

function roundValue(value)
{
    return Math.round((Number(value) || 0) * 100) / 100;
}

export class MesCountDashboard extends Component
{
    static template = "mes_core.CountDashboard";
    static props = ["*"];

    setup()
    {
        this.orm = useService("orm");
        this.canvasRef = useRef("dashboardCanvas");
        this.chartInstance = null;
        this.chartReady = false;

        this.state = useState({
            loading: true,
            error: false,
            emptyMessage: "",
            counts: [],
            selectableCounts: [],
            selectedCountIds: [],
            settingsOpen: false,
            rows: [],
            buckets: [],
            columnTotals: [],
            grandTotal: 0,
            activeCountId: false,
            activeCountName: "",
            periodMode: "shift",
            effectiveMode: "shift",
            periodHours: 8,
            intervalMinutes: 60,
            intervalOptions: [],
            periodHoursRange: [],
            viewMode: "table",
            windowStart: "",
            windowEnd: "",
            lastUpdate: "",
        });

        onWillStart(async () =>
        {
            await this.loadChartLibrary();
            await this.loadConfig();
        });

        onMounted(() =>
        {
            this.refreshHandle = window.setInterval(() => this.refreshTail(), REFRESH_INTERVAL_MS);
            this.syncChart();
        });

        onPatched(() =>
        {
            this.syncChart();
        });

        onWillUnmount(() =>
        {
            window.clearInterval(this.refreshHandle);
            this.destroyChart();
        });
    }

    async loadChartLibrary()
    {
        try
        {
            await loadJS("/web/static/lib/Chart/Chart.js");
            this.chartReady = typeof window.Chart !== "undefined";
        }
        catch (error)
        {
            this.chartReady = false;
        }
    }

    async loadConfig()
    {
        const config = await this.orm.call("mes.count.dashboard", "get_dashboard_config", []);
        this.state.counts = config.counts || [];
        this.state.selectableCounts = config.selectable_counts || [];
        this.state.selectedCountIds = config.selected_count_ids || [];
        this.state.intervalOptions = config.interval_options || [];
        this.state.periodHoursRange = config.period_hours_range || [];
        this.state.activeCountId = config.active_count_id || false;
        this.state.periodMode = config.period_mode || "shift";
        this.state.periodHours = config.period_hours || 8;
        this.state.intervalMinutes = config.interval_minutes || 60;
        this.state.viewMode = config.view_mode || "table";
        await this.loadData();
    }

    applyPayload(result)
    {
        this.state.error = false;
        this.state.emptyMessage = result.empty ? (result.message || _t("Nothing to show")) : "";
        this.state.counts = result.counts || [];
        this.state.activeCountId = result.active_count_id;
        this.state.activeCountName = result.active_count_name || "";
        this.state.buckets = result.buckets || [];
        this.state.rows = result.rows || [];
        this.state.columnTotals = result.column_totals || [];
        this.state.grandTotal = result.grand_total || 0;
        this.state.intervalMinutes = result.interval_minutes || this.state.intervalMinutes;
        this.state.effectiveMode = result.effective_mode || this.state.effectiveMode;
        this.state.windowStart = result.window_start || "";
        this.state.windowEnd = result.window_end || "";
        this.state.lastUpdate = new Date().toLocaleTimeString();
    }

    failPayload(message)
    {
        this.state.error = message;
        this.state.emptyMessage = "";
        this.state.rows = [];
        this.state.buckets = [];
        this.state.columnTotals = [];
        this.state.grandTotal = 0;
    }

    requestParams(extra)
    {
        return Object.assign({
            count_id: this.state.activeCountId,
            period_mode: this.state.periodMode,
            period_hours: this.state.periodHours,
            interval_minutes: this.state.intervalMinutes,
        }, extra || {});
    }

    async loadData()
    {
        this.state.loading = true;
        let result;
        try
        {
            result = await this.orm.call(
                "mes.count.dashboard", "get_dashboard_data", [], this.requestParams());
        }
        catch (error)
        {
            this.state.loading = false;
            this.failPayload(error.message?.data?.message || _t("Counts dashboard data is unavailable"));
            return;
        }
        this.state.loading = false;

        if (!result || result.error)
        {
            this.failPayload((result && result.error) || _t("Counts dashboard data is unavailable"));
            return;
        }
        this.applyPayload(result);
    }

    async refreshTail()
    {
        if (this.state.error || this.state.emptyMessage || !this.state.buckets.length)
        {
            await this.loadData();
            return;
        }

        const lastBucket = this.state.buckets[this.state.buckets.length - 1];
        let result;
        try
        {
            result = await this.orm.call(
                "mes.count.dashboard", "get_dashboard_tail", [],
                this.requestParams({ last_bucket_start: lastBucket.start }));
        }
        catch (error)
        {
            return;
        }

        if (!result || result.error)
        {
            return;
        }
        if (result.empty || result.full)
        {
            this.applyPayload(result);
            return;
        }

        const index = this.state.buckets.length - 1;
        const values = result.values || {};
        for (const row of this.state.rows)
        {
            const fresh = values[String(row.workcenter_id)];
            if (fresh === undefined)
            {
                continue;
            }
            const previous = row.values[index] || 0;
            row.values[index] = fresh;
            row.total = roundValue(row.total - previous + fresh);
        }

        const previousColumn = this.state.columnTotals[index] || 0;
        this.state.columnTotals[index] = result.column_total;
        this.state.grandTotal = roundValue(this.state.grandTotal - previousColumn + result.column_total);
        this.state.windowEnd = result.window_end || this.state.windowEnd;
        this.state.lastUpdate = new Date().toLocaleTimeString();
    }

    async savePrefs(values)
    {
        try
        {
            await this.orm.call("mes.count.dashboard", "save_dashboard_prefs", [], values);
        }
        catch (error)
        {
            return;
        }
    }

    toggleSettings()
    {
        this.state.settingsOpen = !this.state.settingsOpen;
    }

    isCountSelected(countId)
    {
        return this.state.selectedCountIds.includes(countId);
    }

    async toggleCountSelection(countId)
    {
        const selected = this.state.selectedCountIds.filter((value) => value !== countId);
        if (selected.length === this.state.selectedCountIds.length)
        {
            selected.push(countId);
        }
        this.state.selectedCountIds = selected;
        await this.savePrefs({ count_ids: selected });
        await this.loadData();
    }

    async selectCount(countId)
    {
        if (this.state.activeCountId === countId)
        {
            return;
        }
        this.state.activeCountId = countId;
        await this.loadData();
        await this.savePrefs({ count_id: countId });
    }

    async setViewMode(mode)
    {
        if (this.state.viewMode === mode)
        {
            return;
        }
        this.state.viewMode = mode;
        await this.savePrefs({ view_mode: mode });
    }

    async setPeriodMode(mode)
    {
        if (this.state.periodMode === mode)
        {
            return;
        }
        this.state.periodMode = mode;
        await this.loadData();
        await this.savePrefs({ period_mode: mode });
    }

    async onPeriodHoursChange(event)
    {
        const hours = parseInt(event.target.value, 10);
        if (!hours || hours === this.state.periodHours)
        {
            return;
        }
        this.state.periodHours = hours;
        await this.loadData();
        await this.savePrefs({ period_hours: hours });
    }

    async onIntervalChange(event)
    {
        const minutes = parseInt(event.target.value, 10);
        if (!minutes || minutes === this.state.intervalMinutes)
        {
            return;
        }
        this.state.intervalMinutes = minutes;
        await this.loadData();
        await this.savePrefs({ interval_minutes: minutes });
    }

    async onManualRefresh()
    {
        await this.loadData();
    }

    formatValue(value)
    {
        const numeric = Number(value) || 0;
        return Number.isInteger(numeric) ? String(numeric) : numeric.toFixed(2);
    }

    seriesColor(index)
    {
        return SERIES_COLORS[index % SERIES_COLORS.length];
    }

    destroyChart()
    {
        if (this.chartInstance)
        {
            this.chartInstance.destroy();
            this.chartInstance = null;
        }
    }

    syncChart()
    {
        try
        {
            this.buildChart();
        }
        catch (error)
        {
            this.destroyChart();
            this.state.error = _t("Chart could not be rendered: ") + (error && error.message ? error.message : error);
        }
    }

    buildChart()
    {
        if (this.state.viewMode !== "chart" || !this.chartReady || !this.canvasRef.el)
        {
            this.destroyChart();
            return;
        }
        this.destroyChart();

        const labels = this.state.buckets.map((bucket) => bucket.long_label);
        const datasets = this.state.rows
            .filter((row) => row.mapped)
            .map((row, index) =>
            {
                const color = this.seriesColor(index);
                return {
                    label: row.name,
                    data: row.values,
                    borderColor: color,
                    backgroundColor: color,
                    borderWidth: 2,
                    fill: false,
                    tension: 0.25,
                    pointRadius: 2,
                };
            });

        const isV3 = typeof window.Chart.defaults.plugins !== "undefined";
        const axisTitle = this.state.activeCountName || _t("Counts");
        const scales = isV3
            ? {
                x: { ticks: { maxRotation: 60, minRotation: 45, autoSkip: true } },
                y: { beginAtZero: true, title: { display: true, text: axisTitle } },
            }
            : {
                xAxes: [{ ticks: { maxRotation: 60, minRotation: 45, autoSkip: true } }],
                yAxes: [{
                    ticks: { beginAtZero: true },
                    scaleLabel: { display: true, labelString: axisTitle },
                }],
            };

        this.chartInstance = new window.Chart(this.canvasRef.el.getContext("2d"), {
            type: "line",
            data: { labels: labels, datasets: datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 0 },
                scales: scales,
                interaction: isV3 ? { mode: "index", intersect: false } : undefined,
                hover: isV3 ? undefined : { mode: "index", intersect: false },
                tooltips: isV3 ? undefined : { mode: "index", intersect: false },
                legend: isV3 ? undefined : { position: "bottom" },
                plugins: isV3 ? { legend: { position: "bottom" } } : {},
            },
        });
    }
}

registry.category("actions").add("mes_core.count_dashboard", MesCountDashboard);
