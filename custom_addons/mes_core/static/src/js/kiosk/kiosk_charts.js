/** @odoo-module **/

import { Component, useState, onMounted, onWillUnmount, useRef, useEffect } from "@odoo/owl";
import { useService } from "@web/core/utils/hooks";
import { loadJS } from "@web/core/assets";

import { meshRpc } from "./mesh_rpc";
import {
    addTextAxis,
    buildTextDataset,
    isTextTooltipItemV2,
    isTextTooltipItemV3,
    textLabelsPlugin,
} from "../text_process_overlay";

const _t = (() => { try { return require("@web/core/l10n/translation")._t; } catch (_) {} return (str) => str; })();

const KIOSK_CHART_COLORS = {
    primary: '#22d3ee',
    ideal: '#ef4444',
    related: ['#f59e0b', '#a78bfa', '#fb923c', '#34d399', '#f472b6'],
    process: ['#dc3545', '#007bff', '#fd7e14', '#6f42c1', '#20c997'],
};

export class KioskCharts extends Component {
    static template = "mes_core.KioskCharts";
    static props = {
        workcenterId: Number,
        forceCountId: { type: [Number, Boolean], optional: true },
        onTimelineClick: { type: Function, optional: true },
        onGoodPartReset: { type: Function, optional: true },
        customMode: { type: Boolean, optional: true },
    };

    setup() {
        this.notification = useService("notification");
        this.canvasRef = useRef("kioskCanvas");
        this.chartInst = null;
        this.rawMetric = null;
        this.baseEpochMs = 0;
        this.pollHandle = null;

        this.state = useState({
            loading: true,
            error: null,
            availableCounts: [],
            selectedCountId: false,
            selectedCountName: '',
            relatedCounts: [],
            availableProcesses: [],
            selectedProcessId: false,
            availableTextProcesses: [],
            selectedTextProcessId: false,
            visibleTimeline: [],
            statusBlocks: [],
            overlayBlocks: [],
            collapsedGroups: {},
            chartMode: 'total',
            renderTick: 0,
            periodHours: 2,
            bucketMinutes: 5,
            pendingPeriodHours: 2,
            pendingBucketMinutes: 5,
            pendingCountId: false,
            windowStart: '',
            windowEnd: '',
        });

        useEffect(
            () => {
                if (this.canvasRef.el && this.rawMetric && window.Chart) {
                    this.renderChart();
                }
            },
            () => [this.state.renderTick, this.state.chartMode, this.canvasRef.el]
        );

        useEffect(
            () => {
                const target = this.props.forceCountId;
                if (target !== undefined && target !== this.state.selectedCountId) {
                    this.state.selectedCountId = target || false;
                    this.fetchData();
                }
            },
            () => [this.props.forceCountId]
        );

        onMounted(async () => {
            await loadJS("/web/static/lib/Chart/Chart.js");
            await this.fetchData();
            if (!this.props.customMode) {
                this.pollHandle = window.setInterval(() => this.fetchData(), 30000);
            }
        });

        onWillUnmount(() => {
            window.clearInterval(this.pollHandle);
            this.destroyChart();
        });
    }

    parseIsoMs(val) {
        if (!val) return NaN;
        return new Date(String(val).replace(' ', 'T') + 'Z').getTime();
    }

    destroyChart() {
        if (this.chartInst) {
            this.chartInst.destroy();
            this.chartInst = null;
        }
    }

    async fetchData() {
        try {
            const endpoint = this.props.customMode
                ? '/mes/kiosk/custom_chart_data'
                : '/mes/kiosk/chart_data';
            const payload = {
                workcenter_id: this.props.workcenterId,
                selected_count_id: this.state.selectedCountId || false,
                selected_process_id: this.state.selectedProcessId || false,
                selected_text_process_id: this.state.selectedTextProcessId || false,
            };
            if (this.props.customMode) {
                payload.period_hours = this.state.periodHours;
                payload.bucket_minutes = this.state.bucketMinutes;
            }
            const res = await meshRpc(endpoint, payload);
            if (res.error) {
                this.state.error = res.error;
                this.state.loading = false;
                return;
            }
            this.rawMetric = res;
            this.baseEpochMs = this.parseIsoMs(res.shift_start || '1970-01-01T00:00:00');
            if (this.props.customMode) {
                this.state.periodHours = res.period_hours || this.state.periodHours;
                this.state.bucketMinutes = res.bucket_minutes || this.state.bucketMinutes;
                this.state.windowStart = res.window_start || '';
                this.state.windowEnd = res.window_end || '';
            }
            this.state.availableCounts = res.available_counts || [];
            this.state.selectedCountId = res.selected_count_id || false;
            this.state.selectedCountName = res.selected_count_name || '';
            this.state.relatedCounts = res.related_counts || [];
            this.state.availableProcesses = res.available_processes || [];
            this.state.availableTextProcesses = res.available_text_processes || [];
            this.state.selectedTextProcessId = res.selected_text_process_id || false;
            this.state.error = null;
            this.state.loading = false;
            this.buildTimeline();
            this.state.renderTick += 1;
        } catch (err) {
            this.state.error = _t("Chart data unavailable.");
            this.state.loading = false;
        }
    }

    setPendingPeriod(hours) {
        this.state.pendingPeriodHours = Math.min(24, Math.max(2, Number(hours) || 2));
    }

    setPendingBucket(minutes) {
        this.state.pendingBucketMinutes = Math.min(120, Math.max(5, Number(minutes) || 5));
    }

    setPendingCount(countId) {
        this.state.pendingCountId = countId ? Number(countId) : false;
    }

    async showCustomChart() {
        this.state.periodHours = this.state.pendingPeriodHours;
        this.state.bucketMinutes = this.state.pendingBucketMinutes;
        this.state.selectedCountId = this.state.pendingCountId;
        this.state.loading = true;
        await this.fetchData();
    }

    buildTimeline() {
        if (!this.rawMetric?.timeline) return;
        const durSec = this.rawMetric.chart_duration_sec || 28800;
        this.state.visibleTimeline = this.rawMetric.timeline.map(blk => {
            const bStart = (this.parseIsoMs(blk.start) - this.baseEpochMs) / 1000;
            const bEnd = (this.parseIsoMs(blk.end) - this.baseEpochMs) / 1000;
            return {
                ...blk,
                leftPct: Math.max(0, (bStart / durSec) * 100),
                widthPct: Math.max(0, ((bEnd - bStart) / durSec) * 100),
                durationMin: Math.round(blk.duration / 60),
            };
        }).filter(b => b.widthPct > 0);

        this.state.statusBlocks = this.state.visibleTimeline.filter(b => !b.overlay);
        this.state.overlayBlocks = this.state.visibleTimeline.filter(b => b.overlay);
    }

    get timelineGroups() {
        const overlays = (this.state.overlayBlocks || []).slice()
            .sort((a, b) => this.parseIsoMs(a.start) - this.parseIsoMs(b.start));
        const statuses = this.state.statusBlocks || [];
        const grouped = new Set();

        const groups = overlays.map((overlay) => {
            const from = this.parseIsoMs(overlay.start);
            const to = this.parseIsoMs(overlay.end);
            const children = statuses.filter((blk) => {
                const mid = (this.parseIsoMs(blk.start) + this.parseIsoMs(blk.end)) / 2;
                return mid >= from && mid <= to;
            });
            children.forEach((child) => grouped.add(child));
            return {
                key: `${overlay.overlay}-${overlay.id}`,
                label: overlay.name,
                kind: overlay.overlay,
                color: overlay.color,
                start: overlay.start,
                end: overlay.end,
                durationMin: overlay.durationMin,
                children,
            };
        }).filter((group) => group.children.length || group.kind);

        const ungrouped = statuses.filter((blk) => !grouped.has(blk));
        return { groups, ungrouped };
    }

    toggleTimelineGroup(key) {
        const collapsed = { ...this.state.collapsedGroups };
        collapsed[key] = !collapsed[key];
        this.state.collapsedGroups = collapsed;
    }

    isGroupCollapsed(key) {
        return Boolean(this.state.collapsedGroups[key]);
    }

    _buildRunningBlocksSec() {
        const raw = [];
        for (const blk of (this.rawMetric?.timeline || [])) {
            if (blk.name === 'Running' || blk.color === '#28a745') {
                raw.push({
                    start: (this.parseIsoMs(blk.start) - this.baseEpochMs) / 1000,
                    end: (this.parseIsoMs(blk.end) - this.baseEpochMs) / 1000,
                });
            }
        }
        raw.sort((a, b) => a.start - b.start);
        const merged = [];
        for (const rb of raw) {
            const last = merged[merged.length - 1];
            if (last && rb.start <= last.end) {
                last.end = Math.max(last.end, rb.end);
            } else {
                merged.push({ start: rb.start, end: rb.end });
            }
        }
        return merged;
    }

    _rateFromBucket(qty, bucketEndSec, stepSec, runningBlocks) {
        const bucketStart = bucketEndSec - stepSec;
        let runSec = 0;
        for (const rb of runningBlocks) {
            const oStart = Math.max(bucketStart, rb.start);
            const oEnd = Math.min(bucketEndSec, rb.end);
            if (oStart < oEnd) runSec += oEnd - oStart;
        }
        if (runSec <= 0) return 0;
        return Math.round((qty * 3600) / runSec);
    }

    renderChart() {
        if (!this.canvasRef.el || !this.rawMetric?.chart) return;
        const chart = this.rawMetric.chart;
        const durSec = this.rawMetric.chart_duration_sec || 28800;
        const stepSec = chart.bucket_sec || 900;
        const isRate = this.state.chartMode === 'rate';
        const prodData = chart.production || [];
        const isV3 = typeof window.Chart.defaults.plugins !== 'undefined';
        const runningBlocks = isRate ? this._buildRunningBlocksSec() : [];
        const hrs = stepSec / 3600;

        const fmtTime = (sec) => {
            if (isNaN(sec)) return '';
            const dt = new Date(this.baseEpochMs + sec * 1000);
            return dt.toISOString().substring(11, 16);
        };

        const outProd = prodData.map((qty, i) => {
            const x = i * stepSec;
            const rawQty = Number(qty) || 0;
            const y = isRate ? this._rateFromBucket(rawQty, x, stepSec, runningBlocks) : rawQty;
            return { x, y };
        });
        const outIdeal = (chart.ideal || []).map((q, i) => {
            const x = i * stepSec;
            const rawQty = Number(q) || 0;
            const y = isRate ? Math.round(rawQty / hrs) : rawQty;
            return { x, y };
        });

        const ds = [{
            label: `${this.state.selectedCountName || 'Production'} ${isRate ? '(Units/h)' : '(Total)'}`,
            data: outProd,
            xAxisID: 'x', yAxisID: 'yCount',
            borderColor: KIOSK_CHART_COLORS.primary,
            backgroundColor: 'rgba(34,211,238,0.12)',
            borderWidth: 2, fill: true, tension: 0.3, pointRadius: 2,
        }];

        if (chart.show_ideal && outIdeal.length) {
            ds.push({
                label: 'Ideal',
                data: outIdeal,
                xAxisID: 'x', yAxisID: 'yCount',
                borderColor: KIOSK_CHART_COLORS.ideal,
                borderWidth: 1, borderDash: [5, 5],
                fill: false, pointRadius: 0,
            });
        }

        let hasProc = false;
        (chart.processes || []).forEach((prc, idx) => {
            if (!prc.data?.length) return;
            const c = KIOSK_CHART_COLORS.process[idx % KIOSK_CHART_COLORS.process.length];
            hasProc = true;
            const pts = prc.data.map(pt => ({
                x: (this.parseIsoMs(pt.x) - this.baseEpochMs) / 1000,
                y: Number(pt.y),
            }));
            ds.push({
                label: prc.name,
                data: pts,
                xAxisID: 'x', yAxisID: 'yProcess',
                borderColor: c, backgroundColor: 'transparent',
                borderWidth: 2, fill: false, stepped: true,
                tension: 0, pointRadius: 2,
            });
        });

        (chart.related_counts || []).forEach((rc, idx) => {
            const pts = (rc.data || []).map((qty, i) => {
                const x = i * stepSec;
                const rawQty = Number(qty) || 0;
                const y = isRate ? this._rateFromBucket(rawQty, x, stepSec, runningBlocks) : rawQty;
                return { x, y };
            });
            if (!pts.some(p => p.y > 0)) return;
            const c = KIOSK_CHART_COLORS.related[idx % KIOSK_CHART_COLORS.related.length];
            ds.push({
                label: rc.name,
                data: pts,
                xAxisID: 'x', yAxisID: 'yCount',
                borderColor: c, backgroundColor: 'transparent',
                borderWidth: 2, borderDash: [3, 3],
                fill: false, tension: 0.2, pointRadius: 2,
            });
        });

        const textDataset = buildTextDataset(
            chart.text_process,
            (iso) => (this.parseIsoMs(iso) - this.baseEpochMs) / 1000,
            0,
            durSec,
            'dark'
        );
        if (textDataset) {
            ds.push(textDataset);
        }

        const axisColor = '#94a3b8';
        const gridColor = 'rgba(148,163,184,0.1)';

        const scales = isV3 ? {
            x: {
                type: 'linear', min: 0, max: durSec,
                ticks: { color: axisColor, stepSize: stepSec, maxRotation: 45, callback: fmtTime },
                grid: { color: gridColor },
            },
            yCount: {
                type: 'linear', position: 'left', beginAtZero: true,
                ticks: { color: axisColor },
                grid: { color: gridColor },
                title: { display: true, text: isRate ? 'Units/h' : 'pcs', color: axisColor },
            },
            yProcess: {
                type: 'linear', position: 'right', beginAtZero: false,
                display: hasProc, grid: { drawOnChartArea: false },
                ticks: { color: axisColor },
            },
        } : {
            xAxes: [{ id: 'x', type: 'linear', ticks: { min: 0, max: durSec, stepSize: stepSec, callback: fmtTime } }],
            yAxes: [
                { id: 'yCount', type: 'linear', position: 'left', ticks: { beginAtZero: true } },
                { id: 'yProcess', type: 'linear', position: 'right', display: hasProc, gridLines: { drawOnChartArea: false } },
            ],
        };

        if (textDataset) {
            addTextAxis(scales, isV3);
        }

        this.destroyChart();
        const ctx = this.canvasRef.el.getContext('2d');
        const syncTimelineAlign = (inst) => {
            const area = inst.chartArea;
            const cvs = inst.canvas || inst.chart?.canvas;
            if (!cvs || !area) return;
            const wrap = cvs.closest('.o_mes_kiosk_chart_block')?.querySelector('.o_mes_kiosk_timeline_bar');
            if (wrap) {
                wrap.style.marginLeft = `${area.left}px`;
                wrap.style.width = `${area.right - area.left}px`;
            }
        };
        this.chartInst = new window.Chart(ctx, {
            type: 'line',
            data: { datasets: ds },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { duration: 0, onComplete: ({ chart }) => syncTimelineAlign(chart) },
                onResize: syncTimelineAlign,
                scales,
                plugins: isV3 ? {
                    legend: { labels: { color: axisColor, boxWidth: 14, padding: 12 } },
                    tooltip: { mode: 'index', intersect: false, filter: isTextTooltipItemV3, callbacks: { title: (i) => i.length ? fmtTime(i[0].parsed.x) : '' } },
                } : {},
                tooltips: isV3 ? {} : { mode: 'index', intersect: false, filter: isTextTooltipItemV2 },
            },
            plugins: [textLabelsPlugin],
        });
    }

    async onCountChange(ev) {
        this.state.selectedCountId = parseInt(ev.target.value) || false;
        await this.fetchData();
    }

    async onProcessChange(ev) {
        this.state.selectedProcessId = parseInt(ev.target.value) || false;
        await this.fetchData();
    }

    async onTextProcessChange(ev) {
        this.state.selectedTextProcessId = parseInt(ev.target.value) || false;
        await this.fetchData();
    }

    setChartMode(mode) {
        this.state.chartMode = mode;
    }

    resetToGoodPart() {
        this.state.selectedCountId = false;
        if (this.props.onGoodPartReset) {
            this.props.onGoodPartReset();
        }
        this.fetchData();
    }
}
