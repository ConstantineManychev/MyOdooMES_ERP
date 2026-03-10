/** @odoo-module **/

import { registry } from "@web/core/registry";
import { useService } from "@web/core/utils/hooks";
import { loadJS } from "@web/core/assets";
import { Component, onMounted, onWillUnmount, useState, useRef } from "@odoo/owl";

export class MachineLiveCharts extends Component {
    setup() {
        this.orm = useService("orm");
        this.canvasRef = useRef("chartCanvas");
        this.chartInstance = null;
        this.refreshInterval = null;
        this.rawData = null; 
        
        this.state = useState({
            error: false,
            visibleTimeline: [],
            zoomLevel: 1,  
            panOffset: 0,
            availableCounts: [],
            selectedCountId: false,
            selectedCountName: 'Good Parts'
        });

        onMounted(async () => {
            await loadJS("/web/static/lib/Chart/Chart.js");
            await this.fetchData();
            
            if (this.props.record.resId) {
                const freq = Math.max(this.props.record.data.refresh_frequency || 60, 10);
                this.refreshInterval = setInterval(() => this.fetchData(), freq * 1000);
            }
        });

        onWillUnmount(() => {
            if (this.refreshInterval) clearInterval(this.refreshInterval);
            if (this.chartInstance) this.chartInstance.destroy();
        });
    }

    async fetchData() {
        if (!this.props.record.resId) {
            this.state.error = "Please save the machine to view live charts.";
            return;
        }

        await this.orm.call("mrp.workcenter", "action_force_metrics_update", [[this.props.record.resId]]);
        if (this.props.record.load) {
            await this.props.record.load();
        }

        const result = await this.orm.call(
            "mrp.workcenter", 
            "get_live_chart_data", 
            [this.props.record.resId, this.state.selectedCountId || false]
        );

        if (result.error) {
            this.state.error = result.error;
            return;
        }

        this.state.error = false;
        this.rawData = result;
        this.state.availableCounts = result.available_counts;
        this.state.selectedCountId = result.selected_count_id;
        this.state.selectedCountName = result.selected_count_name;
        
        this.applyZoomAndPan(); 
    }

    async onCountChange(ev) {
        this.state.selectedCountId = parseInt(ev.target.value);
        await this.fetchData();
    }

    applyZoomAndPan() {
        if (!this.rawData) return;
        
        const zl = parseFloat(this.state.zoomLevel);
        const pan = parseFloat(this.state.panOffset);
        const totalSec = this.rawData.chart_duration_sec;
        const bucketSec = this.rawData.chart.bucket_sec;
        const shiftStart = new Date(this.rawData.shift_start).getTime();

        const desiredViewSec = totalSec / zl;
        const maxOffsetSec = totalSec - desiredViewSec;
        const desiredStartSec = maxOffsetSec * (pan / 100);
        const desiredEndSec = desiredStartSec + desiredViewSec;

        let startIdx = Math.floor(desiredStartSec / bucketSec);
        let endIdx = Math.ceil(desiredEndSec / bucketSec);

        startIdx = Math.max(0, startIdx);
        endIdx = Math.min(this.rawData.chart.labels.length - 1, endIdx);

        if (endIdx - startIdx < 1) {
            endIdx = Math.min(this.rawData.chart.labels.length - 1, startIdx + 1);
        }

        const actualStartSec = startIdx * bucketSec;
        const actualEndSec = endIdx * bucketSec;
        const actualViewSec = actualEndSec - actualStartSec;

        this.state.visibleTimeline = [];
        for (const block of this.rawData.timeline) {
            const blockStartSec = (new Date(block.start).getTime() - shiftStart) / 1000;
            const blockEndSec = (new Date(block.end).getTime() - shiftStart) / 1000;

            const clampedStart = Math.max(actualStartSec, blockStartSec);
            const clampedEnd = Math.min(actualEndSec, blockEndSec);

            if (clampedStart < clampedEnd) {
                this.state.visibleTimeline.push({
                    ...block,
                    widthPct: ((clampedEnd - clampedStart) / actualViewSec) * 100,
                    durationMin: Math.round(block.duration / 60)
                });
            }
        }

        const slicedData = {
            labels: this.rawData.chart.labels.slice(startIdx, endIdx + 1),
            production: this.rawData.chart.production.slice(startIdx, endIdx + 1),
            ideal: this.rawData.chart.ideal.slice(startIdx, endIdx + 1),
            show_ideal: this.rawData.chart.show_ideal
        };

        this.updateChart(slicedData);
    }

    onWheelZoom(ev) {
        ev.preventDefault(); 
        const zoomStep = 0.5;
        let newZoom = parseFloat(this.state.zoomLevel);
        
        if (ev.deltaY < 0) {
            newZoom = Math.min(20, newZoom + zoomStep);
        } else {
            newZoom = Math.max(1, newZoom - zoomStep);
        }
        
        this.state.zoomLevel = newZoom;
        this.applyZoomAndPan();
    }

    updateChart(data) {
        if (!this.canvasRef.el) return;

        const idealDataset = {
            label: 'Ideal Capacity',
            data: data.ideal,
            type: 'line',
            borderColor: '#dc3545',
            borderWidth: 2,
            borderDash: [5, 5],
            fill: false,
            pointRadius: 0,
            order: 1
        };
        
        if (this.chartInstance) {
            this.chartInstance.data.labels = data.labels;
            this.chartInstance.data.datasets[0].data = data.production;
            this.chartInstance.data.datasets[0].label = this.state.selectedCountName;
            
            if (data.show_ideal) {
                if (this.chartInstance.data.datasets.length === 1) {
                    this.chartInstance.data.datasets.push(idealDataset);
                } else {
                    this.chartInstance.data.datasets[1].data = data.ideal;
                }
            } else {
                if (this.chartInstance.data.datasets.length > 1) {
                    this.chartInstance.data.datasets.pop();
                }
            }
            
            this.chartInstance.update();
            return;
        }

        const ctx = this.canvasRef.el.getContext("2d");
        
        const alignTimeline = (chart) => {
            const chartArea = chart.chartArea;
            const canvas = chart.canvas || (chart.chart && chart.chart.canvas);
            if (!canvas || !chartArea) return;
            
            const dashboard = canvas.closest('.o_mes_live_dashboard');
            const wrapper = dashboard ? dashboard.querySelector('.mes-timeline-wrapper') : null;
            if (wrapper) {
                wrapper.style.marginLeft = chartArea.left + 'px';
                wrapper.style.width = (chartArea.right - chartArea.left) + 'px';
            }
        };

        const datasets = [{
            label: this.state.selectedCountName,
            data: data.production,
            borderColor: '#28a745',
            backgroundColor: 'rgba(40, 167, 69, 0.15)',
            borderWidth: 2,
            fill: true,
            tension: 0.3,
            pointRadius: 3,
            pointBackgroundColor: '#28a745',
            order: 2
        }];

        if (data.show_ideal) {
            datasets.push(idealDataset);
        }

        this.chartInstance = new window.Chart(ctx, {
            type: 'line',
            data: {
                labels: data.labels,
                datasets: datasets
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                animation: { 
                    duration: 0,
                    onComplete: function() { alignTimeline(this); },
                    onProgress: function() { alignTimeline(this); }
                }, 
                scales: {
                    yAxes: [{ ticks: { beginAtZero: true } }],
                    xAxes: [{ ticks: { maxRotation: 45, minRotation: 45 } }]
                },
                tooltips: { mode: 'index', intersect: false },
                hover: { mode: 'nearest', intersect: true }
            }
        });
    }
}

MachineLiveCharts.template = "mes_core.MachineLiveChartsTmpl";
registry.category("view_widgets").add("machine_live_charts", { component: MachineLiveCharts });