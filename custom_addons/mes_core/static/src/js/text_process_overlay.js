/** @odoo-module **/

export const TEXT_AXIS_ID = "yText";

export const TEXT_THEMES = {
    light: { line: "rgba(3, 105, 161, 0.55)", text: "#075985", background: "rgba(255, 255, 255, 0.9)", point: "#0284c7" },
    dark: { line: "rgba(125, 211, 252, 0.6)", text: "#e0f2fe", background: "rgba(15, 23, 42, 0.85)", point: "#38bdf8" },
};

export function buildTextDataset(textProcess, toSeconds, xMin, xMax, theme) {
    if (!textProcess || !Array.isArray(textProcess.points) || !textProcess.points.length) {
        return null;
    }
    const all = textProcess.points
        .map((point) => ({ x: toSeconds(point.x), y: 1, label: point.label }))
        .filter((point) => !isNaN(point.x));
    const before = all.filter((point) => point.x < xMin);
    const visible = all.filter((point) => point.x >= xMin && point.x <= xMax);
    if (before.length) {
        visible.unshift({ x: xMin, y: 1, label: before[before.length - 1].label, carried: true });
    }
    if (!visible.length) {
        return null;
    }
    const colors = TEXT_THEMES[theme] || TEXT_THEMES.light;
    return {
        label: textProcess.name,
        data: visible,
        xAxisID: "x",
        yAxisID: TEXT_AXIS_ID,
        mesText: true,
        mesColors: colors,
        showLine: false,
        fill: false,
        pointRadius: 5,
        pointHoverRadius: 6,
        pointStyle: "rectRot",
        borderColor: colors.point,
        backgroundColor: colors.point,
        pointBackgroundColor: colors.point,
        order: 0,
    };
}

export function addTextAxis(scales, isV3) {
    if (isV3) {
        scales[TEXT_AXIS_ID] = { type: "linear", min: 0, max: 1.05, display: false };
    } else {
        scales.yAxes.push({ id: TEXT_AXIS_ID, type: "linear", display: false, ticks: { min: 0, max: 1.05 } });
    }
}

export function isTextTooltipItemV3(item) {
    return !item.dataset?.mesText;
}

export function isTextTooltipItemV2(item, data) {
    return !data.datasets[item.datasetIndex]?.mesText;
}

function elementX(element) {
    return element.x !== undefined ? element.x : element._model?.x;
}

export const textLabelsPlugin = {
    id: "mesTextLabels",
    afterDatasetsDraw(chart) {
        const area = chart.chartArea;
        if (!area) {
            return;
        }
        const ctx = chart.ctx;
        chart.data.datasets.forEach((dataset, index) => {
            if (!dataset.mesText) {
                return;
            }
            const meta = chart.getDatasetMeta(index);
            if (meta.hidden || (meta.hidden === null && dataset.hidden)) {
                return;
            }
            const colors = dataset.mesColors || TEXT_THEMES.light;
            ctx.save();
            ctx.font = "11px sans-serif";
            ctx.textBaseline = "top";
            meta.data.forEach((element, position) => {
                const point = dataset.data[position];
                const x = elementX(element);
                if (point === undefined || x === undefined || x < area.left - 1 || x > area.right + 1) {
                    return;
                }
                if (!point.carried) {
                    ctx.strokeStyle = colors.line;
                    ctx.lineWidth = 1;
                    ctx.setLineDash([4, 4]);
                    ctx.beginPath();
                    ctx.moveTo(x, area.top);
                    ctx.lineTo(x, area.bottom);
                    ctx.stroke();
                    ctx.setLineDash([]);
                }
                const text = String(point.label);
                const width = ctx.measureText(text).width;
                const top = area.top + 10 + (position % 3) * 15;
                const left = Math.max(area.left + 2, Math.min(x + 5, area.right - width - 4));
                ctx.fillStyle = colors.background;
                ctx.fillRect(left - 3, top - 2, width + 6, 15);
                ctx.fillStyle = colors.text;
                ctx.fillText(text, left, top);
            });
            ctx.restore();
        });
    },
};
