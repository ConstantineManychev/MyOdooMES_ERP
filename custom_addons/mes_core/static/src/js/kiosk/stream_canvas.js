/** @odoo-module **/

export function computeStreamCanvasSize(layout) {
    if (!layout) return { width: 0, height: 0 };
    const declaredWidth = Number(layout.canvas_width) || 0;
    const declaredHeight = Number(layout.canvas_height) || 0;
    if (layout.configured) {
        return { width: declaredWidth, height: declaredHeight };
    }
    const items = layout.items || [];
    if (!items.length) return { width: declaredWidth, height: 0 };

    let usedRight = 0;
    let usedBottom = 0;
    for (const item of items) {
        const left = (Number(item.pos_x) || 0) / 100 * declaredWidth;
        const top = (Number(item.pos_y) || 0) / 100 * declaredHeight;
        usedRight = Math.max(usedRight, left + (Number(item.width) || 0));
        usedBottom = Math.max(usedBottom, top + (Number(item.height) || 0));
    }
    return {
        width: Math.min(declaredWidth, Math.ceil(usedRight)) || declaredWidth,
        height: Math.min(declaredHeight, Math.ceil(usedBottom)) || declaredHeight,
    };
}
