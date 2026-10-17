/** @odoo-module **/

export function findTemplate(options, templateId) {
    return (options?.templates || []).find((template) => template.id === templateId) || null;
}

export function matchVariant(template, valueIds) {
    if (!template) {
        return null;
    }
    const wanted = [...valueIds].sort((a, b) => a - b).join(",");
    return template.variants.find(
        (variant) => [...variant.value_ids].sort((a, b) => a - b).join(",") === wanted
    ) || null;
}

export function replaceLineValue(template, valueIds, lineId, valueId) {
    const line = template.lines.find((item) => item.id === lineId);
    if (!line) {
        return valueIds;
    }
    const lineValueIds = new Set(line.values.map((value) => value.id));
    return [...valueIds.filter((id) => !lineValueIds.has(id)), valueId];
}

export function activeSessions(sessions) {
    return ["operator", "supervisor", "technician"]
        .map((role) => sessions?.[role])
        .filter((session) => session);
}
