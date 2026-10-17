/** @odoo-module **/

let _rpcId = 1;

export async function meshRpc(route, params = {}) {
    const response = await fetch(route, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
            jsonrpc: '2.0',
            method: 'call',
            id: _rpcId++,
            params,
        }),
    });
    if (!response.ok) {
        throw new Error(`HTTP ${response.status} on ${route}`);
    }
    const data = await response.json();
    if (data.error) {
        const msg = data.error.data?.message || data.error.message || JSON.stringify(data.error);
        throw new Error(msg);
    }
    return data.result;
}
