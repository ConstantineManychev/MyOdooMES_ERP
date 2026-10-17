/** @odoo-module **/

export function acceptLiveStatus(current, incoming)
{
    if (!incoming || !incoming.state_since)
    {
        return false;
    }
    if (!current)
    {
        return true;
    }
    if (incoming.state_since < current.state_since)
    {
        return false;
    }
    return !(incoming.state_since === current.state_since && current.has_reason && !incoming.has_reason);
}

export function savedStatusIsCurrent(saved, live)
{
    if (!live)
    {
        return true;
    }
    if (!saved || !saved.since)
    {
        return false;
    }
    if (saved.since > live.state_since)
    {
        return true;
    }
    return saved.since === live.state_since && !(live.has_reason && saved.provisional);
}

export function newerSavedStatus(first, second)
{
    if (!first || !first.since)
    {
        return second || null;
    }
    if (!second || !second.since)
    {
        return first;
    }
    return second.since >= first.since ? second : first;
}
