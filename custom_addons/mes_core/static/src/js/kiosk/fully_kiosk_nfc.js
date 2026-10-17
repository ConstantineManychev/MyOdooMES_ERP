/** @odoo-module **/

const NFC_FLAG_NFC_A = 1;
const NFC_FLAG_NFC_B = 2;
const NFC_FLAG_NFC_F = 4;
const NFC_FLAG_NFC_V = 8;
const NFC_FLAG_SKIP_NDEF_CHECK = 128;
const NFC_FLAG_NO_PLATFORM_SOUNDS = 256;

const NFC_READER_FLAGS =
    NFC_FLAG_NFC_A | NFC_FLAG_NFC_B | NFC_FLAG_NFC_F | NFC_FLAG_NFC_V
    | NFC_FLAG_SKIP_NDEF_CHECK | NFC_FLAG_NO_PLATFORM_SOUNDS;
const NFC_DEBOUNCE_MS = 800;

const PRESENCE_TIMEOUT_MS = 2400;
const PRESENCE_POLL_MS = 400;

const TAG_HANDLER_NAME = "mesKioskFullyNfcTag";
const REMOVED_HANDLER_NAME = "mesKioskFullyNfcRemoved";
const DIAGNOSTICS_NAME = "mesKioskNfcDiagnostics";
const DISCOVERY_EVENTS = ["onNfcTagDiscovered", "onNdefDiscovered"];
const REMOVAL_EVENT = "onNfcTagRemoved";
const RESUME_EVENTS = ["screenOn", "onScreensaverStop", "onDaydreamStop"];
const REARM_HANDLER_NAME = "mesKioskFullyNfcRearm";
const MAX_ATTEMPT_LOG = 6;

let activeHandler = null;
let pendingSerial = null;
let presentSerial = null;
let presentSeenAt = 0;
let presenceTimer = null;

const diagnostics = {
    fullyObject: false,
    scanStartFn: false,
    bindFn: false,
    boundEvents: [],
    startAttempts: [],
    started: false,
    lastSerial: "",
    lastSerialAt: "",
    tagEvents: 0,
    suppressedRepeats: 0,
    removalEvents: 0,
    presentSerial: "",
    rearmCount: 0,
    lastRearmAt: "",
    lastRearmReason: "",
};

export function getFullyApi() {
    const api = window.fully;
    if (!api) return null;
    return api;
}

export function isFullyKiosk() {
    const api = getFullyApi();
    return !!(api && typeof api.nfcScanStart === "function");
}

export function fullyHint() {
    const api = getFullyApi();
    if (!api) {
        return "Fully Kiosk JavaScript Interface not detected. Open Fully settings, "
            + "Advanced Web Settings, enable JavaScript Interface (PLUS), then reload the page.";
    }
    if (typeof api.nfcScanStart !== "function") {
        return "Fully Kiosk found but its NFC API is missing. Update Fully Kiosk to version 1.42 or newer.";
    }
    return "";
}

function recordApiShape() {
    const api = getFullyApi();
    diagnostics.fullyObject = !!api;
    diagnostics.scanStartFn = !!(api && typeof api.nfcScanStart === "function");
    diagnostics.bindFn = !!(api && typeof api.bind === "function");
    diagnostics.presentSerial = presentSerial || "";
}

function clearPresence() {
    presentSerial = null;
    presentSeenAt = 0;
    diagnostics.presentSerial = "";
    if (presenceTimer) {
        window.clearInterval(presenceTimer);
        presenceTimer = null;
    }
}

function startPresenceWatchdog() {
    if (presenceTimer) return;
    presenceTimer = window.setInterval(() => {
        if (!presentSerial) {
            clearPresence();
            return;
        }
        if (Date.now() - presentSeenAt > PRESENCE_TIMEOUT_MS) {
            clearPresence();
        }
    }, PRESENCE_POLL_MS);
}

function deliver(serial, kind) {
    diagnostics.tagEvents += 1;
    const now = Date.now();

    if (presentSerial === serial) {
        presentSeenAt = now;
        diagnostics.suppressedRepeats += 1;
        return;
    }

    presentSerial = serial;
    presentSeenAt = now;
    diagnostics.presentSerial = serial;
    startPresenceWatchdog();

    diagnostics.lastSerial = serial;
    diagnostics.lastSerialAt = new Date().toISOString();

    if (typeof activeHandler === "function") {
        activeHandler(serial, kind);
        return;
    }
    pendingSerial = { serial: serial, kind: kind };
}

function release(serial) {
    diagnostics.removalEvents += 1;
    if (!serial || presentSerial === serial) {
        clearPresence();
    }
}

function installGlobalHandlers() {
    if (typeof window[TAG_HANDLER_NAME] !== "function") {
        window[TAG_HANDLER_NAME] = function (serial, kind) {
            deliver(String(serial === undefined ? "" : serial), String(kind || ""));
        };
    }
    if (typeof window[REMOVED_HANDLER_NAME] !== "function") {
        window[REMOVED_HANDLER_NAME] = function (serial) {
            release(String(serial === undefined ? "" : serial));
        };
    }
    if (typeof window[REARM_HANDLER_NAME] !== "function") {
        window[REARM_HANDLER_NAME] = function (reason) {
            rearmFullyNfc(String(reason || "fully_event"));
        };
    }
    if (typeof window[DIAGNOSTICS_NAME] !== "function") {
        window[DIAGNOSTICS_NAME] = function () {
            return getDiagnosticsText();
        };
    }
}

installGlobalHandlers();

function bindEvents(api) {
    diagnostics.boundEvents = [];
    if (typeof api.bind !== "function") return;
    DISCOVERY_EVENTS.forEach((eventName) => {
        const kind = eventName === "onNdefDiscovered" ? "ndef" : "tag";
        try {
            api.bind(eventName, `window.${TAG_HANDLER_NAME}("$serial", "${kind}");`);
            diagnostics.boundEvents.push(eventName);
        } catch (error) {
            diagnostics.boundEvents.push(eventName + ":failed");
        }
    });
    try {
        api.bind(REMOVAL_EVENT, `window.${REMOVED_HANDLER_NAME}("$serial");`);
        diagnostics.boundEvents.push(REMOVAL_EVENT);
    } catch (error) {
        diagnostics.boundEvents.push(REMOVAL_EVENT + ":failed");
    }
    RESUME_EVENTS.forEach((eventName) => {
        try {
            api.bind(eventName, `window.${REARM_HANDLER_NAME}("${eventName}");`);
            diagnostics.boundEvents.push(eventName);
        } catch (error) {
            diagnostics.boundEvents.push(eventName + ":failed");
        }
    });
}

function unbindEvents(api) {
    if (typeof api.bind !== "function") return;
    DISCOVERY_EVENTS.concat([REMOVAL_EVENT]).concat(RESUME_EVENTS).forEach((eventName) => {
        try {
            api.bind(eventName, "");
        } catch (error) {
            return;
        }
    });
}

function tryScanStart(api, label, flags, debounceMs) {
    try {
        const result = flags === undefined
            ? api.nfcScanStart()
            : api.nfcScanStart(flags, debounceMs);
        diagnostics.startAttempts.push(label + "=" + String(result));
        trimAttempts();
        return result !== false;
    } catch (error) {
        diagnostics.startAttempts.push(label + "=threw:" + (error && error.message ? error.message : "error"));
        trimAttempts();
        return false;
    }
}

function trimAttempts() {
    if (diagnostics.startAttempts.length > MAX_ATTEMPT_LOG) {
        diagnostics.startAttempts = diagnostics.startAttempts.slice(-MAX_ATTEMPT_LOG);
    }
}

export function rearmFullyNfc(reason) {
    const api = getFullyApi();
    if (!api || typeof api.nfcScanStart !== "function") return false;
    if (typeof activeHandler !== "function") return false;
    if (presentSerial) return false;

    bindEvents(api);
    let started = tryScanStart(api, "rearm", NFC_READER_FLAGS, NFC_DEBOUNCE_MS);
    if (!started) {
        started = tryScanStart(api, "rearm-plain");
    }
    diagnostics.started = started;
    diagnostics.rearmCount += 1;
    diagnostics.lastRearmAt = new Date().toISOString();
    diagnostics.lastRearmReason = String(reason || "unknown");
    return started;
}

function safeCall(fn) {
    try {
        return String(fn());
    } catch (error) {
        return "n/a";
    }
}

export function getDiagnosticsText() {
    recordApiShape();
    const api = getFullyApi();
    const lines = [
        "window.fully present: " + diagnostics.fullyObject,
        "fully.nfcScanStart: " + diagnostics.scanStartFn,
        "fully.bind: " + diagnostics.bindFn,
        "fully version: " + (api && typeof api.getFullyVersion === "function"
            ? safeCall(() => api.getFullyVersion()) : "n/a"),
        "reader flags: " + NFC_READER_FLAGS,
        "debounce ms: " + NFC_DEBOUNCE_MS,
        "bound events: " + (diagnostics.boundEvents.join(", ") || "none"),
        "scan start attempts: " + (diagnostics.startAttempts.join(", ") || "none"),
        "scan started: " + diagnostics.started,
        "tag events received: " + diagnostics.tagEvents,
        "repeats suppressed: " + diagnostics.suppressedRepeats,
        "removal events: " + diagnostics.removalEvents,
        "tag currently present: " + (diagnostics.presentSerial || "none"),
        "rearm count: " + diagnostics.rearmCount,
        "last rearm: " + (diagnostics.lastRearmAt || "none") + " (" + (diagnostics.lastRearmReason || "none") + ")",
        "last serial: " + (diagnostics.lastSerial || "none"),
        "last serial at: " + (diagnostics.lastSerialAt || "none"),
        "NDEFReader present: " + ("NDEFReader" in window),
        "secure context: " + window.isSecureContext,
        "protocol: " + window.location.protocol,
    ];
    return lines.join("\n");
}

export function startFullyNfc(onTag) {
    installGlobalHandlers();
    recordApiShape();

    const api = getFullyApi();
    if (!api || !diagnostics.scanStartFn) {
        diagnostics.started = false;
        return { started: false, status: fullyHint() };
    }

    activeHandler = onTag;
    diagnostics.startAttempts = [];
    clearPresence();
    bindEvents(api);

    let started = tryScanStart(api, "flags", NFC_READER_FLAGS, NFC_DEBOUNCE_MS);
    if (!started) {
        started = tryScanStart(api, "plain");
    }
    diagnostics.started = started;

    if (!started) {
        activeHandler = null;
        unbindEvents(api);
        return {
            started: false,
            status: "Fully Kiosk did not start the NFC reader. Turn NFC on in Android settings "
                + "and keep the kiosk app in the foreground.",
        };
    }

    if (pendingSerial) {
        const buffered = pendingSerial;
        pendingSerial = null;
        onTag(buffered.serial, buffered.kind);
    }
    return { started: true, status: "" };
}

export function stopFullyNfc() {
    activeHandler = null;
    clearPresence();
    const api = getFullyApi();
    if (!api || typeof api.nfcScanStop !== "function") return;
    try {
        api.nfcScanStop();
    } catch (error) {
        return;
    }
    unbindEvents(api);
    diagnostics.started = false;
}
