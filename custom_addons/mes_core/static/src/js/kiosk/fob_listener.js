/** @odoo-module **/

import { onMounted, onWillUnmount, useState } from "@odoo/owl";
import { isFullyKiosk, startFullyNfc, stopFullyNfc, rearmFullyNfc, fullyHint, getDiagnosticsText } from "./fully_kiosk_nfc";

const TAG_CHAR_REGEX = /^[0-9a-zA-Z]$/;
const KEYSTROKE_GAP_MS = 200;
const SCANNER_MAX_GAP_MS = 60;
const TAG_MIN_LENGTH = 6;
const RESCAN_GUARD_MS = 1500;
const DECIMAL_UID_LENGTH = 10;
const GESTURE_EVENTS = ["pointerdown", "touchstart", "click"];
const NFC_HEALTH_INTERVAL_MS = 20000;

function isEditableTarget(targetDom) {
    if (!targetDom) return false;
    const tagName = (targetDom.tagName || "").toLowerCase();
    return tagName === "input" || tagName === "textarea" || targetDom.isContentEditable;
}

function stripTypedTag(targetDom, typedUid) {
    if (!targetDom || typeof targetDom.value !== "string" || !typedUid) return;
    if (!targetDom.value.endsWith(typedUid)) return;
    targetDom.value = targetDom.value.slice(0, targetDom.value.length - typedUid.length);
    targetDom.dispatchEvent(new Event("input", { bubbles: true }));
}

function hexToPaddedDecimal(hexString) {
    if (!hexString) return "";
    try {
        return BigInt("0x" + hexString).toString().padStart(DECIMAL_UID_LENGTH, "0");
    } catch (_) {
        return "";
    }
}

export function buildUidCandidates(serialNumber) {
    const raw = String(serialNumber || "").trim();
    if (!raw) return [];

    const hexOnly = raw.replace(/[^0-9a-fA-F]/g, "").toUpperCase();
    const bytes = hexOnly.match(/.{2}/g) || [];
    const reversedHex = bytes.slice().reverse().join("");

    const candidates = [
        hexToPaddedDecimal(reversedHex),
        hexToPaddedDecimal(hexOnly),
        hexOnly,
        raw.toUpperCase(),
    ];
    return [...new Set(candidates.filter(Boolean))];
}

export function useFobReader(callback, options = {}) {
    const enabled = options.enabled !== false;
    const hasWebNfc = enabled && "NDEFReader" in window;
    const hasFullyNfc = enabled && isFullyKiosk();

    const nfc = useState({
        supported: hasWebNfc || hasFullyNfc,
        webNfc: hasWebNfc,
        fully: hasFullyNfc,
        secureContext: window.isSecureContext,
        active: false,
        passive: enabled,
        override: false,
        source: "",
        status: "",
        diagnostics: "",
        showDiagnostics: false,
    });

    let kbBuffer = "";
    let lastKeyTimeMs = 0;
    let maxKeystrokeGapMs = 0;
    let lastScan = { uid: "", at: 0 };
    let nfcReader = null;
    let nfcAbort = null;
    let nfcHealthHandle = null;

    const emit = (uid, source, forced, alternatives) => {
        if (!nfc.passive && !nfc.override && !forced) return;

        const nowMs = Date.now();
        if (uid === lastScan.uid && nowMs - lastScan.at < RESCAN_GUARD_MS) return;

        lastScan = { uid, at: nowMs };
        callback(uid, source, alternatives || []);
    };

    const resetKbBuffer = () => {
        kbBuffer = "";
        maxKeystrokeGapMs = 0;
    };

    const onKeyDown = (ev) => {
        const nowMs = Date.now();
        const gapMs = nowMs - lastKeyTimeMs;

        if (gapMs > KEYSTROKE_GAP_MS) {
            resetKbBuffer();
        } else if (kbBuffer) {
            maxKeystrokeGapMs = Math.max(maxKeystrokeGapMs, gapMs);
        }
        lastKeyTimeMs = nowMs;

        if (ev.key === "Enter") {
            const candidateUid = kbBuffer.trim();
            const isScanner = maxKeystrokeGapMs <= SCANNER_MAX_GAP_MS;
            resetKbBuffer();

            if (candidateUid.length < TAG_MIN_LENGTH) return;

            const editable = isEditableTarget(ev.target);
            if (editable && !isScanner) return;

            ev.preventDefault();
            ev.stopPropagation();
            if (editable) {
                stripTypedTag(ev.target, candidateUid);
            }
            emit(candidateUid, "reader");
            return;
        }

        if (TAG_CHAR_REGEX.test(ev.key)) {
            kbBuffer += ev.key;
        } else {
            resetKbBuffer();
        }
    };

    const onTagSerial = (serialNumber, sourceName) => {
        const candidates = buildUidCandidates(serialNumber);
        if (!candidates.length) {
            nfc.status = "Tag delivered no serial number";
            return;
        }
        nfc.status = "";
        emit(candidates[0], sourceName, false, candidates);
    };

    const startFullyReader = () => {
        const result = startFullyNfc((serialNumber) => onTagSerial(serialNumber, "fully_nfc"));
        nfc.active = result.started;
        nfc.source = result.started ? "fully" : "";
        nfc.status = result.status;
        return result.started;
    };

    const startNfc = async () => {
        if (!nfc.supported) return false;
        if (nfc.active || (!nfc.passive && !nfc.override)) return false;

        if (nfc.fully) {
            return startFullyReader();
        }
        if (!nfc.webNfc || !nfc.secureContext) {
            const hint = fullyHint();
            nfc.status = hint || "No NFC reader is available in this browser.";
            return false;
        }

        try {
            nfcReader = new window.NDEFReader();
            nfcAbort = new AbortController();
            await nfcReader.scan({ signal: nfcAbort.signal });

            nfcReader.onreading = (event) => onTagSerial(event.serialNumber, "nfc");

            nfcReader.onreadingerror = () => {
                nfc.status = "Tag could not be read (unsupported tag type)";
            };

            nfc.active = true;
            nfc.source = "webnfc";
            nfc.status = "";
            return true;
        } catch (error) {
            nfc.active = false;
            nfc.source = "";
            const hint = fullyHint();
            const webNfcReason = error && error.name === "NotAllowedError"
                ? "Web NFC permission was refused by the browser."
                : "Web NFC could not start.";
            nfc.status = hint ? hint + " " + webNfcReason : webNfcReason;
            return false;
        }
    };

    const stopNfc = () => {
        if (nfc.fully) {
            stopFullyNfc();
        }
        if (nfcAbort) {
            nfcAbort.abort();
            nfcAbort = null;
        }
        nfcReader = null;
        nfc.active = false;
        nfc.source = "";
    };

    const restartNfc = async () => {
        stopNfc();
        return startNfc();
    };

    const attemptStartOnGesture = () => {
        if (!nfc.supported) return;
        if (nfc.active) {
            if (nfc.fully) {
                rearmFullyNfc("gesture");
            }
            return;
        }
        startNfc();
    };

    const startWhenVisible = () => {
        if (document.visibilityState !== "visible") return;
        if (nfc.fully) {
            if (nfc.active) {
                rearmFullyNfc("visibility");
                return;
            }
            restartNfc();
            return;
        }
        if (nfc.active) {
            restartNfc();
            return;
        }
        startNfc();
    };

    const nfcHealthCheck = () => {
        if (!nfc.fully || !nfc.active) return;
        if (document.visibilityState !== "visible") return;
        rearmFullyNfc("watchdog");
    };

    onMounted(() => {
        if (!enabled) return;
        window.addEventListener("keydown", onKeyDown, true);
        GESTURE_EVENTS.forEach((name) =>
            window.addEventListener(name, attemptStartOnGesture, true));
        document.addEventListener("visibilitychange", startWhenVisible, true);
        window.addEventListener("focus", startWhenVisible, true);
        window.addEventListener("pageshow", startWhenVisible, true);
        nfcHealthHandle = window.setInterval(nfcHealthCheck, NFC_HEALTH_INTERVAL_MS);
        startNfc();
    });

    onWillUnmount(() => {
        if (!enabled) return;
        window.removeEventListener("keydown", onKeyDown, true);
        GESTURE_EVENTS.forEach((name) =>
            window.removeEventListener(name, attemptStartOnGesture, true));
        document.removeEventListener("visibilitychange", startWhenVisible, true);
        window.removeEventListener("focus", startWhenVisible, true);
        window.removeEventListener("pageshow", startWhenVisible, true);
        if (nfcHealthHandle) {
            window.clearInterval(nfcHealthHandle);
            nfcHealthHandle = null;
        }
        stopNfc();
    });

    const setPassive = (isEnabled) => {
        nfc.passive = !!isEnabled;
        if (nfc.passive) {
            startNfc();
        } else {
            stopNfc();
        }
    };

    const setOverride = (isEnabled) => {
        nfc.override = !!isEnabled;
        if (nfc.override) {
            startNfc();
        } else if (!nfc.passive) {
            stopNfc();
        }
    };

    const scanOnce = (uid, source) => emit(uid, source || "manual", true);

    const toggleDiagnostics = () => {
        nfc.showDiagnostics = !nfc.showDiagnostics;
        nfc.diagnostics = nfc.showDiagnostics ? getDiagnosticsText() : "";
    };

    return { nfc, startNfc, setPassive, setOverride, scanOnce, toggleDiagnostics };
}
