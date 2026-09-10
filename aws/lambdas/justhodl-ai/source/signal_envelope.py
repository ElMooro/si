"""Validation and taint controls for model-consumable signals.

The envelope is deliberately small and transport-neutral.  Producers must
declare when a fact happened (``event_time``), when it became knowable
(``available_at``), its provenance, and its taint state.  Validation never
silently removes taint: taint discovered anywhere in the payload or provenance
is propagated to the normalized envelope and ``require_clean`` fails closed.
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, Mapping, Optional

SCHEMA_VERSION = "1.0"
MAX_ENVELOPE_BYTES = 256_000
_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/#-]{0,255}$")
_TAINT_MARKERS = frozenset({"_tainted", "tainted", "contains_future_data", "label_leakage"})


class SignalEnvelopeError(ValueError):
    """The signal does not satisfy the envelope contract."""


class TaintedSignalError(SignalEnvelopeError):
    """A tainted signal was offered to a clean-only consumer."""


def parse_timestamp(value: Any, field: str = "timestamp") -> datetime:
    """Parse an RFC3339 value and require an explicit timezone."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            raise SignalEnvelopeError("%s must not be empty" % field)
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError as exc:
            raise SignalEnvelopeError("%s must be RFC3339" % field) from exc
    else:
        raise SignalEnvelopeError("%s must be an RFC3339 string" % field)
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise SignalEnvelopeError("%s must include a timezone" % field)
    return parsed.astimezone(timezone.utc)


def format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _check_json(value: Any, path: str = "payload", depth: int = 0) -> None:
    if depth > 32:
        raise SignalEnvelopeError("%s exceeds maximum nesting depth" % path)
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise SignalEnvelopeError("%s contains a non-finite number" % path)
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _check_json(child, "%s[%d]" % (path, index), depth + 1)
        return
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise SignalEnvelopeError("%s keys must be strings" % path)
            _check_json(child, "%s.%s" % (path, key), depth + 1)
        return
    raise SignalEnvelopeError("%s contains unsupported type %s" % (path, type(value).__name__))


def _payload_taint(value: Any, path: str = "payload") -> Iterable[str]:
    if isinstance(value, Mapping):
        for key, child in value.items():
            child_path = "%s.%s" % (path, key)
            if str(key).lower() in _TAINT_MARKERS and child is True:
                yield child_path
            yield from _payload_taint(child, child_path)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from _payload_taint(child, "%s[%d]" % (path, index))


def _text_id(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not _ID.fullmatch(text):
        raise SignalEnvelopeError("%s is missing or invalid" % field)
    return text


def validate_signal_envelope(envelope: Mapping[str, Any], *, max_bytes: int = MAX_ENVELOPE_BYTES) -> Dict[str, Any]:
    """Validate and return a canonical, detached envelope.

    ``available_at`` may not precede ``event_time``. ``produced_at`` may not
    precede ``available_at``.  Any tainted provenance or embedded taint marker
    makes the result tainted even if the producer claimed it was clean.
    """
    if not isinstance(envelope, Mapping):
        raise SignalEnvelopeError("signal envelope must be an object")
    if envelope.get("schema_version") != SCHEMA_VERSION:
        raise SignalEnvelopeError("schema_version must equal %s" % SCHEMA_VERSION)

    signal_id = _text_id(envelope.get("signal_id"), "signal_id")
    source = _text_id(envelope.get("source"), "source")
    entity_id = _text_id(envelope.get("entity_id"), "entity_id")
    event_time = parse_timestamp(envelope.get("event_time"), "event_time")
    available_at = parse_timestamp(envelope.get("available_at"), "available_at")
    produced_at = parse_timestamp(envelope.get("produced_at", envelope.get("available_at")), "produced_at")
    if available_at < event_time:
        raise SignalEnvelopeError("available_at must not precede event_time")
    if produced_at < available_at:
        raise SignalEnvelopeError("produced_at must not precede available_at")

    payload = envelope.get("payload")
    if not isinstance(payload, Mapping):
        raise SignalEnvelopeError("payload must be an object")
    _check_json(payload)

    provenance = envelope.get("provenance", [])
    if not isinstance(provenance, list):
        raise SignalEnvelopeError("provenance must be a list")
    clean_provenance = []
    discovered_reasons = list(_payload_taint(payload))
    for index, item in enumerate(provenance):
        if not isinstance(item, Mapping):
            raise SignalEnvelopeError("provenance[%d] must be an object" % index)
        source_id = _text_id(item.get("source_id"), "provenance[%d].source_id" % index)
        fingerprint = str(item.get("fingerprint") or "").strip()
        if not re.fullmatch(r"[A-Fa-f0-9]{32,128}", fingerprint):
            raise SignalEnvelopeError("provenance[%d].fingerprint must be a hex digest" % index)
        tainted = item.get("tainted", False)
        if not isinstance(tainted, bool):
            raise SignalEnvelopeError("provenance[%d].tainted must be boolean" % index)
        if tainted:
            discovered_reasons.append("tainted provenance:%s" % source_id)
        clean_provenance.append({
            "source_id": source_id,
            "fingerprint": fingerprint.lower(),
            "tainted": tainted,
        })

    taint = envelope.get("taint", {"status": "CLEAN", "reasons": []})
    if not isinstance(taint, Mapping):
        raise SignalEnvelopeError("taint must be an object")
    status = taint.get("status")
    if status not in ("CLEAN", "TAINTED"):
        raise SignalEnvelopeError("taint.status must be CLEAN or TAINTED")
    reasons = taint.get("reasons", [])
    if not isinstance(reasons, list) or any(not isinstance(reason, str) or not reason.strip() for reason in reasons):
        raise SignalEnvelopeError("taint.reasons must be a list of non-empty strings")
    if status == "TAINTED" and not reasons:
        raise SignalEnvelopeError("tainted envelopes must include at least one reason")
    all_reasons = sorted(set(reason.strip() for reason in reasons) | set(discovered_reasons))
    effective_status = "TAINTED" if status == "TAINTED" or all_reasons else "CLEAN"

    normalized = {
        "schema_version": SCHEMA_VERSION,
        "signal_id": signal_id,
        "source": source,
        "entity_id": entity_id,
        "event_time": format_timestamp(event_time),
        "available_at": format_timestamp(available_at),
        "produced_at": format_timestamp(produced_at),
        "payload": deepcopy(dict(payload)),
        "provenance": clean_provenance,
        "taint": {"status": effective_status, "reasons": all_reasons},
    }
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    if len(encoded) > max_bytes:
        raise SignalEnvelopeError("signal envelope exceeds %d bytes" % max_bytes)
    return normalized


def require_clean(envelope: Mapping[str, Any], *, purpose: str = "model input") -> Dict[str, Any]:
    """Return a validated envelope or reject any effective taint."""
    normalized = validate_signal_envelope(envelope)
    if normalized["taint"]["status"] != "CLEAN":
        raise TaintedSignalError(
            "%s rejects tainted signal %s: %s"
            % (purpose, normalized["signal_id"], "; ".join(normalized["taint"]["reasons"]))
        )
    return normalized


def mark_tainted(envelope: Mapping[str, Any], reason: str) -> Dict[str, Any]:
    """Return a validated copy with an additional immutable taint reason."""
    if not isinstance(reason, str) or not reason.strip():
        raise SignalEnvelopeError("taint reason must be a non-empty string")
    updated = deepcopy(dict(envelope))
    current = dict(updated.get("taint") or {})
    reasons = list(current.get("reasons") or [])
    reasons.append(reason.strip())
    updated["taint"] = {"status": "TAINTED", "reasons": reasons}
    return validate_signal_envelope(updated)


def envelope_fingerprint(envelope: Mapping[str, Any]) -> str:
    """Produce a stable SHA-256 fingerprint of the canonical envelope."""
    normalized = validate_signal_envelope(envelope)
    raw = json.dumps(normalized, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
