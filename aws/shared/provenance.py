"""aws/shared/provenance.py — F1: universal provenance envelope (ops 4429).

Khalid's founding rule is REAL DATA ONLY. The measured threat: 898 silent-
fabrication sites across 235 engines where a missing source renders as a
confident 0 — indistinguishable, on a page, from a measured zero.

This module makes every published number carry its origin:

    from provenance import wrap, derive, missing, batch_wrap

    wrap(3.42, "hy_oas", unit="%", source="fred", series_id="BAMLH0A0HYM2",
         url="https://api.stlouisfed.org/...", as_of="2026-08-05")
    -> {"value": 3.42, "field": "hy_oas", "unit": "%", "as_of": ...,
        "source": {...}, "confidence": 1.0, "trace_id": "..."}

    missing("hy_oas", reason="FRED returned no observations")
    -> {"value": None, "data_unavailable": True, "reason": ...}

`missing()` is the point: an engine that cannot get a value says so, loudly,
instead of substituting a literal. The frontend renders "data unavailable"
rather than a fake zero.
"""
import hashlib
import json
from datetime import datetime, timezone

SOURCE_KINDS = {
    "fred", "polygon", "sec", "nyfed", "ecb", "cftc", "treasury", "bls",
    "openfigi", "yahoo", "coinmetrics", "imf", "boj", "snb", "bis",
    "llm-anthropic", "llm-openai", "llm-perplexity", "llm-glm",
    "cache", "cache-stale", "computed", "fleet-feed", "manual", "unknown",
}


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _trace(field, source, as_of):
    return hashlib.sha256(
        f"{field}|{source}|{as_of}".encode()).hexdigest()[:12]


def wrap(value, field, unit=None, source="unknown", series_id=None,
         url=None, as_of=None, confidence=1.0, fetched_by=None,
         raw_key=None):
    """Wrap a single measured value with its provenance."""
    if source not in SOURCE_KINDS:
        source = "unknown"
    as_of = as_of or _now()
    return {
        "field": field, "value": value, "unit": unit, "as_of": as_of,
        "confidence": round(float(confidence), 3),
        "source": {"kind": source, "series_id": series_id, "url": url,
                   "fetched_at": _now(), "fetched_by": fetched_by,
                   "raw_snapshot_key": raw_key},
        "trace_id": _trace(field, source, as_of),
    }


def derive(value, field, formula, inputs, unit=None, confidence=None):
    """Wrap a DERIVED value, carrying the provenance of each input so the
    chain stays auditable (e.g. hy_ig_skew = HY OAS - IG OAS)."""
    ins = []
    conf = []
    for i in (inputs or []):
        if isinstance(i, dict) and "source" in i:
            ins.append({"field": i.get("field"), "value": i.get("value"),
                        "source": i.get("source"), "as_of": i.get("as_of")})
            conf.append(float(i.get("confidence") or 1.0))
        else:
            ins.append({"raw": str(i)[:80]})
    c = confidence if confidence is not None else (min(conf) if conf else 1.0)
    return {
        "field": field, "value": value, "unit": unit, "as_of": _now(),
        "confidence": round(float(c), 3),
        "source": {"kind": "computed", "series_id": None, "url": None,
                   "fetched_at": _now(), "fetched_by": "derive"},
        "derivation": {"formula": formula, "inputs": ins},
        "trace_id": _trace(field, "computed", _now()),
    }


def missing(field, reason="source unavailable", unit=None, source=None):
    """Explicitly mark a value as UNAVAILABLE. Use this instead of `or 0`.

    A page reading this renders 'data unavailable'; a zero would have been
    read as a measurement. This is the whole point of the module.
    """
    return {
        "field": field, "value": None, "unit": unit,
        "data_unavailable": True, "reason": str(reason)[:200],
        "as_of": _now(), "confidence": 0.0,
        "source": {"kind": source or "unknown", "series_id": None,
                   "url": None, "fetched_at": _now()},
        "trace_id": _trace(field, "missing", _now()),
    }


def batch_wrap(mapping, source="unknown", unit=None, **kw):
    """Wrap a {field: value} dict; None values become explicit missing()."""
    out = {}
    for k, v in (mapping or {}).items():
        out[k] = (missing(k, "value was None", unit=unit, source=source)
                  if v is None else
                  wrap(v, k, unit=unit, source=source, **kw))
    return out


def unwrap(obj, default=None):
    """Read a value back out of an envelope (or pass a raw value through)."""
    if isinstance(obj, dict) and "value" in obj and "source" in obj:
        return obj.get("value") if not obj.get("data_unavailable") else default
    return obj if not isinstance(obj, dict) else default


def is_envelope(obj):
    return isinstance(obj, dict) and "value" in obj and "source" in obj


def coverage(payload):
    """What fraction of numeric leaves in a payload carry provenance?
    Used by the weekly report (F9) and the rollup (E12)."""
    total = wrapped = 0

    def walk(o, depth=0):
        nonlocal total, wrapped
        if depth > 8:
            return
        if is_envelope(o):
            total += 1
            wrapped += 1
            return
        if isinstance(o, dict):
            for v in o.values():
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    total += 1
                else:
                    walk(v, depth + 1)
        elif isinstance(o, list):
            for v in o[:200]:
                if isinstance(v, (int, float)) and not isinstance(v, bool):
                    total += 1
                else:
                    walk(v, depth + 1)

    walk(payload)
    return {"numeric_leaves": total, "with_provenance": wrapped,
            "coverage_pct": round(100 * wrapped / total, 1) if total else 0.0}
