"""Versioned measurement envelopes. Unknown clocks stay unknown.

A structural envelope is not proof that a provider response was archived. The
quality fields distinguish an attributed value from an evidence reference and
from a verified replay. Model/portfolio permission is always separate.
"""
import hashlib
import json
import math
from datetime import datetime, timezone
from evidence_store import public_source_url

CONTRACT = "measurement-provenance.v2"
SOURCE_KINDS = {"fred", "polygon", "sec", "nyfed", "ecb", "cftc", "treasury", "bls", "bea", "ofr", "fmp",
                "openfigi", "yahoo", "coinmetrics", "imf", "boj", "snb", "bis", "worldbank", "warehouse",
                "llm-anthropic", "llm-openai", "llm-perplexity", "llm-glm", "cache", "cache-stale",
                "computed", "fleet-feed", "manual", "unknown"}


def _now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _confidence(value):
    if value is None or isinstance(value, bool): return None
    try: number = float(value)
    except (TypeError, ValueError): return None
    return round(number, 3) if math.isfinite(number) and 0 <= number <= 1 else None


def _numeric(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _url(value):
    if not value: return None
    try: return public_source_url(value)
    except (ValueError, TypeError): return None


def _trace(payload):
    # Processing clocks do not define an economic observation. Content, units,
    # identity, vintage, raw evidence and parent traces do.
    identity = {k: payload.get(k) for k in ("contract_version", "field", "value", "unit", "as_of", "vintage", "derivation", "reason")}
    source = payload.get("source") or {}
    identity["source"] = {k: source.get(k) for k in ("kind", "series_id", "url", "raw_snapshot_key")}
    identity["evidence"] = payload.get("evidence")
    return hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def wrap(value, field, unit=None, source="unknown", series_id=None, url=None, as_of=None,
         confidence=None, fetched_by=None, raw_key=None, *, published_at=None, received_at=None,
         vintage=None, evidence=None):
    """Attribute a measurement without inventing dates, confidence or permissions."""
    if not isinstance(field, str) or not field.strip():
        raise ValueError("stable measurement field required")
    if not _numeric(value):
        return missing(field, "value is missing or is not a finite number", unit=unit, source=source)
    kind = source if source in SOURCE_KINDS else "unknown"
    source_url = _url(url)
    evidence = evidence if isinstance(evidence, dict) else None
    if evidence is not None:
        # Receipts are references supplied by the producer. Only the replay path
        # can promote them to verified bytes; this pure function cannot do IO.
        evidence = {k: evidence[k] for k in ("contract", "key", "sha256", "bytes", "provider", "source_url", "first_received_at", "captured", "basis") if k in evidence}
        if "source_url" in evidence: evidence["source_url"] = _url(evidence["source_url"])
    gaps = []
    if unit is None: gaps.append("unit_unknown")
    if not as_of: gaps.append("observation_period_unknown")
    if not series_id: gaps.append("series_identity_unknown")
    if kind == "unknown": gaps.append("source_kind_unknown")
    if not source_url: gaps.append("source_url_unknown")
    if not evidence: gaps.append("source_evidence_not_verified")
    result = {"contract_version": CONTRACT, "field": field, "value": value, "unit": unit,
              "as_of": as_of, "observation_period": as_of, "published_at": published_at,
              "received_at": received_at, "wrapped_at": _now(), "vintage": vintage,
              "confidence": _confidence(confidence), "confidence_kind": "producer_heuristic_not_predictive_probability",
              "data_unavailable": False,
              "source": {"kind": kind, "series_id": series_id, "url": source_url,
                         "fetched_at": received_at, "fetched_by": fetched_by, "raw_snapshot_key": raw_key},
              "quality": {"status": "attributed" if not gaps else "incomplete", "gaps": gaps,
                          "evidence_status": "referenced" if evidence else "unverified", "replay_verified": False},
              "evidence": evidence, "sizing_eligible": False}
    result["trace_id"] = _trace(result)
    return result


def derive(value, field, formula, inputs, unit=None, confidence=None):
    """Retain parent identities and zero confidence. This does not execute a formula."""
    parents, confidences, periods, missing_inputs = [], [], [], []
    for index, parent in enumerate(inputs or []):
        if not is_envelope(parent):
            missing_inputs.append(str(index)); continue
        parents.append({k: parent.get(k) for k in ("field", "value", "unit", "source", "as_of", "trace_id", "evidence", "quality")})
        if parent.get("data_unavailable") or not _numeric(parent.get("value")):
            missing_inputs.append(str(parent.get("field") or index))
        confidences.append(_confidence(parent.get("confidence")))
        if parent.get("as_of"): periods.append(str(parent["as_of"]))
    if not parents: missing_inputs.append("no_attributed_inputs")
    declared = _confidence(confidence)
    known_inputs = [c for c in confidences if c is not None]
    inherited = min(known_inputs) if known_inputs else None
    # An explicit output confidence cannot boost known weaker input confidence.
    known = [c for c in (declared, inherited) if c is not None]
    result = {"contract_version": CONTRACT, "field": field,
              "value": value if _numeric(value) and not missing_inputs else None,
              "unit": unit, "as_of": periods[0] if periods and len(periods) == len(parents) and len(set(periods)) == 1 else None,
              "observation_periods": sorted(set(periods)), "calculated_at": _now(),
              "confidence": min(known) if known else None,
              "confidence_kind": "producer_heuristic_not_predictive_probability",
              "data_unavailable": bool(missing_inputs) or not _numeric(value),
              "source": {"kind": "computed", "series_id": None, "url": None, "fetched_at": None, "fetched_by": "derive"},
              "derivation": {"formula": formula, "inputs": parents, "missing_inputs": missing_inputs,
                             "formula_execution_verified": False, "alignment_verified": False},
              "quality": {"status": "unavailable" if missing_inputs else "unverified_calculation",
                          "replay_verified": False, "evidence_status": "referenced_parents",
                          "unknown_input_confidences": sum(c is None for c in confidences)},
              "sizing_eligible": False}
    result["trace_id"] = _trace(result)
    return result


def missing(field, reason="source unavailable", unit=None, source=None):
    result = {"contract_version": CONTRACT, "field": field, "value": None, "unit": unit,
              "data_unavailable": True, "reason": str(reason)[:200], "as_of": None,
              "wrapped_at": _now(), "confidence": 0.0, "sizing_eligible": False,
              "source": {"kind": source or "unknown", "series_id": None, "url": None, "fetched_at": None},
              "quality": {"status": "unavailable", "evidence_status": "unverified", "replay_verified": False}}
    result["trace_id"] = _trace(result)
    return result


def batch_wrap(mapping, source="unknown", unit=None, **kw):
    return {key: missing(key, "value was None", unit=unit, source=source) if value is None else
            wrap(value, key, unit=unit, source=source, **kw) for key, value in (mapping or {}).items()}


def unwrap(obj, default=None):
    if is_envelope(obj): return obj.get("value") if not obj.get("data_unavailable") else default
    return obj if not isinstance(obj, dict) else default


def is_envelope(obj):
    return isinstance(obj, dict) and "value" in obj and "source" in obj


def coverage(payload):
    """Count the full JSON tree; structural coverage is distinct from verified replay."""
    total = wrapped = replayed = unavailable = 0
    # JSON payloads are acyclic; no depth/list truncation can inflate coverage.
    stack = [payload]
    while stack:
        node = stack.pop()
        if is_envelope(node):
            if _numeric(node.get("value")):
                total += 1; wrapped += 1
                if (node.get("contract_version") == CONTRACT and isinstance(node.get("quality"), dict)
                        and node["quality"].get("replay_verified") is True and node.get("evidence")):
                    replayed += 1
            elif node.get("data_unavailable"): unavailable += 1
        elif isinstance(node, dict): stack.extend(node.values())
        elif isinstance(node, list): stack.extend(node)
        elif _numeric(node): total += 1
    return {"contract_version": "provenance-coverage.v2", "numeric_leaves": total, "with_provenance": wrapped,
            "coverage_pct": round(100 * wrapped / total, 1) if total else 0.0,
            "coverage_meaning": "structural_envelope_only_not_source_verification",
            "replay_verified": replayed, "replay_verified_pct": round(100 * replayed / total, 1) if total else 0.0,
            "unavailable_envelopes": unavailable, "scan_complete": True}
