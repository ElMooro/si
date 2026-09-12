"""brief-1.0 contract -- warehouse join objects only. No vendor HTTP."""
from __future__ import annotations
from datetime import datetime, timezone
from typing import Any

BRIEF_SCHEMA = "brief-1.0"
VERDICT_SCHEMA = "verdict-1.0"
MODES = ("official_stats", "plumbing", "market_tape", "positioning", "event")
FORBIDDEN_HOSTS = (
    "api.polygon.io", "api.massive.com", "elite.finviz.com", "finviz.com",
    "fred.stlouisfed.org", "api.stlouisfed.org", "financialmodelingprep.com",
)
TTL_HOURS = {
    "plumbing": 36, "official_stats": 168, "market_tape": 30,
    "positioning": 168, "event": 36,
}


def parse_ts(value: Any):
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def freshness(as_of, ttl_h, now=None):
    dt = parse_ts(as_of)
    if dt is None:
        return "EXPIRED"
    now = now or datetime.now(timezone.utc)
    age_h = (now - dt).total_seconds() / 3600.0
    if age_h <= ttl_h:
        return "FRESH"
    if age_h <= ttl_h * 2:
        return "STALE"
    return "EXPIRED"


def validate_brief(doc):
    err = []
    if not isinstance(doc, dict):
        return ["not an object"]
    if doc.get("schema") != BRIEF_SCHEMA:
        err.append("schema")
    if doc.get("mode") not in MODES:
        err.append("mode")
    if doc.get("status") not in ("LIVE", "HELD"):
        err.append("status")
    if not doc.get("inputs"):
        err.append("inputs")
    if doc.get("status") == "LIVE":
        for name, meta in (doc.get("inputs") or {}).items():
            if not isinstance(meta, dict):
                err.append("input:%s" % name)
                continue
            if meta.get("required") and meta.get("freshness") == "EXPIRED":
                err.append("expired:%s" % name)
            if meta.get("required") and meta.get("error"):
                err.append("error:%s" % name)
    return err


def validate_verdict(doc):
    err = []
    if doc.get("schema") != VERDICT_SCHEMA:
        err.append("schema")
    if doc.get("writer") != "jh-fusion-projection":
        err.append("writer")
    if doc.get("bias") not in ("risk-on", "mixed", "risk-off"):
        err.append("bias")
    if "score" not in doc:
        err.append("score")
    if not isinstance(doc.get("votes"), list):
        err.append("votes")
    return err


def project_verdict(fusion):
    now = datetime.now(timezone.utc).isoformat()
    if not fusion or fusion.get("_error"):
        return {
            "schema": VERDICT_SCHEMA,
            "writer": "jh-fusion-projection",
            "as_of": now,
            "bias": "mixed",
            "score": 0.0,
            "horizon": None,
            "votes": [],
            "conflicts": [{"note": "jh-fusion missing -- no fabricated score"}],
            "shadow_mode": True,
            "coverage": 0.0,
            "missing_families": ["ALL"],
            "fusion_run_id": None,
        }
    regime = fusion.get("regime") or {}
    ent = ((fusion.get("entities") or {}).get("market:US_EQUITY") or {})
    best = ent.get("best_horizon") or "INTERMEDIATE"
    hz = ((ent.get("horizons") or {}).get(best) or {})
    direction = (hz.get("direction") or "neutral").lower()
    if direction in ("bullish", "slightly_bullish"):
        bias = "risk-on"
    elif direction in ("bearish", "slightly_bearish"):
        bias = "risk-off"
    else:
        bias = "mixed"
    score = hz.get("fusion_score")
    if score is None:
        score = regime.get("score") or 0.0
    votes = []
    for leg in regime.get("legs") or []:
        sc = leg.get("score") or 0
        votes.append({
            "tape": "fusion_leg",
            "engine": leg.get("engine_id"),
            "sign": 1 if sc > 0.15 else (-1 if sc < -0.15 else 0),
            "score": leg.get("score"),
            "freshness": leg.get("freshness"),
            "label": leg.get("label"),
            "why": "%s %s" % (leg.get("engine_id"), leg.get("label") or leg.get("signal_type")),
        })
    conflicts = []
    if (hz.get("contradiction_class") or "LOW") not in ("LOW", None):
        conflicts.append({
            "field": "contradiction",
            "note": hz.get("contradiction_class"),
            "score": hz.get("contradiction_score"),
        })
    for fam in hz.get("missing_families") or []:
        conflicts.append({"field": "missing_family", "note": fam})
    return {
        "schema": VERDICT_SCHEMA,
        "writer": "jh-fusion-projection",
        "as_of": fusion.get("generated_at") or now,
        "bias": bias,
        "score": score,
        "horizon": best,
        "direction": direction,
        "conviction": hz.get("conviction"),
        "confidence": hz.get("confidence"),
        "coverage": hz.get("fusion_coverage"),
        "missing_families": hz.get("missing_families") or [],
        "capital_decision": hz.get("capital_decision"),
        "regime_label": regime.get("label"),
        "regime_score": regime.get("score"),
        "votes": votes,
        "conflicts": conflicts,
        "shadow_mode": bool(fusion.get("shadow_mode")),
        "fusion_run_id": fusion.get("run_id"),
        "inputs": {"data/jh-fusion.json": fusion.get("generated_at")},
    }
