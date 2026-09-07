"""justhodl-jh-fusion v1.1.0 -- Fusion Engine v1 for the pilot universe (Release 1, SHADOW MODE).

Reads the bridge's current-state read model (data/jhsignal/state/latest.json),
learned reliability (data/engine-trust.json effective_trust + signal-scorecard
multipliers) and the engine correlation matrix (data/signal-orthogonality.json)
when present, runs aws/shared/jh_fusion_core.run_fusion, and writes:

  data/jh-fusion.json                          the read model (all pilot entities, all horizons)
  data/jh-fusion/ledger/YYYY/MM/DD/<run_id>.json.gz   the reproducible fusion ledger (phase 31)

Triggered by the coordinator on `jhsignal.batch_published` (payload
{trigger_event, trigger_detail, ...}) and by a daily Scheduler fallback.
A trigger whose snapshot run_id was already fused is skipped (idempotent).
Shadow mode: nothing here feeds justhodl-sizing-engine.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any, Dict, Optional

import jhsignal as J
from jh_fusion_core import run_fusion, shadow_comparison
from jh_registry import EngineRegistry, load_flags, load_universe
from jh_state_store import STATE_KEY, Bus, Metrics, S3Store, _log

try:
    from _sentry_lite import track_errors
except Exception:  # pragma: no cover
    def track_errors(fn):
        return fn

VERSION = "1.1.0"
ENGINE = "jh-fusion"
OUT_KEY = "data/jh-fusion.json"
LEDGER_PREFIX = "data/jh-fusion/ledger/"
TRUST_KEY = "data/engine-trust.json"
SCORECARD_KEY = "data/signal-scorecard.json"
ORTHO_KEY = "data/signal-orthogonality.json"
CONVICTION_KEY = "data/conviction.json"
SHADOW_KEY = "data/jh-fusion/shadow.json"
SIGNALS_TABLE = os.environ.get("SIGNALS_TABLE", "justhodl-signals")
SHADOW_WINDOWS = ["5", "21", "63"]           # tactical / swing / intermediate grading windows (outcome-checker day_N)
YAHOO_SYMBOL = {"crypto": lambda t: "%s-USD" % t, "equity": lambda t: t, "etf": lambda t: t, "index": lambda t: "^" + t}


def shadow_log(result, *, flags, run_id):
    """Phase 51: put every directional best-horizon read into the fleet's graded ledger (justhodl-signals) as
    signal_type `jh_fusion` through aws/shared/signals_emit.log_signal -- the same contract the harvester uses, so
    outcome-checker prices it forward and signal-scorecard grades fusion next to every other engine. Never raises;
    one row per ticker per day (log_signal dedupes on signal_id)."""
    out = {"enabled": bool(flags.get("FUSION_SHADOW_LOGGING", True)), "logged": 0, "skipped": 0, "errors": []}
    if not out["enabled"]:
        return out
    try:
        import boto3
        from signals_emit import log_signal, yprice
        table = boto3.resource("dynamodb", region_name=os.environ.get("AWS_REGION", "us-east-1")).Table(SIGNALS_TABLE)
    except Exception as exc:
        out["errors"].append("setup: %s" % str(exc)[:160]); return out
    for eid, r in (result.get("entities") or {}).items():
        et, sym = eid.split(":", 1)
        h = (r.get("horizons") or {}).get(r.get("best_horizon")) or {}
        fs = float(h.get("fusion_score") or 0.0)
        if et not in YAHOO_SYMBOL or abs(fs) < 0.15 or float(h.get("confidence") or 0) < 0.3 or h.get("capital_decision") == "BLOCKED":
            out["skipped"] += 1; continue
        ysym = YAHOO_SYMBOL[et](sym)
        try:
            px = yprice(ysym)
            if not px:
                out["skipped"] += 1; out["errors"].append("%s: no price" % ysym); continue
            ok = log_signal(table, "jh_fusion", ysym, "UP" if fs > 0 else "DOWN", SHADOW_WINDOWS, px,
                            confidence=float(h.get("confidence") or 0.5),
                            rationale="fusion %+.2f conviction %s confidence %.2f coverage %.2f contradiction %s (%s, shadow)" % (fs, h.get("conviction"), float(h.get("confidence") or 0), float(h.get("fusion_coverage") or 0), h.get("contradiction_score"), r.get("best_horizon")),
                            metadata={"entity_id": eid, "horizon": r.get("best_horizon"), "fusion_result_id": run_id, "conviction": h.get("conviction"), "coverage": h.get("fusion_coverage"),
                                      "contradiction": h.get("contradiction_score"), "independent": h.get("independent_evidence_count"), "capital_decision": h.get("capital_decision"), "shadow": True},
                            signal_value="%.4f" % fs)
            out["logged" if ok else "skipped"] += 1
        except Exception as exc:
            out["errors"].append("%s: %s" % (ysym, str(exc)[:120]))
    out["errors"] = out["errors"][:8]
    return out


def _variants(trust_key: str):
    return (trust_key, "eng:" + trust_key, "justhodl-" + trust_key, trust_key.replace("-", "_"), "eng:justhodl-" + trust_key)


def load_reliability(s3: S3Store, registry: EngineRegistry) -> Dict[str, Any]:
    """engine_id -> reliability weight in [0.25, 1.5], with the basis for each. Missing -> not in the map (=1.0)."""
    trust, _ = s3.get_json(TRUST_KEY)
    score, _ = s3.get_json(SCORECARD_KEY)
    tmap = {e.get("signal_type"): e for e in (trust or {}).get("engines") or [] if isinstance(e, dict)}
    mult = (score or {}).get("multipliers") or {}
    out, basis = {}, {}
    for e in registry.doc["engines"]:
        tk = e.get("trust_key") or e["engine_id"]
        val = None
        for v in _variants(tk):
            row = tmap.get(v)
            if row and isinstance(row.get("effective_trust"), (int, float)) and row.get("status") not in ("WARMING", None):
                val, basis[e["engine_id"]] = float(row["effective_trust"]), "engine-trust:%s(%s,n=%s)" % (v, row.get("status"), row.get("n_scored"))
                break
        if val is None:
            for v in _variants(tk):
                m = mult.get(v)
                if isinstance(m, (int, float)):
                    val, basis[e["engine_id"]] = float(m), "scorecard multiplier:%s" % v
                    break
        if val is not None:
            out[e["engine_id"]] = max(0.25, min(1.5, val))
    return {"weights": out, "basis": basis}


def load_correlation(s3: S3Store, registry: EngineRegistry) -> Optional[Dict[str, Dict[str, float]]]:
    doc, _ = s3.get_json(ORTHO_KEY)
    m = (doc or {}).get("correlation_matrix")
    if not isinstance(m, dict):
        return None
    alias = {}
    for e in registry.doc["engines"]:
        for v in _variants(e.get("trust_key") or e["engine_id"]):
            if v in m:
                alias[e["engine_id"]] = v
                break
    out: Dict[str, Dict[str, float]] = {}
    for a, av in alias.items():
        row = m.get(av) or {}
        out[a] = {b: float(row[bv]) for b, bv in alias.items() if b != a and isinstance(row.get(bv), (int, float))}
    return out or None


@track_errors
def lambda_handler(event=None, context=None):
    t0 = time.time()
    event = event if isinstance(event, dict) else {}
    now = J.utcnow()
    run_id = "%s-%s" % (now.strftime("%Y%m%dT%H%M%SZ"), uuid.uuid4().hex[:8])
    trace = {"trace_id": run_id, "component": ENGINE}
    trigger = {"event": event.get("trigger_event") or event.get("mode") or "scheduled", "detail": event.get("trigger_detail") or {}, "triggered_by": event.get("triggered_by")}
    flags = load_flags(force=True)
    registry = EngineRegistry.load()
    universe = load_universe()
    s3 = S3Store()
    metrics = Metrics()
    bus = Bus(enabled=bool(flags.get("FUSION_SIGNAL_BUS_ENABLED", True)), origin_engine=ENGINE, max_depth=int(flags.get("FUSION_MAX_PROPAGATION_DEPTH", 3)))

    snapshot, meta = s3.get_json(STATE_KEY)
    if not snapshot:
        _log(level="error", msg="no current-state snapshot", key=STATE_KEY, meta=meta, **trace)
        metrics.put("FusionSkipped", 1, reason="no_snapshot"); metrics.flush()
        return {"ok": False, "reason": "no snapshot at %s" % STATE_KEY, "run_id": run_id}
    prior, _ = s3.get_json(OUT_KEY)
    if trigger["event"] == J.EVT_BATCH_PUBLISHED and prior and prior.get("snapshot_run_id") == snapshot.get("run_id") and not event.get("force"):
        _log(level="info", msg="snapshot already fused, skipping (idempotent)", snapshot_run_id=snapshot.get("run_id"), **trace)
        metrics.put("FusionSkipped", 1, reason="duplicate_trigger"); metrics.flush()
        return {"ok": True, "skipped": True, "snapshot_run_id": snapshot.get("run_id"), "run_id": run_id}
    parent = trigger["detail"] if isinstance(trigger["detail"], dict) and trigger["detail"].get("event_id") else None

    rel = load_reliability(s3, registry) if flags.get("FUSION_RELIABILITY_WEIGHTING_ENABLED", True) else {"weights": {}, "basis": {}}
    corr = load_correlation(s3, registry) if flags.get("FUSION_CORRELATION_ADJUST_ENABLED", True) else None
    result = run_fusion(snapshot, registry_doc=registry.doc, universe_doc=universe, flags=flags, reliability=rel["weights"], corr=corr,
                        prior=prior, now=now, run_id=run_id, trigger=trigger)
    result["reliability_basis"]["per_engine"] = rel["basis"]
    result["reliability_basis"]["n_correlation_engines"] = len(corr or {})
    result["version"] = VERSION
    # phase 51 -- shadow comparison + graded ledger (fusion never feeds sizing; the truth layer grades it instead)
    conviction, _ = s3.get_json(CONVICTION_KEY)
    shadow = shadow_comparison(result, snapshot, conviction_doc=conviction, now=now)
    shadow["logging"] = shadow_log(result, flags=flags, run_id=run_id)
    s3.put_json(SHADOW_KEY, shadow)
    result["shadow"] = {"key": SHADOW_KEY, "agreement_rate": shadow.get("agreement_rate"), "n_compared": shadow.get("n_compared"), "logging": shadow["logging"]}

    # ledger (full) + read model (full minus nothing -- the doc is small for the pilot)
    ledger_key = "%s%s/%s.json.gz" % (LEDGER_PREFIX, now.strftime("%Y/%m/%d"), run_id)
    s3.put_json(ledger_key, {"fusion_result_id": run_id, "snapshot": {k: snapshot.get(k) for k in ("run_id", "generated_at", "n_signals", "n_entities", "freshness_counts")}, "result": result}, gz=True)
    result["ledger_key"] = ledger_key
    out_bytes = s3.put_json(OUT_KEY, result)

    # events: fusion changed / hard veto / critical dependency
    n_changed = 0
    prior_ents = (prior or {}).get("entities") or {}
    for eid, r in result["entities"].items():
        pe = (prior_ents.get(eid) or {}).get("horizons") or {}
        for h, hr in r["horizons"].items():
            ph = pe.get(h) or {}
            moved = ph.get("fusion_score") is None or abs(hr["fusion_score"] - float(ph["fusion_score"])) >= 0.15 or hr["capital_decision"] != ph.get("capital_decision")
            if moved:
                n_changed += 1
                bus.publish(J.EVT_FUSION_CHANGED, {"entity_id": eid, "horizon": h, "fusion_score": hr["fusion_score"], "prev_fusion_score": ph.get("fusion_score"), "conviction": hr["conviction"], "confidence": hr["confidence"], "capital_decision": hr["capital_decision"], "fusion_result_id": run_id}, parent=parent)
            if hr["hard_vetoes"] and not ph.get("hard_vetoes"):
                bus.publish(J.EVT_HARD_VETO, {"entity_id": eid, "horizon": h, "vetoes": hr["hard_vetoes"][:3], "fusion_result_id": run_id}, parent=parent)
    if result["critical_dependencies"]["failures"] and not ((prior or {}).get("critical_dependencies") or {}).get("failures"):
        bus.publish(J.EVT_CRITICAL_DEPENDENCY_FAILED, {"failures": result["critical_dependencies"]["failures"], "fusion_result_id": run_id}, parent=parent)
    if prior and (prior.get("regime") or {}).get("label") not in (None, result["regime"]["label"]):
        bus.publish(J.EVT_REGIME_CHANGED, {"previous": prior["regime"]["label"], "current": result["regime"]["label"], "score": result["regime"]["score"], "fusion_result_id": run_id}, parent=parent)

    st = result["stats"]
    metrics.put("FusionLatencyMs", (time.time() - t0) * 1000.0, unit="Milliseconds")
    metrics.put("FusionRecomputations", 1); metrics.put("EntitiesFused", st["n_entities"])
    metrics.put("HardVetoCount", st["hard_vetoes"]); metrics.put("SoftVetoCount", st["soft_vetoes"])
    metrics.put("CoverageMean", st["coverage_mean"], unit="None"); metrics.put("ConfidenceMean", st["confidence_mean"], unit="None")
    metrics.put("CriticalDependencyFailures", len(result["critical_dependencies"]["failures"]))
    metrics.put("FusionChangedEvents", n_changed); metrics.put("EventPublishFailures", bus.failed)
    metrics.put("ShadowSignalsLogged", shadow["logging"].get("logged", 0)); metrics.put("ShadowAgreementRate", (shadow.get("agreement_rate") or 0.0), unit="None")
    flushed = metrics.flush()
    out = {"ok": True, "run_id": run_id, "out_key": OUT_KEY, "ledger_key": ledger_key, "bytes": out_bytes, "stats": st, "regime": result["regime"]["label"],
           "critical_failures": len(result["critical_dependencies"]["failures"]), "events": {"sent": bus.sent, "failed": bus.failed, "changed": n_changed},
           "shadow": result["shadow"],
           "metrics_flushed": flushed, "elapsed_s": round(time.time() - t0, 2), "shadow_mode": result["shadow_mode"]}
    _log(level="info", msg="fusion done", **out, **trace)
    return out
