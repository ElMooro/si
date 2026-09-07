"""justhodl-jhsignal-bridge v1.0.0 -- the JustHodl Intelligence Network signal bridge (Release 1).

    existing engine artifacts (S3, unchanged)
      -> SignalAdapter per registered engine (aws/shared/jh_adapters.py)
      -> JHSIGNAL-1.0 (strictly validated)
      -> current-state store (DynamoDB justhodl-jhsignal-state + S3 read model)
      -> append-only archive (data/jhsignal/archive/YYYY/MM/DD/<run_id>.jsonl.gz)
      -> intelligence bus (EventBridge justhodl-system-events via system_events.publish)
      -> CloudWatch metrics JustHodl/Fusion + structured logs

Engines never call each other: the bridge reads what they already publish
and republishes facts. Fusion is triggered by the `jhsignal.batch_published`
event through the existing coordinator route (no new EventBridge rule --
the classic rule cap is saturated) and by a daily Scheduler fallback.

Modes (event["mode"]):
  run            default -- full pass
  validate_only  run adapters, validate, report; write nothing, publish nothing
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List

import jhsignal as J
from jh_adapters import adapter_for
from jh_registry import EngineRegistry, load_flags, load_universe, universe_index
from jh_state_store import (REGISTRY_KEY, RUN_KEY, STATE_KEY, Bus, DynamoState, Metrics, S3Store, _log,
                            build_snapshot, diff_snapshots)

try:
    from _sentry_lite import track_errors
except Exception:  # pragma: no cover
    def track_errors(fn):
        return fn

VERSION = "1.0.0"
ENGINE = "jhsignal-bridge"
MAX_PER_SIGNAL_EVENTS = int(os.environ.get("JH_MAX_PER_SIGNAL_EVENTS", "250"))
_DDB_READY = {"ok": False}


def _status_for(freshness: str) -> str:
    return {"FRESH": "ACTIVE", "STALE": "STALE"}.get(freshness, "EXPIRED")


@track_errors
def lambda_handler(event=None, context=None):
    t0 = time.time()
    event = event if isinstance(event, dict) else {}
    mode = event.get("mode") or "run"
    now = J.utcnow()
    run_id = "%s-%s" % (now.strftime("%Y%m%dT%H%M%SZ"), uuid.uuid4().hex[:8])
    trace = {"trace_id": run_id, "component": ENGINE}
    flags = load_flags(force=True)
    registry = EngineRegistry.load()
    universe_doc = load_universe()
    uni = universe_index(universe_doc)
    s3 = S3Store()
    metrics = Metrics(enabled=(mode == "run"))
    bus = Bus(enabled=bool(flags.get("FUSION_SIGNAL_BUS_ENABLED", True)) and mode == "run", origin_engine=ENGINE,
              max_depth=int(flags.get("FUSION_MAX_PROPAGATION_DEPTH", 3)))
    _log(level="info", msg="bridge start", mode=mode, version=VERSION, flags=flags.get("_ssm_override"), **trace)

    signals: List[Dict[str, Any]] = []
    reports: List[Dict[str, Any]] = []
    fam = {e["engine_id"]: e["engine_family"] for e in registry.doc["engines"]}
    for spec in registry.active(flags.get("FUSION_DISABLED_ENGINES")):
        eid = spec["engine_id"]
        doc, meta = s3.get_json(spec["artifact"])
        try:
            adapter = adapter_for(spec, uni, now=now)
            res = adapter.parse_existing_output(doc, meta)
        except Exception as exc:  # adapter construction/shape failure is a metric, not a crash
            metrics.put("AdapterFailures", 1, engine=eid)
            reports.append({"engine_id": eid, "n_signals": 0, "n_rejected": 0, "source_status": "ADAPTER_ERROR",
                            "diagnostics": ["%s: %s" % (type(exc).__name__, str(exc)[:160])], "artifact_meta": meta})
            _log(level="error", msg="adapter failed", engine_id=eid, err=str(exc)[:200], **trace)
            continue
        rep = res.as_dict(); rep["artifact_meta"] = {k: meta.get(k) for k in ("last_modified", "bytes", "error")}
        rep["family"] = spec["engine_family"]; rep["criticality"] = spec["criticality"]
        reports.append(rep)
        # belt-and-braces: re-validate everything the adapter produced before it can reach the store
        good = []
        for s in res.signals:
            probs = J.validate(s, now=now)
            if probs:
                res.rejected.append({"signal_id": s.get("signal_id"), "problems": probs[:4]})
            else:
                good.append(s)
        signals.extend(good)
        metrics.put("SignalsPublished", len(good), engine=eid)
        metrics.put("SignalsRejected", len(res.rejected), engine=eid)
        metrics.put("SourceStale", 1 if res.source_status == "STALE" else 0, engine=eid)
        metrics.put("SourceMissing", 1 if res.source_status in ("MISSING", "INVALID") else 0, engine=eid)
        if res.source_status != "OK":
            _log(level="warn", msg="source not OK", engine_id=eid, status=res.source_status, diagnostics=res.diagnostics[:3], **trace)

    snapshot = build_snapshot(signals, registry_doc=registry.doc, run_id=run_id, now=now, adapter_reports=reports, flags=flags)
    prev, _ = s3.get_json(STATE_KEY)
    diff = diff_snapshots(prev, snapshot)
    stale_n = snapshot["freshness_counts"].get("STALE", 0); expired_n = snapshot["freshness_counts"].get("EXPIRED", 0)
    metrics.put("StaleSignals", stale_n); metrics.put("ExpiredSignals", expired_n)
    metrics.put("SignalsTotal", len(signals)); metrics.put("EntitiesWithSignals", snapshot["n_entities"])
    hard = [(s["entity_id"], s["engine_id"], (s.get("metadata") or {}).get("veto")) for s in signals if ((s.get("metadata") or {}).get("veto") or {}).get("type") == "HARD"]
    metrics.put("HardVetoSignals", len(hard))

    summary: Dict[str, Any] = {
        "version": VERSION, "run_id": run_id, "mode": mode, "generated_at": J.iso(now), "n_engines": len(reports),
        "n_signals": len(signals), "n_entities": snapshot["n_entities"], "freshness_counts": snapshot["freshness_counts"],
        "changes": {k: len(v) for k, v in diff.items()}, "hard_veto_signals": len(hard),
        "engines": [{k: r.get(k) for k in ("engine_id", "family", "criticality", "n_signals", "n_rejected", "source_status", "data_asof", "asof_basis")} for r in reports],
        "rejections": [{"engine_id": r["engine_id"], "sample": r.get("rejected_sample")} for r in reports if r.get("n_rejected")],
    }
    if mode != "run":
        summary["elapsed_s"] = round(time.time() - t0, 2)
        _log(level="info", msg="validate_only done", **summary, **trace)
        return summary

    # ---- current state: DynamoDB (flag) --------------------------------------------------------------
    ddb_res: Dict[str, Any] = {"enabled": bool(flags.get("FUSION_STATE_DDB_ENABLED", True))}
    if ddb_res["enabled"]:
        try:
            ddb = DynamoState()
            if not _DDB_READY["ok"]:
                ddb_res["table"] = ddb.ensure_table(); _DDB_READY["ok"] = True
            items = [DynamoState.item_from_signal(s, family=fam.get(s["engine_id"], "UNKNOWN"), status=_status_for(J.freshness_state(s, now)))
                     for s in signals if J.freshness_state(s, now) != "EXPIRED"]
            ddb_res["written"] = ddb.put_batch(items)
            ddb_res["expired_swept"] = ddb.sweep_expired(now=now)
        except Exception as exc:
            ddb_res["error"] = "%s: %s" % (type(exc).__name__, str(exc)[:200])
            metrics.put("StateWriteFailures", 1)
            _log(level="error", msg="ddb write failed", err=ddb_res["error"], **trace)
    summary["state_store"] = ddb_res

    # ---- archive + read model -------------------------------------------------------------------------
    if flags.get("FUSION_ARCHIVE_ENABLED", True):
        try:
            summary["archive_key"] = s3.append_archive(run_id, signals, now=now)
        except Exception as exc:
            summary["archive_error"] = str(exc)[:200]; metrics.put("ArchiveFailures", 1)
    summary["snapshot_bytes"] = s3.put_json(STATE_KEY, snapshot)
    s3.put_json(REGISTRY_KEY, {"generated_at": J.iso(now), "run_id": run_id, "registry": registry.doc, "universe": universe_doc, "flags": snapshot["flags"]})

    # ---- bus -------------------------------------------------------------------------------------------
    per_signal = str(flags.get("FUSION_PER_SIGNAL_EVENTS", "changes_only"))
    ev_sent = 0
    if per_signal in ("changes_only", "all"):
        pool = ([("published", s) for s in (signals if per_signal == "all" else diff["new"])]
                + [("revised", r["cur"]) for r in diff["revised"]] + [("expired", s) for s in diff["expired"]])
        for kind, s in pool[:MAX_PER_SIGNAL_EVENTS]:
            name = {"published": J.EVT_SIGNAL_PUBLISHED, "revised": J.EVT_SIGNAL_REVISED, "expired": J.EVT_SIGNAL_EXPIRED}[kind]
            bus.publish(name, {"signal": {k: s.get(k) for k in ("signal_id", "engine_id", "entity_id", "signal_type", "horizon", "direction", "score", "confidence", "data_asof")}, "run_id": run_id})
            ev_sent += 1
    for eid, engine_id, veto in hard:
        bus.publish(J.EVT_HARD_VETO, {"entity_id": eid, "engine_id": engine_id, "veto": veto, "run_id": run_id})
    bus.publish(J.EVT_BATCH_PUBLISHED, {"run_id": run_id, "n_signals": len(signals), "n_entities": snapshot["n_entities"], "changes": summary["changes"],
                                        "state_key": STATE_KEY, "archive_key": summary.get("archive_key"), "hard_veto_signals": len(hard)})
    summary["bus"] = {"sent": bus.sent, "failed": bus.failed, "suppressed": bus.suppressed, "per_signal_events": ev_sent}
    metrics.put("EventPublishFailures", bus.failed); metrics.put("EventsPublished", bus.sent)
    metrics.put("BridgeLatencyMs", (time.time() - t0) * 1000.0, unit="Milliseconds")
    summary["metrics_flushed"] = metrics.flush()
    summary["elapsed_s"] = round(time.time() - t0, 2)
    s3.put_json(RUN_KEY, summary)
    _log(level="info", msg="bridge done", n_signals=len(signals), n_entities=snapshot["n_entities"], changes=summary["changes"], elapsed_s=summary["elapsed_s"], **trace)
    return summary
