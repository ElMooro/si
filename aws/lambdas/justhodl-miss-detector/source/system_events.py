"""system_events — shared helper for emitting events to the JustHodl
EventBridge custom bus.

WHY THIS EXISTS
═══════════════
Today engines coordinate by writing to S3 and other engines polling. That
works at hourly granularity but breaks down when:

  - An engine produces output mid-cycle and consumers need to know NOW
    (e.g., outcome-checker resolves 50 outcomes — calibrator should run
    immediately, not wait until next scheduled cycle)
  - A composite engine detects a regime change — every downstream consumer
    needs to refresh, but they don't know to until their own next cron
  - A signal calibrator deprecates a signal — engines using that signal
    should stop weighting it immediately

The fix is an event-driven coordination layer. Engines emit small JSON
events to a custom EventBridge bus; downstream engines subscribe via
EventBridge rules that match on event detail. NO POLLING.

USAGE
═════

```python
from system_events import publish

# In outcome-checker, after resolving outcomes:
publish("outcome.resolved", {
    "n_resolved": 47,
    "n_correct": 23,
    "as_of": now.isoformat(),
})

# In any market-regime engine, after a regime transition:
publish("regime.changed", {
    "previous":  "EXPANSION",
    "current":   "CONTRACTION",
    "khalid_score": 38.2,
    "source_engine": "market-regime",
})

# In miss-calibrator, after generating a HIGH-confidence proposal:
publish("calibrator.proposal_high_confidence", {
    "signal_type":  "screener_top_pick",
    "delta_pct":    -0.18,
    "near_misses":  240,
    "confidence":   "HIGH",
})
```

EVENT SHAPE
═══════════
EventBridge events: source = "justhodl.<engine>", detail-type = the event
name, detail = the JSON payload. EventBridge rules pattern-match either
the detail-type or specific detail fields.

The publish() helper is fire-and-forget by design — engines should never
block on event publication. Failures are logged but never raised.

STANDARDISED EVENT NAMES (extend as needed):
  outcome.resolved                — outcome-checker resolved N outcomes
  outcome.deferred                — outcome-checker hit a data gap
  regime.changed                  — composite regime state changed
  regime.flashing_bucket          — specific bucket entered alert
  signal.fired                    — a directional signal crossed threshold
  signal.deprecated               — scorecard moved a signal to deprecated
  signal.promoted                 — scorecard moved a signal to promoted
  miss.detected                   — miss-detector found a true miss
  near_miss.extreme               — near-miss count exceeded threshold
  calibrator.proposal_high_confidence — high-conf threshold proposal
  calibrator.weights_updated      — calibrator wrote new SSM weights
  engine.error                    — engine hit a recoverable error worth flagging

Use the constants below or just pass the string.
"""
import json
import os
from datetime import datetime, timezone
from typing import Optional

# Defer boto3 import inside publish() to avoid cold-start cost when engines
# import this module but don't actually emit events.
EVENT_BUS_NAME = os.environ.get("JUSTHODL_EVENT_BUS", "justhodl-system-events")
DEFAULT_REGION = "us-east-1"

# ─── Standard event names (use these instead of string literals where possible) ──
EVT_OUTCOME_RESOLVED               = "outcome.resolved"
EVT_OUTCOME_DEFERRED               = "outcome.deferred"
EVT_REGIME_CHANGED                 = "regime.changed"
EVT_REGIME_FLASHING_BUCKET         = "regime.flashing_bucket"
EVT_SIGNAL_FIRED                   = "signal.fired"
EVT_SIGNAL_DEPRECATED              = "signal.deprecated"
EVT_SIGNAL_PROMOTED                = "signal.promoted"
EVT_MISS_DETECTED                  = "miss.detected"
EVT_NEAR_MISS_EXTREME              = "near_miss.extreme"
EVT_CALIBRATOR_PROPOSAL_HIGH_CONF  = "calibrator.proposal_high_confidence"
EVT_CALIBRATOR_WEIGHTS_UPDATED     = "calibrator.weights_updated"
EVT_ENGINE_ERROR                   = "engine.error"


def publish(event_name: str, detail: dict,
             source_engine: Optional[str] = None,
             bus_name: Optional[str] = None) -> bool:
    """Publish an event to the JustHodl system event bus.

    Returns True on success, False on failure. NEVER raises — failure is
    logged but the caller continues. Event publication is best-effort
    coordination, not a synchronous dependency.
    """
    try:
        import boto3
        client = boto3.client("events", region_name=DEFAULT_REGION)
        
        # Auto-derive source from AWS_LAMBDA_FUNCTION_NAME if not provided
        if source_engine is None:
            fn = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown")
            source_engine = fn.replace("justhodl-", "")
        source = f"justhodl.{source_engine}"
        
        # Attach standard fields so subscribers always see timestamp + source
        payload = dict(detail or {})
        payload.setdefault("_emitted_at", datetime.now(timezone.utc).isoformat())
        payload.setdefault("_source_engine", source_engine)
        
        entry = {
            "Source":       source,
            "DetailType":   event_name,
            "Detail":       json.dumps(payload, default=str),
            "EventBusName": bus_name or EVENT_BUS_NAME,
        }
        
        resp = client.put_events(Entries=[entry])
        failed = resp.get("FailedEntryCount", 0)
        if failed:
            err = resp.get("Entries", [{}])[0].get("ErrorMessage", "?")
            print(f"[system_events] publish '{event_name}' failed: {err}")
            return False
        return True
    except Exception as e:
        # Never propagate — log only
        print(f"[system_events] publish '{event_name}' err: {type(e).__name__}: {str(e)[:150]}")
        return False


def publish_many(events: list, bus_name: Optional[str] = None) -> dict:
    """Batch publish (up to 10 events per call, EventBridge limit).
    
    Each event is a tuple (event_name, detail_dict) or
    (event_name, detail_dict, source_engine).
    """
    try:
        import boto3
        client = boto3.client("events", region_name=DEFAULT_REGION)
        
        # Build entries
        default_source = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "unknown")\
                            .replace("justhodl-", "")
        entries = []
        for evt in events[:10]:   # EventBridge: max 10 entries per put
            if len(evt) == 3:
                name, detail, src = evt
            else:
                name, detail = evt
                src = default_source
            payload = dict(detail or {})
            payload.setdefault("_emitted_at", datetime.now(timezone.utc).isoformat())
            payload.setdefault("_source_engine", src)
            entries.append({
                "Source":       f"justhodl.{src}",
                "DetailType":   name,
                "Detail":       json.dumps(payload, default=str),
                "EventBusName": bus_name or EVENT_BUS_NAME,
            })
        
        resp = client.put_events(Entries=entries)
        return {
            "ok": resp.get("FailedEntryCount", 0) == 0,
            "n_failed": resp.get("FailedEntryCount", 0),
            "n_published": len(entries) - resp.get("FailedEntryCount", 0),
        }
    except Exception as e:
        print(f"[system_events] publish_many err: {type(e).__name__}: {str(e)[:200]}")
        return {"ok": False, "err": str(e)[:200]}
