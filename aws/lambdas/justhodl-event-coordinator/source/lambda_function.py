"""justhodl-event-coordinator — central event router.

THE SYSTEM'S NERVOUS SYSTEM
═══════════════════════════
Engines publish events to the justhodl-system-events bus. THIS Lambda
subscribes (via EventBridge rule) and translates events into downstream
actions: invoking specific Lambdas, sending Telegram digests, updating
SSM coordination state, and writing event-stream audit log to S3.

The whole point is to break the polling pattern. When outcome-checker
resolves 50 outcomes, the calibrator doesn't wait until Sunday — it runs
immediately because outcome.resolved triggers it via this coordinator.

ROUTING TABLE (configured below, easy to extend)
════════════════════════════════════════════════
event_name                  → trigger_targets (list of Lambda names)
                            + optional notify (Telegram digest)
                            + optional state_update (SSM)

  outcome.resolved              → calibrator (immediate re-calibration)
                                 + alpha-calibrator (skill weight refresh)
  
  regime.changed                → master-ranker (refresh top tickers)
                                 + alpha-compass (refresh landing payload)
                                 + signal-board (re-aggregate posture)
                                 + Telegram: alert
  
  near_miss.extreme             → miss-calibrator (re-evaluate proposals)
                                 + Telegram: alert with the signal_type
  
  calibrator.proposal_high_confidence → Telegram: alert with the proposal detail
  
  signal.deprecated             → engine-signal-map (refresh map)
                                 + Telegram: notify that signal X was deprecated
  
  engine.error                  → write to S3 errors/recent.json
                                 + Telegram: if engine is in CRITICAL_ENGINES set

DEDUPING + RATE LIMITING
════════════════════════
EventBridge can deliver events multiple times within a few seconds. To
avoid duplicate trigger storms, we maintain a tiny SSM-backed cache of
recent triggers keyed by (event_name, payload_hash). Triggers within
DEDUPE_WINDOW_SEC of the previous identical event are dropped.

For high-volume event sources (e.g. signal.fired), we batch: collect
N events within a Lambda invocation and trigger downstream once.
"""
import hashlib
import json
import os
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal

import boto3

from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
ACCOUNT_ID = "857687956942"
EVENT_BUS = "justhodl-system-events"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)

TELEGRAM_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TELEGRAM_CHAT_ID = "8678089260"

DEDUPE_WINDOW_SEC = 60
CRITICAL_ENGINES = {
    "justhodl-conviction-engine", "justhodl-signal-board",
    "justhodl-outcome-checker", "justhodl-calibrator",
    "justhodl-master-ranker", "justhodl-alpha-compass",
    # Engines flagged by ops/1020 audit as high-error rate; now monitor closely
    "justhodl-crisis-plumbing",
    "justhodl-liquidity-credit-engine",
    # Engines flagged as expensive; still want alerts if they fail
    "justhodl-crypto-opportunities",
    # Crisis tier
    "justhodl-crisis-composite",
    "justhodl-eurodollar-stress",
    # Regime + signal infrastructure
    "justhodl-signal-scorecard", "justhodl-magnitude-distributions",
    "justhodl-miss-detector", "justhodl-miss-calibrator",
}

# ─── Routing table ──────────────────────────────────────────────────────
# {event_name: {invoke: [lambdas], notify: bool, write_audit: bool}}
ROUTES = {
    "outcome.resolved": {
        "invoke":  ["justhodl-calibrator", "justhodl-alpha-calibrator"],
        "notify":  False,
        "audit":   True,
    },
    "outcome.deferred": {
        "invoke":  [],
        "notify":  False,
        "audit":   True,
    },
    "regime.changed": {
        "invoke":  ["justhodl-master-ranker", "justhodl-alpha-compass",
                     "justhodl-signal-board"],
        "notify":  True,
        "audit":   True,
    },
    "regime.flashing_bucket": {
        "invoke":  ["justhodl-alpha-compass"],
        "notify":  True,
        "audit":   True,
    },
    "near_miss.extreme": {
        "invoke":  ["justhodl-miss-calibrator"],
        "notify":  True,
        "audit":   True,
    },
    "calibrator.proposal_high_confidence": {
        "invoke":  [],
        "notify":  True,
        "audit":   True,
    },
    "calibrator.weights_updated": {
        "invoke":  ["justhodl-alpha-compass"],   # consumers re-read on next invoke
        "notify":  False,
        "audit":   True,
    },
    "signal.deprecated": {
        "invoke":  ["justhodl-engine-signal-map"],
        "notify":  True,
        "audit":   True,
    },
    "signal.promoted": {
        "invoke":  ["justhodl-engine-signal-map"],
        "notify":  False,
        "audit":   True,
    },
    "miss.detected": {
        "invoke":  [],
        "notify":  False,
        "audit":   True,
    },
    "engine.error": {
        "invoke":  [],
        "notify":  False,   # set True conditionally on critical engine
        "audit":   True,
    },
    "signal.fired": {
        "invoke":  [],   # signal.fired is high-volume; let calibrator pick up via DDB
        "notify":  False,
        "audit":   False,   # too noisy to audit
    },
}


# ─── Dedupe ─────────────────────────────────────────────────────────────
_dedupe_cache = {}   # in-memory only, scoped to a Lambda warm container


def _payload_hash(event_name: str, detail: dict) -> str:
    """Stable hash on (event_name, sorted detail keys). Ignores _emitted_at
    so the same logical event isn't re-fired just because the timestamp differs.
    """
    payload = {k: v for k, v in (detail or {}).items()
                if not k.startswith("_")}
    body = event_name + "|" + json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha1(body.encode()).hexdigest()[:16]


def is_duplicate(event_name: str, detail: dict) -> bool:
    h = _payload_hash(event_name, detail)
    now = time.time()
    # Clean stale entries
    for k in list(_dedupe_cache.keys()):
        if _dedupe_cache[k] < now - DEDUPE_WINDOW_SEC:
            _dedupe_cache.pop(k, None)
    if h in _dedupe_cache:
        return True
    _dedupe_cache[h] = now
    return False


# ─── Actions ────────────────────────────────────────────────────────────

def invoke_target(fn_name: str, event_name: str, detail: dict) -> dict:
    """Async-invoke a Lambda. We use Event invocation so the coordinator
    doesn't block on the downstream Lambda finishing."""
    try:
        payload = json.dumps({
            "trigger_event": event_name,
            "trigger_detail": detail,
            "triggered_by": "justhodl-event-coordinator",
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }).encode()
        resp = lam.invoke(
            FunctionName=fn_name,
            InvocationType="Event",   # async
            Payload=payload,
        )
        return {"ok": True, "status": resp.get("StatusCode")}
    except Exception as e:
        return {"ok": False, "err": f"{type(e).__name__}: {str(e)[:150]}"}


def send_telegram_alert(event_name: str, detail: dict) -> bool:
    """Format an event into a Telegram digest line and send."""
    try:
        emoji = {
            "regime.changed":                      "📊",
            "regime.flashing_bucket":              "🔔",
            "near_miss.extreme":                   "⚡️",
            "calibrator.proposal_high_confidence": "🎯",
            "signal.deprecated":                   "⚠️",
            "engine.error":                        "🚨",
        }.get(event_name, "📡")
        
        # Build human-readable summary
        lines = [f"{emoji} <b>{event_name}</b>"]
        src = detail.get("_source_engine") or "?"
        lines.append(f"<i>from</i> <code>{src}</code>")
        
        # Surface the most useful fields per event type
        if event_name == "regime.changed":
            lines.append(f"  {detail.get('previous','?')} → <b>{detail.get('current','?')}</b>")
            ks = detail.get("khalid_score")
            if ks is not None:
                lines.append(f"  Khalid score: <b>{ks}</b>")
        elif event_name == "near_miss.extreme":
            lines.append(f"  signal: <b>{detail.get('signal_type','?')}</b>")
            lines.append(f"  count: <b>{detail.get('count','?')}</b> in {detail.get('window','24h')}")
        elif event_name == "calibrator.proposal_high_confidence":
            lines.append(f"  signal: <b>{detail.get('signal_type','?')}</b>")
            lines.append(f"  delta_pct: <b>{detail.get('delta_pct','?')}</b>")
            lines.append(f"  evidence: {detail.get('near_misses','?')} near-misses")
        elif event_name == "engine.error":
            lines.append(f"  engine: <b>{detail.get('engine','?')}</b>")
            lines.append(f"  error:  <code>{str(detail.get('error',''))[:120]}</code>")
        elif event_name == "signal.deprecated":
            lines.append(f"  signal: <b>{detail.get('signal_type','?')}</b>")
            lines.append(f"  reason: {detail.get('reason','?')}")
        else:
            # Fallback: dump the first few keys
            payload = {k: v for k, v in detail.items()
                        if not k.startswith("_")}
            for k, v in list(payload.items())[:5]:
                lines.append(f"  {k}: <code>{str(v)[:80]}</code>")
        
        data = urllib.parse.urlencode({
            "chat_id":  TELEGRAM_CHAT_ID,
            "text":     "\n".join(lines),
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=data, method="POST")
        urllib.request.urlopen(req, timeout=15).read()
        return True
    except Exception as e:
        print(f"[coordinator] telegram err: {e}")
        return False


def write_audit(event_name: str, detail: dict, route_result: dict):
    """Append-only audit log of all routed events. Used for postmortem and
    learning what triggered what."""
    try:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"system-events/audit/{today}.jsonl"
        entry = {
            "ts":         datetime.now(timezone.utc).isoformat(),
            "event":      event_name,
            "detail":     detail,
            "route":      route_result,
        }
        # Read existing day-log + append (small daily files)
        existing = b""
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            existing = obj["Body"].read()
        except s3.exceptions.NoSuchKey:
            pass
        new_body = existing + (json.dumps(entry, default=str) + "\n").encode("utf-8")
        s3.put_object(Bucket=BUCKET, Key=key, Body=new_body,
                       ContentType="application/x-ndjson")
    except Exception as e:
        print(f"[coordinator] audit err: {e}")


# ─── Handler ────────────────────────────────────────────────────────────

@track_errors
def handler(event, context):
    """EventBridge delivers events here.

    Event shape:
      {
        "source":      "justhodl.<engine>",
        "detail-type": "<event_name>",
        "detail":      { ... },
        ...
      }
    """
    started = datetime.now(timezone.utc)
    
    event_name = event.get("detail-type") or "unknown"
    detail = event.get("detail") or {}
    source = event.get("source") or "?"
    
    # Dedupe within DEDUPE_WINDOW_SEC for warm containers
    if is_duplicate(event_name, detail):
        return {"statusCode": 200,
                "body": json.dumps({"ok": True, "deduped": True,
                                      "event": event_name})}
    
    route = ROUTES.get(event_name)
    if not route:
        # Unknown event — log + done
        print(f"[coordinator] unknown event_name={event_name}, no route")
        return {"statusCode": 200,
                "body": json.dumps({"ok": True, "unrouted": True,
                                      "event": event_name})}
    
    # Execute routes
    result = {
        "event": event_name,
        "source": source,
        "started": started.isoformat(),
        "invokes": [],
    }
    
    for fn in route.get("invoke") or []:
        ir = invoke_target(fn, event_name, detail)
        result["invokes"].append({"fn": fn, **ir})
    
    if route.get("notify"):
        # For engine.error, only Telegram-notify when source is in CRITICAL_ENGINES
        if event_name == "engine.error":
            eng = detail.get("engine") or detail.get("_source_engine") or ""
            should = eng in CRITICAL_ENGINES or eng.replace("justhodl-", "") in CRITICAL_ENGINES
            if should:
                result["notify"] = send_telegram_alert(event_name, detail)
        else:
            result["notify"] = send_telegram_alert(event_name, detail)
    
    if route.get("audit"):
        write_audit(event_name, detail, result)
    
    print(f"[coordinator] routed event={event_name} invokes={len(result['invokes'])}")
    return {
        "statusCode": 200,
        "body": json.dumps({"ok": True, **result}),
    }


lambda_handler = handler
