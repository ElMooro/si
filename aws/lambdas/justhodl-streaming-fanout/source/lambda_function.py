"""
justhodl-streaming-fanout -- the institutional intraday signal-delta
driver.

The batch engines update on 3-6h cadences; justhodl-live-pulse runs
every 15 minutes. Subscribed dashboards (live.html) need to be
notified the moment a tracked signal moves enough to matter. This
engine runs every 1 minute (US market window 13:30-21:00 UTC), reads
each tracked S3 output, compares to its last-broadcast snapshot
sidecar, and asynchronously invokes openbb-websocket-broadcast with a
concise delta payload if-and-only-if the move is institutionally
meaningful (score delta >= threshold OR level/posture/regime flip).

This is the "smart fanout" layer. It guarantees:
  - subscribers see every meaningful move, not silence between batches
  - subscribers are NOT spammed by every byte-level S3 write
  - the trigger is auditable in S3 (one sidecar per engine + a master
    fanout log for observability)

Tracked engines and their delta thresholds:

    engine               channel        headline          delta rule
    --------------------- -------------- ----------------- -----------------
    global-stress        global_stress  global_stress_idx |dx| >= 3 or flip
    live-pulse           live_pulse     pulse             |dx| >= 5 or flip
    vol-radar            vol_radar      spike_risk_score  |dx| >= 3 or flip
    signal-board         signal_board   composite         |dx| >= 3 or flip
    crisis-composite     crisis         score             |dx| >= 3 or flip
    master-allocation    master_alloc   active_risk_bps   |dx| >= 100 or flip
    dollar-radar         dollar_radar   score             |dx| >= 3 or flip

Outputs:
    data/streaming-fanout.json      -- per-run audit log
    data/_streaming/{engine}_last.json  -- per-engine sidecar
    data/streaming-config.json      -- public WS endpoint config for
                                       live.html to discover

Env (set by ops 896):
    WS_API_ID    -- API Gateway WebSocket API ID (for client URL)
    WS_STAGE     -- WS stage, default "prod"
    BROADCAST_FN -- downstream broadcast Lambda name
                    (default "openbb-websocket-broadcast")
"""
import json
import os
import time
import hmac
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
S3_BUCKET = "justhodl-dashboard-live"
FANOUT_LOG_KEY = "data/streaming-fanout.json"
CONFIG_KEY = "data/streaming-config.json"
SIDECAR_PREFIX = "data/_streaming/"
ADMIN_TOKEN_SSM = "/justhodl/push/admin-token"

WS_API_ID = os.environ.get("WS_API_ID", "")
WS_STAGE = os.environ.get("WS_STAGE", "prod")
BROADCAST_FN = os.environ.get("BROADCAST_FN", "openbb-websocket-broadcast")

s3 = boto3.client("s3", region_name=REGION)
ssm = boto3.client("ssm", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

# ----- tracked engines ----------------------------------------------------
# Each engine: (name, channel, s3_key, headline rules, flip-field rules)
# Headline change rule: |new - prev| >= threshold.
# Flip rule: any change of the categorical field triggers broadcast.
TRACKED = [
    {
        "name": "global_stress",
        "channel": "global_stress",
        "s3_key": "data/global-stress.json",
        "headline_field": "global_stress_index",
        "headline_threshold": 3,
        "flip_field": "global_stress_level",
        "summary_fields": ["global_stress_index", "global_stress_level",
                           "as_of"],
    },
    {
        "name": "live_pulse",
        "channel": "live_pulse",
        "s3_key": "data/live-pulse.json",
        "headline_field": "pulse",
        "headline_threshold": 5,
        "flip_field": "level",
        "summary_fields": ["pulse", "level", "drift_vs_morning_gsi",
                           "as_of"],
    },
    {
        "name": "vol_radar",
        "channel": "vol_radar",
        "s3_key": "data/vol-radar.json",
        "headline_field": "spike_risk_score",
        "headline_threshold": 3,
        "flip_field": "regime",
        "summary_fields": ["spike_risk_score", "regime",
                           "exhaustion_score", "as_of"],
    },
    {
        "name": "signal_board",
        "channel": "signal_board",
        "s3_key": "data/signal-board.json",
        "headline_field": "composite_score",
        "headline_threshold": 3,
        "flip_field": "posture",
        "summary_fields": ["composite_score", "posture",
                           "n_engines_aligned", "as_of"],
    },
    {
        "name": "crisis_composite",
        "channel": "crisis",
        "s3_key": "data/crisis-composite.json",
        "headline_field": "score",
        "headline_threshold": 3,
        "flip_field": "level",
        "summary_fields": ["score", "level", "as_of"],
    },
    {
        "name": "master_alloc",
        "channel": "master_alloc",
        "s3_key": "data/master-allocation.json",
        "headline_field": "active_risk_bps",
        "headline_threshold": 100,
        "flip_field": "posture",
        "summary_fields": ["posture", "active_risk_bps",
                           "confidence", "as_of"],
    },
    {
        "name": "dollar_radar",
        "channel": "dollar_radar",
        "s3_key": "data/dollar-radar.json",
        "headline_field": "score",
        "headline_threshold": 3,
        "flip_field": "stance",
        "summary_fields": ["score", "stance", "as_of"],
    },
]


def _now():
    return datetime.now(timezone.utc).isoformat()


def _read_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception:
        return None


def _write_json(key, body, cache="no-cache"):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                  Body=json.dumps(body, default=str),
                  ContentType="application/json",
                  CacheControl=cache)


def _admin_token():
    """Read the admin token once per cold start."""
    if hasattr(_admin_token, "_cached"):
        return _admin_token._cached
    try:
        p = ssm.get_parameter(Name=ADMIN_TOKEN_SSM, WithDecryption=True)
        _admin_token._cached = p["Parameter"]["Value"]
    except Exception as e:
        print("admin token fetch fail: %s" % e)
        _admin_token._cached = ""
    return _admin_token._cached


def _extract_summary(engine, body):
    """Pick the small set of fields we want to broadcast."""
    summary = {"engine": engine["name"]}
    for f in engine["summary_fields"]:
        v = body.get(f)
        if v is not None:
            summary[f] = v
    return summary


def _is_meaningful_delta(engine, prev_summary, curr_summary):
    """Apply the per-engine delta rule. Returns (bool, reason)."""
    if prev_summary is None:
        return True, "first-broadcast"

    # headline numeric change
    hf = engine["headline_field"]
    prev_h = prev_summary.get(hf)
    curr_h = curr_summary.get(hf)
    if (isinstance(prev_h, (int, float)) and
            isinstance(curr_h, (int, float))):
        if abs(curr_h - prev_h) >= engine["headline_threshold"]:
            return True, "headline %s -> %s (>= %s)" % (
                prev_h, curr_h, engine["headline_threshold"])

    # categorical flip
    ff = engine.get("flip_field")
    if ff:
        prev_f = prev_summary.get(ff)
        curr_f = curr_summary.get(ff)
        if prev_f and curr_f and prev_f != curr_f:
            return True, "%s flip %s -> %s" % (ff, prev_f, curr_f)

    return False, "below threshold"


def _broadcast(channel, payload):
    """Asynchronously invoke openbb-websocket-broadcast with HTTP-shaped
    event so its existing POST path handles auth and fanout."""
    token = _admin_token()
    if not token:
        return {"sent": False, "reason": "no admin token"}
    event = {
        "requestContext": {"http": {"method": "POST"}},
        "headers": {"x-justhodl-admin-token": token,
                    "content-type": "application/json"},
        "body": json.dumps({"channel": channel, **payload}),
    }
    try:
        lam.invoke(FunctionName=BROADCAST_FN,
                   InvocationType="Event",
                   Payload=json.dumps(event).encode("utf-8"))
        return {"sent": True, "channel": channel}
    except Exception as e:
        return {"sent": False, "reason": str(e)[:200]}


def _write_streaming_config():
    """Publish the WS endpoint so live.html can self-configure."""
    if not WS_API_ID:
        return None
    cfg = {
        "as_of": _now(),
        "ws_url": "wss://%s.execute-api.%s.amazonaws.com/%s" % (
            WS_API_ID, REGION, WS_STAGE),
        "channels": sorted(set(e["channel"] for e in TRACKED)),
        "engines": [{"name": e["name"], "channel": e["channel"],
                     "headline_field": e["headline_field"],
                     "headline_threshold": e["headline_threshold"],
                     "flip_field": e["flip_field"]}
                    for e in TRACKED],
        "notes": (
            "WebSocket endpoint for live institutional signal stream. "
            "Connect with no params, then send "
            "{\"action\":\"subscribe\",\"channels\":[...]} to subscribe. "
            "Server sends "
            "{\"action\":\"push\",\"channel\":\"...\","
            "\"ts\":\"...\",\"payload\":{...}}."),
    }
    try:
        _write_json(CONFIG_KEY, cfg)
        return cfg
    except Exception as e:
        return {"error": str(e)}


def lambda_handler(event, context):
    started = _now()
    actions = []

    cfg = _write_streaming_config()

    for engine in TRACKED:
        action = {"engine": engine["name"], "channel": engine["channel"]}
        curr = _read_json(engine["s3_key"])
        if not curr:
            action["status"] = "source missing"
            actions.append(action)
            continue

        curr_summary = _extract_summary(engine, curr)
        sidecar_key = SIDECAR_PREFIX + engine["name"] + "_last.json"
        prev_summary = (_read_json(sidecar_key) or {}).get("summary")

        meaningful, reason = _is_meaningful_delta(
            engine, prev_summary, curr_summary)
        action["reason"] = reason
        action["headline"] = curr_summary.get(engine["headline_field"])
        action["flip_state"] = curr_summary.get(engine.get("flip_field"))

        if meaningful:
            payload = {"updated": True, "delta_reason": reason,
                       "summary": curr_summary, "ts": _now()}
            res = _broadcast(engine["channel"], payload)
            action["status"] = "broadcast"
            action["broadcast_result"] = res

            # update sidecar with broadcast-state snapshot
            try:
                _write_json(sidecar_key, {
                    "as_of": _now(),
                    "last_polled_at": _now(),
                    "last_broadcast_at": _now(),
                    "broadcast": True,
                    "summary": curr_summary,
                    "last_broadcast_reason": reason,
                })
            except Exception as e:
                action["sidecar_write_err"] = str(e)[:200]
        else:
            action["status"] = "no-op"
            # OBSERVABILITY FIX 2026-05-21: refresh sidecar even on no-op so
            # ops audits can distinguish "engine is calm" (recent
            # last_polled_at, broadcast=False) from "fanout stopped polling"
            # (stale last_polled_at). Previously sidecar only updated on
            # broadcast, making 5 of 7 sidecars appear 33h+ stale despite
            # the upstream Lambda running every minute.
            try:
                existing = _read_json(sidecar_key) or {}
                _write_json(sidecar_key, {
                    "as_of": _now(),
                    "last_polled_at": _now(),
                    "last_broadcast_at": existing.get("last_broadcast_at"),
                    "broadcast": False,
                    "summary": curr_summary,
                    "no_op_reason": reason,
                    "last_broadcast_reason":
                        existing.get("last_broadcast_reason"),
                })
            except Exception as e:
                action["sidecar_write_err"] = str(e)[:200]

        actions.append(action)

    log = {
        "ok": True,
        "as_of": _now(),
        "started": started,
        "tracked_engines": len(TRACKED),
        "broadcasts": sum(1 for a in actions
                          if a.get("status") == "broadcast"),
        "no_ops": sum(1 for a in actions if a.get("status") == "no-op"),
        "missing": sum(1 for a in actions
                       if a.get("status") == "source missing"),
        "actions": actions,
        "ws_config": cfg,
    }
    try:
        _write_json(FANOUT_LOG_KEY, log)
    except Exception as e:
        log["log_write_err"] = str(e)[:200]

    print("fanout: broadcasts=%d no_ops=%d missing=%d" %
          (log["broadcasts"], log["no_ops"], log["missing"]))
    return {"statusCode": 200, "body": json.dumps(log, default=str)}
