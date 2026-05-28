"""
justhodl-scheduler — EventBridge Schedule Fanout Router
========================================================
Replaces 1-rule-per-Lambda with 10 tick rules + this router. Reads the
manifest (s3://justhodl-dashboard-live/config/schedule-manifest.json) and
invokes every Lambda registered against the current tick.

EVENT INPUT (passed by each tick rule's Input parameter):
  {"tick": "5min"}            -- one of the canonical tick names

MANIFEST SHAPE:
  {
    "version": "1.0",
    "ticks": {
      "1min":          ["lambda-a", ...],
      "5min":          [...],
      "15min":         [...],
      "30min":         [...],
      "hourly":        [...],
      "4hourly":       [...],
      "daily-morn":    [...],   # ~11 UTC = 7AM ET
      "daily-eve":     [...],   # ~22 UTC = 6PM ET
      "weekly-sun":    [...],
      "monthly":       [...]
    },
    "disabled": ["lambda-x", ...]    # never invoked even if listed in ticks
  }

INVOCATION: async (InvocationType="Event") — fire-and-forget, parallel.
Errors on individual invokes are logged and counted but don't fail the tick.
"""
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
MANIFEST_KEY = "config/schedule-manifest.json"
MAX_WORKERS = 24

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def _load_manifest():
    try:
        o = s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)
        m = json.loads(o["Body"].read())
        return m, None
    except Exception as e:
        return None, str(e)


def _invoke_one(fn_name):
    """Async invoke — returns (fn_name, status_code, err_str)."""
    try:
        r = lam.invoke(FunctionName=fn_name, InvocationType="Event", Payload=b"{}")
        return fn_name, r.get("StatusCode"), None
    except Exception as e:
        return fn_name, None, str(e)[:200]


def lambda_handler(event, context):
    started = time.time()
    tick = (event or {}).get("tick") or (event or {}).get("Input", {}).get("tick")
    if not tick:
        return {"statusCode": 400, "body": json.dumps({"err": "no tick in event", "event": event})}

    manifest, err = _load_manifest()
    if not manifest:
        return {"statusCode": 500, "body": json.dumps({"err": f"manifest load failed: {err}"})}

    disabled = set(manifest.get("disabled") or [])
    fns = [f for f in (manifest.get("ticks") or {}).get(tick, []) if f not in disabled]

    if not fns:
        return {"statusCode": 200, "body": json.dumps({"tick": tick, "invoked": 0, "note": "no jobs"})}

    results = {"ok": [], "err": []}
    with ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(fns))) as ex:
        futures = [ex.submit(_invoke_one, fn) for fn in fns]
        for fut in as_completed(futures, timeout=context.get_remaining_time_in_millis() / 1000 - 5 if context else 120):
            fn, sc, e = fut.result()
            if e or (sc and sc >= 300):
                results["err"].append({"fn": fn, "sc": sc, "err": e})
                print(f"[scheduler] FAIL {fn}: sc={sc} err={e}")
            else:
                results["ok"].append(fn)

    body = {
        "tick": tick,
        "invoked_ok": len(results["ok"]),
        "invoked_err": len(results["err"]),
        "elapsed_s": round(time.time() - started, 2),
        "errors": results["err"][:5],
    }
    print(f"[scheduler] tick={tick} ok={body['invoked_ok']} err={body['invoked_err']} {body['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps(body)}
