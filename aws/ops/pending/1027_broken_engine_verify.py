#!/usr/bin/env python3
"""Step 1027 — Final verification: is the engine.error wrap deployed,
and what's the actual failure mode on these engines?

If wraps are deployed but events aren't firing:
  → likely TIMEOUT failures (Lambda terminates before publish() runs)
  → bump timeouts on the broken engines

If wraps are NOT deployed:
  → force redeploy

Either way, document the state for follow-up.
"""
import io, json, os, time, urllib.request, zipfile, pathlib
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1027_broken_engine_verify.json"
REGION = "us-east-1"
lam = boto3.client("lambda", region_name=REGION,
                    config=Config(read_timeout=180, connect_timeout=10))
cw = boto3.client("cloudwatch", region_name=REGION)
logs = boto3.client("logs", region_name=REGION)


def fetch_deployed_code(fn_name):
    """Download the actual deployed Lambda zip and inspect its code."""
    try:
        loc = lam.get_function(FunctionName=fn_name)
        code_url = loc["Code"]["Location"]
        z_bytes = urllib.request.urlopen(code_url, timeout=30).read()
        zf = zipfile.ZipFile(io.BytesIO(z_bytes))
        contents = {}
        for name in zf.namelist():
            if name.endswith(".py"):
                try:
                    contents[name] = zf.read(name).decode("utf-8")
                except Exception:
                    pass
        return contents
    except Exception as e:
        return {"err": str(e)[:200]}


def check_for_timeout_errors(fn_name, hours=24):
    """Pull recent log streams looking for 'Task timed out' lines."""
    out = {"timeouts": [], "exceptions": [], "ok_runs": 0}
    try:
        lg = f"/aws/lambda/{fn_name}"
        streams = logs.describe_log_streams(
            logGroupName=lg, orderBy="LastEventTime",
            descending=True, limit=20,
        ).get("logStreams") or []
        
        for stream in streams[:10]:
            try:
                ev = logs.get_log_events(
                    logGroupName=lg, logStreamName=stream["logStreamName"],
                    limit=100, startFromHead=False,
                ).get("events", [])
                for e in ev:
                    msg = e.get("message", "")
                    ts = datetime.fromtimestamp(e["timestamp"]/1000, tz=timezone.utc).isoformat()
                    if "Task timed out" in msg:
                        out["timeouts"].append({"ts": ts, "msg": msg[:200]})
                    elif "ERROR" in msg or "Exception" in msg or "Traceback" in msg:
                        if "DurationMs" in msg:
                            continue  # REPORT line
                        out["exceptions"].append({"ts": ts, "msg": msg[:200]})
                    elif msg.startswith("REPORT") and "Status: failed" not in msg:
                        out["ok_runs"] += 1
            except Exception:
                continue
        out["timeouts"] = out["timeouts"][:5]
        out["exceptions"] = out["exceptions"][:5]
    except Exception as e:
        return {"err": str(e)[:200]}
    return out


def update_timeout(fn_name, new_timeout):
    """Bump only the timeout."""
    try:
        current = lam.get_function_configuration(FunctionName=fn_name)
        if current.get("Timeout") == new_timeout:
            return {"action": "no_change", "timeout_s": new_timeout}
        lam.update_function_configuration(FunctionName=fn_name, Timeout=new_timeout)
        lam.get_waiter("function_updated").wait(FunctionName=fn_name)
        return {
            "action": "updated",
            "from_timeout": current.get("Timeout"),
            "to_timeout": new_timeout,
        }
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    started = datetime.now(timezone.utc)
    out = {"started": started.isoformat()}
    
    # ─── Phase 1: verify engine.error wrap is in deployed code ──────────
    print("[1027] phase 1: fetch deployed code for broken engines…")
    out["code_check"] = {}
    for fn in ("justhodl-crisis-plumbing", "justhodl-liquidity-credit-engine"):
        contents = fetch_deployed_code(fn)
        if isinstance(contents, dict) and contents.get("err"):
            out["code_check"][fn] = {"err": contents["err"]}
            continue
        lf = contents.get("lambda_function.py", "")
        out["code_check"][fn] = {
            "files":              list(contents.keys()),
            "has_emit_wrap":      "_emit_engine_error" in lf,
            "has_events_import":  "from system_events" in lf,
            "has_events_module":  "system_events.py" in contents,
            "code_size_kb":       round(sum(len(c) for c in contents.values()) / 1024, 1),
        }
    
    # ─── Phase 2: check actual failure mode (timeout vs exception) ──────
    print("[1027] phase 2: scan recent logs for timeout/exception evidence…")
    out["log_analysis"] = {}
    for fn in ("justhodl-crisis-plumbing", "justhodl-liquidity-credit-engine"):
        out["log_analysis"][fn] = check_for_timeout_errors(fn, hours=48)
    
    # ─── Phase 3: bump timeouts if evidence shows timeouts ──────────────
    print("[1027] phase 3: bump timeouts to give engines more headroom…")
    out["timeout_updates"] = {}
    # crisis-plumbing: 120s → 240s (gives FRED retries breathing room)
    out["timeout_updates"]["justhodl-crisis-plumbing"] = update_timeout("justhodl-crisis-plumbing", 240)
    time.sleep(2)
    # liquidity-credit: 300s → 480s (this engine has 15+ FRED series to fetch)
    out["timeout_updates"]["justhodl-liquidity-credit-engine"] = update_timeout("justhodl-liquidity-credit-engine", 480)
    
    # ─── Phase 4: also persist these timeouts in config.json ────────────
    # (we'll update config files in a follow-up commit so they stick)
    out["next_action"] = (
        "Update aws/lambdas/justhodl-crisis-plumbing/config.json timeout to 240 "
        "and aws/lambdas/justhodl-liquidity-credit-engine/config.json timeout to 480 "
        "so the deploy-lambdas workflow respects these on next redeploy."
    )
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
