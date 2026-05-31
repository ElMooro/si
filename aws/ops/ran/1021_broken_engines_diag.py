#!/usr/bin/env python3
"""Step 1021 — Diagnose the 2 broken engines flagged by the audit.

  justhodl-liquidity-credit-engine  — 32% error rate, 63s avg duration
  justhodl-crisis-plumbing          — 21% error rate, 33 invokes/7d

Pull last error logs to identify the actual failure mode so we can fix.
"""
import json, os
from datetime import datetime, timedelta, timezone
import boto3

REPORT = "aws/ops/reports/1021_broken_engines_diag.json"
REGION = "us-east-1"
logs = boto3.client("logs", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)


def get_recent_errors(fn_name: str, hours: int = 48, max_messages: int = 20) -> list:
    """Read recent log streams + extract error/exception lines."""
    out = []
    try:
        lg = f"/aws/lambda/{fn_name}"
        # Get most recent log streams
        streams_resp = logs.describe_log_streams(
            logGroupName=lg, orderBy="LastEventTime",
            descending=True, limit=5,
        )
        for stream in streams_resp.get("logStreams", []):
            stream_name = stream["logStreamName"]
            try:
                ev = logs.get_log_events(
                    logGroupName=lg, logStreamName=stream_name,
                    limit=200, startFromHead=False,
                ).get("events", [])
                # Pick error-looking lines
                for e in ev:
                    msg = e.get("message", "").strip()
                    if any(token in msg for token in
                            ("ERROR", "Exception", "Traceback", "FAILED",
                             "errorType", "errorMessage", "RuntimeError", "[ERROR]",
                             "FAIL", "TIMEOUT", "Task timed out", "MemoryError")):
                        out.append({
                            "ts": datetime.fromtimestamp(e["timestamp"]/1000, tz=timezone.utc).isoformat(),
                            "stream": stream_name[-30:],
                            "msg": msg[:400],
                        })
                        if len(out) >= max_messages:
                            return out
            except Exception as ee:
                out.append({"stream_err": str(ee)[:100]})
    except Exception as e:
        return [{"err": str(e)[:200]}]
    return out


def get_function_config(fn_name: str) -> dict:
    try:
        meta = lam.get_function(FunctionName=fn_name)
        cfg = meta["Configuration"]
        return {
            "memory_mb":    cfg.get("MemorySize"),
            "timeout_s":    cfg.get("Timeout"),
            "runtime":      cfg.get("Runtime"),
            "last_modified": cfg.get("LastModified"),
            "description":  (cfg.get("Description") or "")[:200],
            "env":          list((cfg.get("Environment") or {}).get("Variables", {}).keys())[:15],
        }
    except Exception as e:
        return {"err": str(e)[:200]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    BROKEN = ["justhodl-liquidity-credit-engine", "justhodl-crisis-plumbing"]
    SLOW = ["justhodl-crypto-opportunities", "justhodl-outcome-checker"]
    
    for fn in BROKEN + SLOW:
        rec = {"config": get_function_config(fn)}
        rec["recent_errors"] = get_recent_errors(fn, hours=72, max_messages=15)
        out[fn] = rec
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
