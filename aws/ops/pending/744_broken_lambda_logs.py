"""ops/744 — diagnostic: pull recent CloudWatch errors for the 4 broken Lambdas.

For each failing function, scans the last 4 days of its log group for
error indicators (tracebacks, [ERROR], errorMessage, timeouts) and
returns the most recent distinct error blocks so the root cause is
visible without guessing.
"""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

cfg = Config(retries={"max_attempts": 3})
logs = boto3.client("logs", region_name="us-east-1", config=cfg)

report = {"ops": 744, "ts": datetime.now(timezone.utc).isoformat(),
          "subject": "CloudWatch error diagnostics — 4 broken Lambdas"}

TARGETS = [
    "justhodl-morning-intelligence",
    "news-sentiment-agent",
    "justhodl-email-reports-v2",
    "justhodl-nobrainer-rationale",
]

ERR_HINTS = ("Traceback", "[ERROR]", "errorMessage", "errorType",
             "Task timed out", "Runtime.", "Unable to import",
             "ModuleNotFoundError", "Syntax", "KeyError", "NameError",
             "AttributeError", "TypeError", "ValueError", "ImportError",
             "Exception", "ERROR ")

start_ms = int((time.time() - 4 * 86400) * 1000)
diagnostics = {}

for fn in TARGETS:
    lg = f"/aws/lambda/{fn}"
    entry = {"log_group": lg}
    try:
        events = []
        token = None
        pages = 0
        while pages < 5:
            kw = dict(logGroupName=lg, startTime=start_ms, limit=400,
                      interleaved=True)
            if token:
                kw["nextToken"] = token
            resp = logs.filter_log_events(**kw)
            events.extend(resp.get("events", []))
            token = resp.get("nextToken")
            pages += 1
            if not token:
                break
        # keep only error-ish lines, most recent last
        errs = [e for e in events
                if any(h in (e.get("message") or "") for h in ERR_HINTS)]
        errs.sort(key=lambda e: e.get("timestamp", 0))
        # take the most recent ~25 error lines, trimmed
        recent = errs[-25:]
        entry["total_events_scanned"] = len(events)
        entry["error_lines"] = len(errs)
        entry["recent_errors"] = [
            {"t": datetime.fromtimestamp(e["timestamp"] / 1000,
                                         timezone.utc).isoformat(),
             "msg": (e.get("message") or "").strip()[:600]}
            for e in recent]
    except logs.exceptions.ResourceNotFoundException:
        entry["error"] = "log group not found"
    except Exception as e:
        entry["error"] = str(e)[:240]
    diagnostics[fn] = entry

report["diagnostics"] = diagnostics
print(json.dumps(report, indent=2, default=str))
os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/744_broken_lambda_logs.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print("[ok] wrote aws/ops/reports/744_broken_lambda_logs.json")
