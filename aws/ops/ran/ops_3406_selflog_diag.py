"""ops 3406 — one-shot: exact self-log line from best-setups CW."""
import json, sys, time
from pathlib import Path
import boto3
from ops_report import report
LOGS=boto3.client("logs","us-east-1")
with report("3406_selflog_diag") as rep:
    rep.heading("ops 3406 — self-log line")
    ev=LOGS.filter_log_events(logGroupName="/aws/lambda/justhodl-best-setups",
        startTime=int((time.time()-3000)*1000), filterPattern='"self-log"')
    lines=[e["message"].strip()[:300] for e in ev.get("events",[])][-8:]
    print("LINES:"); [print("  ",l) for l in lines]
    if not lines:
        ev=LOGS.filter_log_events(logGroupName="/aws/lambda/justhodl-best-setups",
            startTime=int((time.time()-3000)*1000), filterPattern='"3403"')
        lines=[e["message"].strip()[:300] for e in ev.get("events",[])][-8:]
        print("FALLBACK 3403 LINES:"); [print("  ",l) for l in lines]
    rep.log(" || ".join(lines) or "NO MATCHES")
    Path("aws/ops/reports/3406.json").write_text(json.dumps({"lines":lines},indent=2))
    sys.exit(0)
