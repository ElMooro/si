"""ops 3430 — the 47 failures: per-context status lines from newest stream."""
import json, sys
from pathlib import Path
import boto3
from ops_report import report
LOGS=boto3.client("logs","us-east-1")
with report("3430_router_errs") as rep:
    st=LOGS.describe_log_streams(logGroupName="/aws/lambda/justhodl-ai-brief-router",orderBy="LastEventTime",descending=True,limit=3).get("logStreams",[])
    seen=0
    for s in st:
        ev=LOGS.get_log_events(logGroupName="/aws/lambda/justhodl-ai-brief-router",logStreamName=s["logStreamName"],limit=200,startFromHead=False).get("events",[])
        for e in ev:
            m=e["message"].strip()
            if "]: " not in m and ": ERR" not in m and ": OK" not in m: continue
            if "[ai-brief-router]" in m and (": ERR" in m or ": OK" in m):
                print(m[:180]); rep.log(m[:180]); seen+=1
        if seen>20: break
    Path("aws/ops/reports/3430.json").write_text("{}"); sys.exit(0)
