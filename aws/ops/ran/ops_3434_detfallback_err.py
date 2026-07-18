"""ops 3434 — det-fallback error lines + ERR_EXC assignment context."""
import json, sys
from pathlib import Path
import boto3
from ops_report import report
LOGS=boto3.client("logs","us-east-1")
with report("3434_detfallback_err") as rep:
    st=LOGS.describe_log_streams(logGroupName="/aws/lambda/justhodl-ai-brief-router",orderBy="LastEventTime",descending=True,limit=3).get("logStreams",[])
    n=0
    for s2 in st:
        ev=LOGS.get_log_events(logGroupName="/aws/lambda/justhodl-ai-brief-router",logStreamName=s2["logStreamName"],limit=300,startFromHead=False).get("events",[])
        for e in ev:
            m=e["message"].strip()
            if "det-fallback" in m or "ERR_EXC" in m and "Traceback" in m:
                print(m[:200]); rep.log(m[:200]); n+=1
        if n>=8: break
    if n==0: print("no det-fallback prints found"); rep.log("none found")
    Path("aws/ops/reports/3434.json").write_text("{}"); sys.exit(0)
