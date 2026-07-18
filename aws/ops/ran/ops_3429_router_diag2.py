"""ops 3429 — where did the rotation write? Context output_keys + candidate
key ages + router's own last-run tail (how many OK / timeout?)."""
import json, sys
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report
S3C=boto3.client("s3","us-east-1"); LOGS=boto3.client("logs","us-east-1")
LAM=boto3.client("lambda","us-east-1")
with report("3429_router_diag2") as rep:
    reg=json.loads(S3C.get_object(Bucket="justhodl-dashboard-live",Key="config/ai-brief-contexts.json")["Body"].read())
    cx=reg.get("contexts") or {}
    rc=cx.get("regime-decisive-call") or {}
    line1="regime ctx cfg keys="+json.dumps({k:str(v)[:60] for k,v in rc.items()})[:360]; print(line1); rep.log(line1)
    now=datetime.now(timezone.utc)
    for key in ("data/regime-decisive-call.json","data/aibrief/regime-decisive-call.json",
                f"data/{rc.get('output_key','?')}.json" if rc.get("output_key") else "data/none.json"):
        try:
            o=S3C.head_object(Bucket="justhodl-dashboard-live",Key=key)
            line=f"{key} age_h={round((now-o['LastModified']).total_seconds()/3600,1)}"
        except Exception as e: line=f"{key} ERR {str(e)[:50]}"
        print(line); rep.log(line)
    cfg=LAM.get_function_configuration(FunctionName="justhodl-ai-brief-router")
    line=f"router timeout={cfg.get('Timeout')}s mem={cfg.get('MemorySize')}"; print(line); rep.log(line)
    st=LOGS.describe_log_streams(logGroupName="/aws/lambda/justhodl-ai-brief-router",orderBy="LastEventTime",descending=True,limit=2).get("logStreams",[])
    for s in st[:2]:
        ev=LOGS.get_log_events(logGroupName="/aws/lambda/justhodl-ai-brief-router",logStreamName=s["logStreamName"],limit=60,startFromHead=False).get("events",[])
        keep=[e["message"].strip()[:130] for e in ev if "done:" in e["message"] or "Task timed out" in e["message"] or "running" in e["message"] or "ERR" in e["message"]][-6:]
        for k in keep: print(k); rep.log(k)
    Path("aws/ops/reports/3429.json").write_text("{}"); sys.exit(0)
