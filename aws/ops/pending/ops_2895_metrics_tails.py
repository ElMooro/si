"""ops 2895 — evidence: fresh log tails for ka/khalid-metrics post-invoke."""
import os, json, boto3, traceback
from datetime import datetime, timezone
logs=boto3.client("logs",region_name="us-east-1")
R={"ops":2895,"ts":datetime.now(timezone.utc).isoformat()}
for fn in ("justhodl-khalid-metrics","justhodl-ka-metrics"):
    try:
        st=logs.describe_log_streams(logGroupName="/aws/lambda/"+fn,orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
        evs=logs.get_log_events(logGroupName="/aws/lambda/"+fn,logStreamName=st[0]["logStreamName"],limit=16,startFromHead=False)["events"]
        R[fn]=[e["message"].strip()[:180] for e in evs][-13:]
    except Exception as e: R[fn]=["tail-err:"+str(e)[:80]]
print(json.dumps(R,ensure_ascii=False,indent=1)[:2600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2895_metrics_tails.json","w"),ensure_ascii=False,indent=1)
print("OPS 2895 COMPLETE")
