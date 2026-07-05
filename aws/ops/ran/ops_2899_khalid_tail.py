"""ops 2899 — decisive post-flip tail of khalid-metrics (classify: gated-write vs still-failing)."""
import os, json, boto3, traceback
from datetime import datetime, timezone
logs=boto3.client("logs",region_name="us-east-1")
R={"ops":2899,"ts":datetime.now(timezone.utc).isoformat()}
try:
    st=logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-khalid-metrics",orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
    evs=logs.get_log_events(logGroupName="/aws/lambda/justhodl-khalid-metrics",logStreamName=st[0]["logStreamName"],limit=22,startFromHead=False)["events"]
    R["tail"]=[e["message"].strip()[:180] for e in evs][-16:]
except Exception:
    R["tail"]=[traceback.format_exc()[-200:]]
print(json.dumps(R,ensure_ascii=False,indent=1)[:2400])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2899_khalid_tail.json","w"),ensure_ascii=False,indent=1)
print("OPS 2899 COMPLETE")
