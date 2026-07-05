"""ops 2903 — diag: equity-research latest run tail (v2 module errors) + re-fire ka/khalid with long poll."""
import os, json, time, boto3, traceback
from datetime import datetime, timezone
logs=boto3.client("logs",region_name="us-east-1"); lam=boto3.client("lambda",region_name="us-east-1")
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
R={"ops":2903,"ts":datetime.now(timezone.utc).isoformat()}
try:
    st=logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-equity-research",orderBy="LastEventTime",descending=True,limit=2)["logStreams"]
    lines=[]
    for s_ in st:
        evs=logs.get_log_events(logGroupName="/aws/lambda/justhodl-equity-research",logStreamName=s_["logStreamName"],limit=60,startFromHead=False)["events"]
        lines+= [e["message"].rstrip()[:200] for e in evs]
    R["research_tail"]=[l for l in lines if any(k in l for k in ("crash","Error","Trace","v2","prices_full","spy_light","technic","fetch_all","backlog","name '"))][-22:] or lines[-14:]
except Exception: R["research_tail"]=[traceback.format_exc()[-200:]]
EVT=json.dumps({"source":"aws.events"}).encode()
def age(k):
    try:
        h=s3.head_object(Bucket=B,Key=k)
        return round((datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600,2)
    except Exception: return "missing"
for fn,key in (("justhodl-khalid-metrics","data/khalid-analysis.json"),("justhodl-ka-metrics","data/ka-analysis.json")):
    lam.invoke(FunctionName=fn,InvocationType="Event",Payload=EVT)
R["metrics_before"]={"khalid":age("data/khalid-analysis.json"),"ka":age("data/ka-analysis.json")}
for _ in range(24):
    time.sleep(12)
    a1,a2=age("data/khalid-analysis.json"),age("data/ka-analysis.json")
    if isinstance(a1,float) and a1<0.5 and isinstance(a2,float) and a2<0.5: break
R["metrics_after"]={"khalid":age("data/khalid-analysis.json"),"ka":age("data/ka-analysis.json")}
try:
    st=logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-khalid-metrics",orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
    evs=logs.get_log_events(logGroupName="/aws/lambda/justhodl-khalid-metrics",logStreamName=st[0]["logStreamName"],limit=14,startFromHead=False)["events"]
    R["khalid_tail"]=[e["message"].strip()[:170] for e in evs][-9:]
except Exception as e: R["khalid_tail"]=[str(e)[:100]]
R["status"]="OK"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3200])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2903_diag.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2903 COMPLETE")
