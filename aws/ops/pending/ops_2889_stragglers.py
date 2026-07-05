"""ops 2889 — close the 3 straggler stale feeds: invoke owners with error capture + fresh-age check."""
import os, json, time, traceback, boto3
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; B="justhodl-dashboard-live"
R={"ops":2889,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); logs=boto3.client("logs",region_name=REGION)
def tail(fn,n=8):
    try:
        st=logs.describe_log_streams(logGroupName="/aws/lambda/"+fn,orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
        evs=logs.get_log_events(logGroupName="/aws/lambda/"+fn,logStreamName=st[0]["logStreamName"],limit=n,startFromHead=False)["events"]
        return [e["message"].strip()[:170] for e in evs]
    except Exception as e: return ["tail-err:"+str(e)[:60]]
def age(k):
    try:
        h=s3.head_object(Bucket=B,Key=k)
        return round((datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600,1)
    except Exception: return "missing"
JOBS=[("justhodl-factor-decomposition","data/factor-data-cache.json","sync"),
      ("justhodl-history-snapshotter","data/history-index.json","sync"),
      ("justhodl-alert-backtester","data/spx-history-deep.json","async")]
for fn,key,mode in JOBS:
    rec={"before_h":age(key)}
    try:
        if mode=="sync":
            p=lam.invoke(FunctionName=fn,InvocationType="RequestResponse")
            rec["fn_error"]=p.get("FunctionError"); rec["resp"]=p["Payload"].read().decode()[:150]
        else:
            lam.invoke(FunctionName=fn,InvocationType="Event"); rec["invoked"]="async"
    except Exception as e: rec["invoke_err"]=str(e)[:100]
    R[fn]=rec
# poll ages up to ~150s for the writes to land (backtester heavy)
for _ in range(10):
    time.sleep(15)
    if all(isinstance(age(k),float) and age(k)<2 for _,k,_ in JOBS): break
for fn,key,_ in JOBS:
    R[fn]["after_h"]=age(key)
    if not (isinstance(R[fn]["after_h"],float) and R[fn]["after_h"]<2):
        R[fn]["tail"]=tail(fn)
R["status"]="OK" if all(isinstance(R[f]["after_h"],float) and R[f]["after_h"]<2 for f,_,_ in JOBS) else "PARTIAL"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:3000])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2889_stragglers.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2889 COMPLETE")
