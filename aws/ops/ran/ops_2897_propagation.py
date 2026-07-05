"""ops 2897 — verify router-fix propagation to transitive importers: wait for ka/khalid-metrics
CodeSha change, async EVT-invoke both, poll analyses ages; fleet redeploy census."""
import os, json, time, boto3, traceback
from datetime import datetime, timezone
lam=boto3.client("lambda",region_name="us-east-1"); s3=boto3.client("s3",region_name="us-east-1")
B="justhodl-dashboard-live"
R={"ops":2897,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
EVT=json.dumps({"source":"aws.events"}).encode()
def sha(fn):
    return lam.get_function_configuration(FunctionName=fn).get("CodeSha256","")[:12]
def age(k):
    try:
        h=s3.head_object(Bucket=B,Key=k)
        return round((datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600,1)
    except Exception: return "missing"
try:
    WATCH=["justhodl-khalid-metrics","justhodl-ka-metrics","justhodl-ai-brief-router","justhodl-consumer-pulse"]
    start={fn:sha(fn) for fn in WATCH}
    R["start_shas"]=start
    deadline=time.time()+900; flipped={}
    while time.time()<deadline:
        flipped={fn:(sha(fn)!=start[fn]) for fn in WATCH}
        if flipped["justhodl-khalid-metrics"] and flipped["justhodl-ka-metrics"]: break
        time.sleep(20)
    R["sha_flipped"]=flipped
    if flipped.get("justhodl-khalid-metrics") and flipped.get("justhodl-ka-metrics"):
        for fn in ("justhodl-khalid-metrics","justhodl-ka-metrics"):
            for _ in range(20):
                if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful": break
                time.sleep(4)
            lam.invoke(FunctionName=fn,InvocationType="Event",Payload=EVT)
        for _ in range(24):
            time.sleep(15)
            a1,a2=age("data/khalid-analysis.json"),age("data/ka-analysis.json")
            if isinstance(a1,float) and a1<1.5 and isinstance(a2,float) and a2<1.5: break
        R["analyses_ages_h"]={"khalid":age("data/khalid-analysis.json"),"ka":age("data/ka-analysis.json")}
    newer=0; total=0
    p=lam.get_paginator("list_functions")
    for pg in p.paginate():
        for f in pg["Functions"]:
            total+=1
            if f["LastModified"][:16]>="2026-07-05T17:45": newer+=1
    R["fleet_redeployed_since_fix"]={"n":newer,"total":total}
    a=R.get("analyses_ages_h",{})
    R["status"]="OK" if (isinstance(a.get("khalid"),float) and a["khalid"]<1.5 and isinstance(a.get("ka"),float) and a["ka"]<1.5) else "PARTIAL"
except Exception:
    R["errors"]["main"]=traceback.format_exc()[-400:]; R["status"]="FAILED"
print(json.dumps(R,indent=1,default=str)[:1800])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2897_propagation.json","w"),indent=1,default=str)
print("OPS 2897 COMPLETE")
