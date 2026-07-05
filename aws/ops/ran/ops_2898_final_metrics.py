"""ops 2898 — definitive metrics closer: self-wait for ka/khalid CodeSha flip (new router bundled),
async scheduled-payload invoke, poll analysis ages, fleet census, router graceful-path proof."""
import os, json, time, traceback, boto3
from datetime import datetime, timezone
REGION="us-east-1"; B="justhodl-dashboard-live"
R={"ops":2898,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
logs=boto3.client("logs",region_name=REGION)
OLD={"justhodl-khalid-metrics":"oz0wo81tRCYz","justhodl-ka-metrics":"EipcuagAxXE7"}
KEYS={"justhodl-khalid-metrics":"data/khalid-analysis.json","justhodl-ka-metrics":"data/ka-analysis.json"}
EVT=json.dumps({"source":"aws.events"}).encode()
def age(k):
    try:
        h=s3.head_object(Bucket=B,Key=k)
        return round((datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600,1)
    except Exception: return "missing"
def sha(fn): return lam.get_function_configuration(FunctionName=fn).get("CodeSha256","")[:12]
try:
    # 1. wait for both bundles to flip (deploy loop still running)
    deadline=time.time()+430; flips={}
    while time.time()<deadline:
        flips={fn:(sha(fn)!=old) for fn,old in OLD.items()}
        if all(flips.values()): break
        time.sleep(15)
    R["sha_flipped"]=flips
    # 2. fleet census
    n=0; tot=0
    p=lam.get_paginator("list_functions")
    for pg in p.paginate():
        for f in pg["Functions"]:
            tot+=1
            if f["LastModified"][:16]>="2026-07-05T17:45": n+=1
    R["fleet_redeployed_since_fix"]={"n":n,"total":tot}
    # 3. invoke + age-poll (only if flipped; else record)
    for fn,key in KEYS.items():
        R[fn]={"before_h":age(key),"flipped":flips.get(fn)}
        if flips.get(fn):
            lam.invoke(FunctionName=fn,InvocationType="Event",Payload=EVT)
            R[fn]["invoked"]="async"
    for _ in range(22):
        time.sleep(14)
        if all((not flips.get(fn)) or (isinstance(age(k),float) and age(k)<1.0) for fn,k in KEYS.items()): break
    for fn,key in KEYS.items(): R[fn]["after_h"]=age(key)
    # 4. router graceful-path proof from brief-router (flipped earlier)
    try:
        st=logs.describe_log_streams(logGroupName="/aws/lambda/justhodl-ai-brief-router",orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
        evs=logs.get_log_events(logGroupName="/aws/lambda/justhodl-ai-brief-router",logStreamName=st[0]["logStreamName"],limit=25,startFromHead=False)["events"]
        msgs=" | ".join(e["message"].strip()[:90] for e in evs)
        R["router_graceful_seen"]={"circuit_open":("circuit open" in msgs),"all_down_empty":("ALL providers down" in msgs)}
    except Exception as e: R["router_graceful_seen"]="tail-err:"+str(e)[:60]
    ok=all(flips.values()) and all(isinstance(R[fn]["after_h"],float) and R[fn]["after_h"]<1.0 for fn in KEYS)
    R["status"]="CLOSED" if ok else "PARTIAL"
except Exception:
    R["errors"]["main"]=traceback.format_exc()[-400:]; R["status"]="FAILED"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2400])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2898_final_metrics.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2898 COMPLETE")
