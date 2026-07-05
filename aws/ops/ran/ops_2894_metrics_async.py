"""ops 2894 — ka/khalid-metrics done RIGHT: resize (idempotent) + ASYNC scheduled-payload invoke + poll output ages."""
import os, json, time, traceback, boto3
from datetime import datetime, timezone
REGION="us-east-1"; B="justhodl-dashboard-live"
R={"ops":2894,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
EVT=json.dumps({"source":"aws.events"}).encode()
def age(k):
    try:
        h=s3.head_object(Bucket=B,Key=k)
        return round((datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600,1)
    except Exception: return "missing"
JOBS=[("justhodl-khalid-metrics","data/khalid-analysis.json"),
      ("justhodl-ka-metrics","data/ka-analysis.json")]
try:
    for fn,key in JOBS:
        c=lam.get_function_configuration(FunctionName=fn)
        R[fn]={"was":{"t":c.get("Timeout"),"m":c.get("MemorySize")},"before_h":age(key)}
        if c.get("Timeout")!=300 or c.get("MemorySize")!=512:
            lam.update_function_configuration(FunctionName=fn,Timeout=300,MemorySize=512)
            for _ in range(30):
                if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful": break
                time.sleep(4)
        lam.invoke(FunctionName=fn,InvocationType="Event",Payload=EVT)
        R[fn]["invoked"]="async"
    for _ in range(20):
        time.sleep(15)
        if all(isinstance(age(k),float) and age(k)<1.5 for _,k in JOBS): break
    for fn,key in JOBS:
        R[fn]["after_h"]=age(key)
        c=lam.get_function_configuration(FunctionName=fn)
        R[fn]["now"]={"t":c.get("Timeout"),"m":c.get("MemorySize")}
    R["status"]="OK" if all(isinstance(R[f]["after_h"],float) and R[f]["after_h"]<1.5 for f,_ in JOBS) else "PARTIAL"
except Exception:
    R["errors"]["main"]=traceback.format_exc()[-400:]; R["status"]="FAILED"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:1800])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2894_metrics_async.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2894 COMPLETE")
