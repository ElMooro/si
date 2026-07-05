"""ops 2908 — final ka/khalid healing proof: wait deploy, fire scheduled Events, poll ages, success tail."""
import os, json, time, boto3, traceback
from datetime import datetime, timezone
REGION="us-east-1"; B="justhodl-dashboard-live"
lam=boto3.client("lambda",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
logs=boto3.client("logs",region_name=REGION)
R={"ops":2908,"ts":datetime.now(timezone.utc).isoformat()}
PUSH=datetime.now(timezone.utc)
def age(k):
    try:
        h=s3.head_object(Bucket=B,Key=k)
        return round((datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600,2)
    except Exception: return "missing"
try:
    for fn in ("justhodl-khalid-metrics","justhodl-ka-metrics"):
        for _ in range(45):
            c=lam.get_function_configuration(FunctionName=fn)
            lm=datetime.fromisoformat(c["LastModified"].replace("+0000","+00:00"))
            if lm>PUSH and c.get("LastUpdateStatus")=="Successful": break
            time.sleep(6)
        lam.invoke(FunctionName=fn,InvocationType="Event",Payload=json.dumps({"source":"aws.events"}).encode())
    R["before"]={"khalid":age("data/khalid-analysis.json"),"ka":age("data/ka-analysis.json")}
    for _ in range(30):
        time.sleep(12)
        a1,a2=age("data/khalid-analysis.json"),age("data/ka-analysis.json")
        if isinstance(a1,float) and a1<0.4 and isinstance(a2,float) and a2<0.4: break
    R["after"]={"khalid":age("data/khalid-analysis.json"),"ka":age("data/ka-analysis.json")}
    tails={}
    for fn in ("justhodl-khalid-metrics","justhodl-ka-metrics"):
        try:
            stm=logs.describe_log_streams(logGroupName="/aws/lambda/"+fn,orderBy="LastEventTime",descending=True,limit=1)["logStreams"]
            evs=logs.get_log_events(logGroupName="/aws/lambda/"+fn,logStreamName=stm[0]["logStreamName"],limit=16,startFromHead=False)["events"]
            msgs=[e["message"].strip()[:150] for e in evs]
            tails[fn]=[m for m in msgs if any(k in m for k in ("AI:","grade","Error","Timeout","404","REPORT"))][-5:]
        except Exception as e: tails[fn]=[str(e)[:80]]
    R["tails"]=tails
    ok=isinstance(R["after"]["khalid"],float) and R["after"]["khalid"]<0.4 and isinstance(R["after"]["ka"],float) and R["after"]["ka"]<0.4
    R["HEALED"]=ok; R["status"]="OK"
except Exception:
    R["error"]=traceback.format_exc()[-400:]; R["status"]="FAILED"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2908_metrics_final.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2908 COMPLETE")
