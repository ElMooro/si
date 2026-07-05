"""ops 2893 — last mile: ka/khalid-metrics 300s/512MB + scheduled-payload run (analyses refresh);
schedule the 8 stale-unique periphery engines (staggered dailies) + fire once."""
import os, json, time, traceback, boto3
from datetime import datetime, timezone
from botocore.config import Config
REGION="us-east-1"; ACCT="857687956942"; B="justhodl-dashboard-live"
R={"ops":2893,"ts":datetime.now(timezone.utc).isoformat(),"errors":{}}
def guard(n):
    def d(f):
        def r(*a,**k):
            try: return f(*a,**k)
            except Exception:
                R["errors"][n]=traceback.format_exc()[-380:]; return None
        return r
    return d
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=320,retries={"max_attempts":0}))
ev=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
EVT=json.dumps({"source":"aws.events"}).encode()
def age(k):
    try:
        h=s3.head_object(Bucket=B,Key=k)
        return round((datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600,1)
    except Exception: return "missing"
def wait_ok(fn):
    for _ in range(30):
        if lam.get_function_configuration(FunctionName=fn).get("LastUpdateStatus")=="Successful": return
        time.sleep(4)

@guard("metrics_resize_run")
def metrics_resize_run():
    out={}
    for fn,key in (("justhodl-khalid-metrics","data/khalid-analysis.json"),
                   ("justhodl-ka-metrics","data/ka-analysis.json")):
        c=lam.get_function_configuration(FunctionName=fn)
        rec={"was":{"t":c.get("Timeout"),"m":c.get("MemorySize")},"before_h":age(key)}
        lam.update_function_configuration(FunctionName=fn,Timeout=300,MemorySize=512); wait_ok(fn)
        p=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=EVT)
        rec["fn_error"]=p.get("FunctionError"); rec["resp"]=p["Payload"].read().decode()[:110]
        time.sleep(3); rec["after_h"]=age(key)
        out[fn]=rec
    R["metrics"]=out
    return True

@guard("periphery_schedules")
def periphery_schedules():
    plan=[("justhodl-buyback-yield-ranking","cron(2 14 * * ? *)"),
          ("justhodl-capital-inflows","cron(6 14 * * ? *)"),
          ("justhodl-cta-trend-exhaust","cron(10 14 * * ? *)"),
          ("justhodl-divcut-warning","cron(14 14 * * ? *)"),
          ("justhodl-earnings-quality","cron(18 14 * * ? *)"),
          ("justhodl-gap-fill-confirm","cron(22 14 * * ? *)"),
          ("justhodl-reit-nav-discount","cron(26 14 * * ? *)"),
          ("justhodl-spac-floor-warrant","cron(30 14 * * ? *)")]
    out={}
    for fn,cron in plan:
        rule=fn.replace("justhodl-","jh-")+"-daily"
        try:
            ev.put_rule(Name=rule,ScheduleExpression=cron,State="ENABLED",Description="periphery gap-fix 2893")
            try: lam.add_permission(FunctionName=fn,StatementId="sched2893",Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",SourceArn="arn:aws:events:%s:%s:rule/%s"%(REGION,ACCT,rule))
            except Exception as e:
                if "ResourceConflict" not in str(e): raise
            ev.put_targets(Rule=rule,Targets=[{"Id":"1","Arn":"arn:aws:lambda:%s:%s:function:%s"%(REGION,ACCT,fn)}])
            lam.invoke(FunctionName=fn,InvocationType="Event")
            out[fn]={"rule":rule,"cron":cron,"fired":True}
        except Exception as e: out[fn]="err:"+str(e)[:90]
    R["periphery"]=out
    return True

metrics_resize_run(); periphery_schedules()
R["status"]="OK" if not R["errors"] else "PARTIAL"
print(json.dumps(R,ensure_ascii=False,indent=1,default=str)[:2600])
os.makedirs("aws/ops/reports",exist_ok=True); json.dump(R,open("aws/ops/reports/2893_last_mile.json","w"),ensure_ascii=False,indent=1,default=str)
print("OPS 2893 COMPLETE")
