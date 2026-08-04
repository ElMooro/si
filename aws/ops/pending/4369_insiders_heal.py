"""ops 4369 — insiders rail heal. insiders.html <- data/insider-trades.json
(writer justhodl-insider-trades, rule justhodl-insider-trades-30min);
insider/ <- data/insider-clusters.json (writer among cluster engines).
Diagnose feeds + schedule binding + logs, heal mechanically, force-invoke,
verify fresh writes with row counts."""
import json, os, re, time
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=300,retries={"max_attempts":0}))
ev=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
logs=boto3.client("logs",region_name=REGION)
t0=datetime.now(timezone.utc)
R={"ops":4369,"started":t0.isoformat(),"healed":[],"feeds":{}}

def head(key):
    try:
        h=s3.head_object(Bucket=BUCKET,Key=key)
        age=round((t0-h["LastModified"]).total_seconds()/3600,1)
        d=json.loads(s3.get_object(Bucket=BUCKET,Key=key)["Body"].read())
        rows=None
        for f in ("transactions","clusters","rows","items","data","trades"):
            if isinstance(d.get(f),list): rows=(f,len(d[f])); break
        return {"age_h":age,"kb":round(h["ContentLength"]/1024,1),
                "generated":d.get("generated_at") or d.get("ts") or d.get("updated"),
                "rows":rows}
    except Exception as e:
        return {"err":f"{type(e).__name__}: {str(e)[:80]}"}

R["feeds"]["insider-trades"]=head("data/insider-trades.json")
R["feeds"]["insider-clusters"]=head("data/insider-clusters.json")

def logsum(fn,hours=36):
    try:
        since=int((t0-timedelta(hours=hours)).timestamp()*1000)
        evs,tok=[],None
        while True:
            kw=dict(logGroupName=f"/aws/lambda/{fn}",startTime=since,limit=800)
            if tok: kw["nextToken"]=tok
            r2=logs.filter_log_events(**kw); evs+=r2.get("events",[])
            tok=r2.get("nextToken")
            if not tok or len(evs)>3000: break
        starts=[e for e in evs if "START RequestId" in e["message"]]
        sig={}
        for e2 in evs:
            m=e2["message"]
            if any(k in m for k in ("ERROR","Error","Traceback","errorMessage","Task timed out","S3 ERR")):
                sig[m.strip()[:130]]=sig.get(m.strip()[:130],0)+1
        return {"invocations":len(starts),
                "last":datetime.fromtimestamp(starts[-1]["timestamp"]/1000,timezone.utc).isoformat() if starts else None,
                "errors":dict(sorted(sig.items(),key=lambda kv:-kv[1])[:6])}
    except Exception as e:
        return {"err":str(e)[:100]}

R["engine_logs"]={"justhodl-insider-trades":logsum("justhodl-insider-trades")}
for fn in ("justhodl-insider-cluster-scanner","justhodl-insider-industry-cluster",
           "justhodl-insider-sell-cluster","justhodl-edgar-insiders"):
    R["engine_logs"][fn]=logsum(fn,hours=48)

# schedule binding for the one scheduled engine
RULE="justhodl-insider-trades-30min"; FN="justhodl-insider-trades"
sch={}
try:
    r=ev.describe_rule(Name=RULE); sch["state"]=r.get("State"); sch["expr"]=r.get("ScheduleExpression")
    tg=ev.list_targets_by_rule(Rule=RULE).get("Targets",[])
    sch["target_ok"]=any(FN in (x.get("Arn") or "") for x in tg)
    try:
        pol=json.loads(lam.get_policy(FunctionName=FN)["Policy"])
        sch["perm_ok"]=any(r["Arn"]==(s.get("Condition",{}).get("ArnLike",{}) or {}).get("AWS:SourceArn")
                           for s in pol.get("Statement",[]))
    except lam.exceptions.ResourceNotFoundException:
        sch["perm_ok"]=False
    if sch.get("state")=="DISABLED":
        ev.enable_rule(Name=RULE); R["healed"].append("rule ENABLED")
    if not sch.get("target_ok"):
        arn=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
        ev.put_targets(Rule=RULE,Targets=[{"Id":"insider-trades","Arn":arn}])
        R["healed"].append("target rebuilt")
    if sch.get("perm_ok") is False:
        try:
            lam.add_permission(FunctionName=FN,StatementId="ops4369-"+RULE,
                               Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com",SourceArn=r["Arn"])
            R["healed"].append("permission re-added")
        except lam.exceptions.ResourceConflictException: pass
except Exception as e:
    sch["err"]=str(e)[:120]
R["schedule"]=sch

# env sanity for the writer
try:
    cfg=lam.get_function_configuration(FunctionName=FN)
    vars=(cfg.get("Environment",{}) or {}).get("Variables",{}) or {}
    R["env"]={"state":cfg.get("State"),"keys":sorted(vars.keys()),
              "empty":[k for k in vars if vars[k]==""]}
except Exception as e:
    R["env"]={"err":str(e)[:100]}

# force invokes + re-head
def fire(fn):
    try:
        inv=lam.invoke(FunctionName=fn,InvocationType="RequestResponse",Payload=b"{}")
        body=inv["Payload"].read().decode()[:300]
        return {"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError"),"payload":body}
    except Exception as e:
        return {"err":str(e)[:120]}
R["invoke"]={"justhodl-insider-trades":fire(FN)}
cl=R["feeds"]["insider-clusters"]
if cl.get("err") or (isinstance(cl.get("age_h"),(int,float)) and cl["age_h"]>26):
    for fn in ("justhodl-insider-cluster-scanner","justhodl-insider-industry-cluster"):
        R["invoke"][fn]=fire(fn)
time.sleep(4)
R["feeds_after"]={"insider-trades":head("data/insider-trades.json"),
                  "insider-clusters":head("data/insider-clusters.json")}
ft=R["feeds_after"]["insider-trades"]
R["verdict"]=("HEALED — insider-trades fresh" if isinstance(ft.get("age_h"),(int,float)) and ft["age_h"]<0.2
              else "NOT WRITING — see engine_logs/invoke")
# fresh traceback if the invoke crashed
if R["invoke"]["justhodl-insider-trades"].get("fn_err"):
    try:
        r2=logs.filter_log_events(logGroupName=f"/aws/lambda/{FN}",
                                  startTime=int(t0.timestamp()*1000),limit=300)
        R["fresh_error_tail"]="".join(e["message"] for e in r2.get("events",[]))[-2500:]
    except Exception: pass
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4369_insiders_heal.json","w"),indent=1,default=str)
open("aws/ops/reports/4369_insiders_heal.md","w").write(
    f"# ops 4369 — insiders heal — {R['verdict']}\n"
    f"- feeds before: trades={json.dumps(R['feeds']['insider-trades'])} clusters={json.dumps(R['feeds']['insider-clusters'])}\n"
    f"- trades logs 36h: {json.dumps(R['engine_logs']['justhodl-insider-trades'])}\n"
    f"- schedule: {json.dumps(sch)} | healed: {R['healed'] or 'none needed'}\n"
    f"- env: {json.dumps(R['env'])}\n"
    f"- invokes: {json.dumps(R['invoke'])[:600]}\n"
    f"- feeds after: {json.dumps(R['feeds_after'])}\n")
print(json.dumps(R,indent=1,default=str))
