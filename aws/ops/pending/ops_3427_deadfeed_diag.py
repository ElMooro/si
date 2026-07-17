"""ops 3427 — dead decision-feed diagnosis: writer health (state, schedule,
last log event + tail) for the brief writers + regime-engine, and S3 shapes/
ages for the orphan feeds. Verdicts drive 3428+ fixes."""
import json, sys, time
from datetime import datetime, timezone
from pathlib import Path
import boto3
from ops_report import report
LAM=boto3.client("lambda","us-east-1"); S3C=boto3.client("s3","us-east-1")
EVB=boto3.client("events","us-east-1"); SCH=boto3.client("scheduler","us-east-1")
LOGS=boto3.client("logs","us-east-1")
FNS=["justhodl-ai-brief","justhodl-ai-brief-router","justhodl-regime-engine","justhodl-alert-backtester"]
def fn_diag(fn):
    d={"fn":fn}
    try:
        c=LAM.get_function_configuration(FunctionName=fn)
        d["ok"]=True; d["code_modified"]=c.get("LastModified","")[:16]
    except Exception as e:
        d["ok"]=False; d["err"]=str(e)[:60]; return d
    try:
        rules=[r["Name"] for r in EVB.list_rules(NamePrefix=fn).get("Rules",[])]
        d["classic_rules"]=rules[:3]
    except Exception: d["classic_rules"]=[]
    try:
        scs=[s["Name"] for s in SCH.list_schedules(NamePrefix=fn).get("Schedules",[])]
        d["schedules"]=scs[:3]
    except Exception: d["schedules"]=[]
    try:
        st=LOGS.describe_log_streams(logGroupName=f"/aws/lambda/{fn}",orderBy="LastEventTime",descending=True,limit=1).get("logStreams",[])
        if st:
            ts=st[0].get("lastEventTimestamp",0)/1000
            d["last_log"]=datetime.fromtimestamp(ts,tz=timezone.utc).isoformat()[:16]
            ev=LOGS.get_log_events(logGroupName=f"/aws/lambda/{fn}",logStreamName=st[0]["logStreamName"],limit=6,startFromHead=False).get("events",[])
            d["tail"]=[e["message"].strip()[:110] for e in ev][-4:]
        else: d["last_log"]="NO STREAMS"
    except Exception as e: d["last_log"]="logerr "+str(e)[:40]
    return d
def peek(key):
    try:
        o=S3C.get_object(Bucket="justhodl-dashboard-live",Key=key)
        j=json.loads(o["Body"].read())
        top=list(j.keys())[:8] if isinstance(j,dict) else f"LIST[{len(j)}]"
        return {"age_d":round((datetime.now(timezone.utc)-o["LastModified"]).days,1),"top":top,
                "gen":str(j.get("generated_at") if isinstance(j,dict) else "")[:16]}
    except Exception as e: return {"err":str(e)[:60]}
with report("3427_deadfeed_diag") as rep:
    rep.heading("ops 3427 — dead-feed diagnosis")
    out={"functions":{},"feeds":{}}
    for fn in FNS:
        out["functions"][fn]=fn_diag(fn)
        line=fn+" → "+json.dumps(out["functions"][fn])[:300]; print(line); rep.log(line)
    for k in ("data/regime-decisive-call.json","data/backtest-summary.json",
              "data/market-internals-history.json","data/finviz-signals-state.json","data/regime.json"):
        out["feeds"][k]=peek(k)
        line=k+" → "+json.dumps(out["feeds"][k])[:220]; print(line); rep.log(line)
    Path("aws/ops/reports/3427.json").write_text(json.dumps(out,indent=2)); sys.exit(0)
