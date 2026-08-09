"""ops 4548 — READ-ONLY investigation (Khalid: check if extraction is
still advancing + why FRED/ECB/Polygon/Yahoo don't have "all the data").
Walker states, EventBridge rule states, recent errors, and — separately —
which engine actually feeds each of the four flagged providers, its own
schedule, and its last real invoke. No writes except the report."""
import json,os,time
from datetime import datetime,timezone
import boto3
s3=boto3.client("s3",region_name="us-east-1"); B="justhodl-dashboard-live"
lam=boto3.client("lambda",region_name="us-east-1")
ev=boto3.client("events",region_name="us-east-1")
logs=boto3.client("logs",region_name="us-east-1")
now=datetime.now(timezone.utc)
R={"ops":4548,"at":now.isoformat(),"mode":"READ-ONLY"}

def age_m(k):
    try:
        h=s3.head_object(Bucket=B,Key=k)
        return round((now-h["LastModified"]).total_seconds()/60,1)
    except Exception as e: return f"ERR {str(e)[:40]}"

# 1) SDMX walker states — is it genuinely advancing?
W={}
for ag,tgt in (("eurostat",8146),("oecd",1542),("statcan",6335),("bis",29)):
    try:
        st=json.loads(s3.get_object(Bucket=B,Key=f"data/_state/sdmx-walk-{ag}.json")["Body"].read())
        W[ag]={"done":len(st.get("done") or []),"target_known":tgt,
               "lease_active":bool((st.get("lease_until") or 0)>time.time()),
               "lease_until_s_from_now":round((st.get("lease_until") or 0)-time.time(),1),
               "state_age_min":age_m(f"data/_state/sdmx-walk-{ag}.json"),
               "recent_failures":len(st.get("failures") or {})}
    except Exception as e: W[ag]=str(e)[:60]
R["walkers"]=W
# rule + last invocations for the walker itself
try:
    c=lam.get_function_configuration(FunctionName="justhodl-sdmx-walker")
    arn=c["FunctionArn"]
    rules=ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames",[])
    R["walker_rules"]=[{"name":r,**{k:ev.describe_rule(Name=r).get(k) for k in ("ScheduleExpression","State")}} for r in rules]
except Exception as e: R["walker_rules_err"]=str(e)[:80]
try:
    le=logs.filter_log_events(logGroupName="/aws/lambda/justhodl-sdmx-walker",limit=15)
    R["walker_recent_log_tail"]=[e["message"].strip()[:140] for e in le.get("events",[])[-6:]]
except Exception as e: R["walker_log_err"]=str(e)[:60]

# 2) hub-reported numbers RIGHT NOW (what the page shows)
try:
    hub=json.loads(s3.get_object(Bucket=B,Key="data/provider-catalog.json")["Body"].read())
    R["hub_as_of"]=hub.get("as_of"); R["hub_totals"]=hub.get("totals")
    R["hub_breakdown"]=hub.get("breakdown")
    for slug in ("eurostat","oecd","statcan","bis","fred","ecb","polygon","yahoo"):
        row=next((p for p in hub["providers"] if p["slug"]==slug),{})
        R.setdefault("hub_rows",{})[slug]={k:row.get(k) for k in ("datasets","datasets_target","coverage_pct","n_keys","total_mb","freshest_h")}
except Exception as e: R["hub_err"]=str(e)[:80]

# 3) trace FRED/ECB/Polygon/Yahoo to their REAL producing engines
TRACE={"fred":["justhodl-canary-macro"],
       "ecb":["justhodl-ecb-catalog"],
       "polygon":["justhodl-polygon-daily"],
       "yahoo":["justhodl-yahoo-proxy","justhodl-tradingview"]}
for slug,fns in TRACE.items():
    for fn in fns:
        try:
            c=lam.get_function_configuration(FunctionName=fn)
            arn=c["FunctionArn"]
            rules=ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames",[])
            rinfo=[{"name":r,"sched":ev.describe_rule(Name=r).get("ScheduleExpression"),
                    "state":ev.describe_rule(Name=r).get("State")} for r in rules]
            R.setdefault("engine_trace",{})[fn]={
                "exists":True,"last_modified":str(c.get("LastModified")),
                "rules":rinfo}
        except lam.exceptions.ResourceNotFoundException:
            R.setdefault("engine_trace",{})[fn]={"exists":False}
        except Exception as e:
            R.setdefault("engine_trace",{})[fn]={"err":str(e)[:70]}

# 4) recent errors on each traced engine (last 10 log lines w/ ERROR)
for fn in ("justhodl-ecb-catalog","justhodl-polygon-daily"):
    try:
        le=logs.filter_log_events(logGroupName=f"/aws/lambda/{fn}",
            filterPattern="ERROR",limit=8)
        R.setdefault("engine_errors",{})[fn]=[e["message"].strip()[:140] for e in le.get("events",[])[-4:]]
    except Exception as e:
        R.setdefault("engine_errors",{})[fn]=f"ERR {str(e)[:60]}"

os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4548_investigate.json","w"),indent=1,default=str)
open("aws/ops/reports/4548_investigate.md","w").write(
 "# 4548 READ-ONLY investigation\n"
 "## walkers\n"+json.dumps(R["walkers"],default=str)+"\n"
 "## walker_rules\n"+json.dumps(R.get("walker_rules"),default=str)+"\n"
 "## walker_log_tail\n"+json.dumps(R.get("walker_recent_log_tail"),default=str)+"\n"
 "## hub\n"+json.dumps({"as_of":R.get("hub_as_of"),"totals":R.get("hub_totals"),"breakdown":R.get("hub_breakdown")},default=str)+"\n"
 "## hub_rows\n"+json.dumps(R.get("hub_rows"),default=str)+"\n"
 "## engine_trace\n"+json.dumps(R.get("engine_trace"),default=str)+"\n"
 "## engine_errors\n"+json.dumps(R.get("engine_errors"),default=str)+"\n")
print(json.dumps({"walkers":R["walkers"],"hub_totals":R.get("hub_totals")},default=str)[:500])
