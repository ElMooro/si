"""ops 4370 — resurrect vanished EventBridge rules (07-31 14:55 wipe event).
1. Recreate justhodl-insider-trades-30min (rate parsed from name) + binding.
2. Fleet sweep: every rule named in every config.json -> describe; MISSING
   rules with name-parseable cadence (Nmin/Nh/daily/hourly) are recreated
   with full rule->target->permission binding (ops-1955 pattern); ambiguous
   names reported for manual cadence. 3. insider-clusters: re-head after the
   long scanner run; async-fire if still stale. Verify everything."""
import json, os, re, time, glob
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=60,retries={"max_attempts":1}))
ev=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
t0=datetime.now(timezone.utc)
R={"ops":4370,"started":t0.isoformat(),"recreated":[],"missing_ambiguous":[],
   "already_ok":0,"errors":[]}

def cadence_from_name(name):
    n=name.lower()
    m=re.search(r'(\d+)\s*min',n)
    if m: return f"rate({m.group(1)} minutes)" if m.group(1)!="1" else "rate(1 minute)"
    m=re.search(r'(\d+)\s*h(?:our|r)?\b',n) or re.search(r'-(\d+)h\b',n)
    if m: return f"rate({m.group(1)} hours)" if m.group(1)!="1" else "rate(1 hour)"
    if "hourly" in n: return "rate(1 hour)"
    if "daily" in n or "1d" in n: return "rate(1 day)"
    if "weekly" in n: return "rate(7 days)"
    return None

def bind(rule, expr, fn):
    arn=ev.put_rule(Name=rule,ScheduleExpression=expr,State="ENABLED",
                    Description=f"ops4370 resurrect (07-31 wipe): {fn}")["RuleArn"]
    fn_arn=lam.get_function_configuration(FunctionName=fn)["FunctionArn"]
    ev.put_targets(Rule=rule,Targets=[{"Id":fn[:60],"Arn":fn_arn}])
    try:
        lam.add_permission(FunctionName=fn,StatementId=("ops4370-"+rule)[:100],
                           Action="lambda:InvokeFunction",
                           Principal="events.amazonaws.com",SourceArn=arn)
    except lam.exceptions.ResourceConflictException: pass
    return arn

pairs=[]
for cfg in glob.glob("aws/lambdas/*/config.json"):
    try:
        c=json.load(open(cfg)); fn=c.get("function_name")
        for rule in (c.get("eventbridge_rules") or []):
            if fn and rule: pairs.append((rule,fn))
    except Exception: pass
R["configured_rule_bindings"]=len(pairs)

seen=set()
for rule,fn in pairs:
    if rule in seen: continue
    seen.add(rule)
    try:
        ev.describe_rule(Name=rule); R["already_ok"]+=1
    except ev.exceptions.ResourceNotFoundException:
        expr=cadence_from_name(rule)
        if expr:
            try:
                bind(rule,expr,fn)
                R["recreated"].append({"rule":rule,"fn":fn,"expr":expr})
            except Exception as e:
                R["errors"].append({"rule":rule,"err":str(e)[:100]})
        else:
            R["missing_ambiguous"].append({"rule":rule,"fn":fn})
    except Exception as e:
        R["errors"].append({"rule":rule,"err":str(e)[:100]})
    time.sleep(0.06)

# insider-clusters: long scanner may have completed since 4369's timeout
def head(key):
    try:
        h=s3.head_object(Bucket=BUCKET,Key=key)
        return {"age_h":round((t0-h["LastModified"]).total_seconds()/3600,1),
                "kb":round(h["ContentLength"]/1024,1)}
    except Exception as e:
        return {"err":str(e)[:80]}
cl=head("data/insider-clusters.json")
R["insider_clusters"]=cl
if isinstance(cl.get("age_h"),(int,float)) and cl["age_h"]>1:
    try:
        lam.invoke(FunctionName="justhodl-insider-cluster-scanner",
                   InvocationType="Event",Payload=b"{}")
        R["insider_clusters_async_fired"]=True
    except Exception as e:
        R["insider_clusters_async_err"]=str(e)[:100]

# verify the headline resurrection
try:
    rr=ev.describe_rule(Name="justhodl-insider-trades-30min")
    tg=ev.list_targets_by_rule(Rule="justhodl-insider-trades-30min").get("Targets",[])
    R["insider_trades_rule"]={"state":rr.get("State"),"expr":rr.get("ScheduleExpression"),
                              "targets":len(tg)}
except Exception as e:
    R["insider_trades_rule"]={"err":str(e)[:100]}
R["insider_trades_feed"]=head("data/insider-trades.json")

R["verdict"]=(f"PASS — {len(R['recreated'])} rules resurrected, insider-trades scheduled"
              if R["insider_trades_rule"].get("state")=="ENABLED"
              else "PARTIAL — see fields")
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4370_rule_resurrect.json","w"),indent=1,default=str)
open("aws/ops/reports/4370_rule_resurrect.md","w").write(
    f"# ops 4370 — rule resurrection — {R['verdict']}\n"
    f"- configured bindings scanned: {R['configured_rule_bindings']} | alive: {R['already_ok']}\n"
    f"- RECREATED ({len(R['recreated'])}): {[(x['rule'],x['expr']) for x in R['recreated']]}\n"
    f"- ambiguous cadence, need Khalid ({len(R['missing_ambiguous'])}): {[x['rule'] for x in R['missing_ambiguous']]}\n"
    f"- errors: {R['errors'][:8]}\n"
    f"- insider-trades rule: {json.dumps(R['insider_trades_rule'])} | feed: {json.dumps(R['insider_trades_feed'])}\n"
    f"- insider-clusters: {json.dumps(cl)} async_fired={R.get('insider_clusters_async_fired')}\n")
print(json.dumps(R,indent=1,default=str))
