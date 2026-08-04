"""ops 4371 — rule resurrection v2 (4370 crashed: config eventbridge_rules
entries can be dicts -> unhashable in set). Architecture-aware this time:
the fleet has a CENTRAL scheduler (justhodl-scheduler <- config/schedule-
manifest.json) + schedule-liveness watchdog. So: 1) audit the central
scheduler's own rule + recent invocations + manifest coverage; 2) dict-safe
sweep of per-engine configured rules; recreate ONLY missing rules whose
function is NOT manifest-covered and whose cadence parses from the name;
3) insider-trades-30min explicitly (declared contract); 4) clusters re-head.
Crash-proof: global traceback lands in the report; incremental writes."""
import json, os, re, time, glob, traceback
from datetime import datetime, timezone, timedelta
import boto3
from botocore.config import Config

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=60,retries={"max_attempts":1}))
ev=boto3.client("events",region_name=REGION); s3=boto3.client("s3",region_name=REGION)
logs=boto3.client("logs",region_name=REGION)
t0=datetime.now(timezone.utc)
R={"ops":4371,"started":t0.isoformat(),"recreated":[],"manifest_covered_missing":[],
   "missing_ambiguous":[],"already_ok":0,"errors":[]}

def save():
    os.makedirs("aws/ops/reports",exist_ok=True)
    json.dump(R,open("aws/ops/reports/4371_rule_resurrect.json","w"),indent=1,default=str)

def main():
    # ---- 1. central scheduler audit ----
    sched={"functions_in_manifest":set()}
    try:
        man=json.loads(s3.get_object(Bucket=BUCKET,Key="config/schedule-manifest.json")["Body"].read())
        entries=man.get("schedules") or man.get("entries") or man.get("functions") or man
        if isinstance(entries,dict):
            sched["manifest_entries"]=len(entries)
            sched["functions_in_manifest"]=set(entries.keys())
            sched["sample"]=dict(list(entries.items())[:4])
        elif isinstance(entries,list):
            sched["manifest_entries"]=len(entries)
            for e in entries:
                if isinstance(e,dict):
                    fn=e.get("function") or e.get("fn") or e.get("name")
                    if fn: sched["functions_in_manifest"].add(fn)
            sched["sample"]=entries[:4]
    except Exception as e:
        sched["manifest_err"]=str(e)[:120]
    try:
        rl=ev.list_rules(NamePrefix="justhodl-scheduler").get("Rules",[])
        sched["scheduler_rules"]=[{"name":x["Name"],"state":x.get("State"),
                                    "expr":x.get("ScheduleExpression")} for x in rl]
        since=int((t0-timedelta(hours=12)).timestamp()*1000)
        ee=logs.filter_log_events(logGroupName="/aws/lambda/justhodl-scheduler",
                                  startTime=since,filterPattern="START",limit=100)
        sched["scheduler_invocations_12h"]=len(ee.get("events",[]))
    except Exception as e:
        sched["scheduler_err"]=str(e)[:120]
    covered=sched.get("functions_in_manifest") or set()
    sched["functions_in_manifest"]=sorted(covered)[:400]
    R["central_scheduler"]=sched; save()

    # ---- 2. dict-safe sweep ----
    def cadence(name):
        n=name.lower()
        m=re.search(r'(\d+)\s*min',n)
        if m: return f"rate({m.group(1)} minutes)" if m.group(1)!="1" else "rate(1 minute)"
        m=re.search(r'-(\d+)h\b',n) or re.search(r'(\d+)\s*hour',n)
        if m: return f"rate({m.group(1)} hours)" if m.group(1)!="1" else "rate(1 hour)"
        if "hourly" in n: return "rate(1 hour)"
        if "daily" in n or n.endswith("-1d"): return "rate(1 day)"
        if "weekly" in n: return "rate(7 days)"
        return None
    def bind(rule,expr,fn):
        arn=ev.put_rule(Name=rule,ScheduleExpression=expr,State="ENABLED",
                        Description=f"ops4371 resurrect: {fn}")["RuleArn"]
        fa=lam.get_function_configuration(FunctionName=fn)["FunctionArn"]
        ev.put_targets(Rule=rule,Targets=[{"Id":fn[:60],"Arn":fa}])
        try:
            lam.add_permission(FunctionName=fn,StatementId=("ops4371-"+rule)[:100],
                               Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com",SourceArn=arn)
        except lam.exceptions.ResourceConflictException: pass
    pairs=[]
    for cf in glob.glob("aws/lambdas/*/config.json"):
        try:
            c=json.load(open(cf)); fn=c.get("function_name")
            for r in (c.get("eventbridge_rules") or []):
                if isinstance(r,dict):
                    nm=r.get("name") or r.get("rule")
                    ex=r.get("schedule") or r.get("expression")
                    if fn and nm: pairs.append((str(nm),fn,ex))
                elif isinstance(r,str) and fn:
                    pairs.append((r,fn,None))
        except Exception: pass
    R["configured_rule_bindings"]=len(pairs)
    seen=set()
    for rule,fn,declared_expr in pairs:
        if rule in seen: continue
        seen.add(rule)
        try:
            ev.describe_rule(Name=rule); R["already_ok"]+=1
        except ev.exceptions.ResourceNotFoundException:
            if fn in covered:
                R["manifest_covered_missing"].append({"rule":rule,"fn":fn})
                continue
            expr=declared_expr or cadence(rule)
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
        time.sleep(0.08)
    save()

    # ---- 3+4. headline verifications ----
    try:
        rr=ev.describe_rule(Name="justhodl-insider-trades-30min")
        tg=ev.list_targets_by_rule(Rule="justhodl-insider-trades-30min").get("Targets",[])
        R["insider_trades_rule"]={"state":rr.get("State"),"expr":rr.get("ScheduleExpression"),
                                  "targets":len(tg)}
    except Exception as e:
        R["insider_trades_rule"]={"err":str(e)[:100]}
    for key,label in [("data/insider-trades.json","insider_trades_feed"),
                      ("data/insider-clusters.json","insider_clusters_feed")]:
        try:
            h=s3.head_object(Bucket=BUCKET,Key=key)
            R[label]={"age_h":round((t0-h["LastModified"]).total_seconds()/3600,1),
                      "kb":round(h["ContentLength"]/1024,1)}
        except Exception as e:
            R[label]={"err":str(e)[:80]}
    R["verdict"]=("PASS" if R["insider_trades_rule"].get("state")=="ENABLED"
                  else "PARTIAL")

try:
    main()
except Exception:
    R["fatal_traceback"]=traceback.format_exc()[-2000:]
    R["verdict"]="CRASHED — traceback captured"
R["finished"]=datetime.now(timezone.utc).isoformat()
save()
open("aws/ops/reports/4371_rule_resurrect.md","w").write(
    f"# ops 4371 — rule resurrection v2 — {R.get('verdict')}\n"
    f"- central scheduler: rules={json.dumps((R.get('central_scheduler') or {}).get('scheduler_rules'))} "
    f"invocations_12h={(R.get('central_scheduler') or {}).get('scheduler_invocations_12h')} "
    f"manifest_entries={(R.get('central_scheduler') or {}).get('manifest_entries')}\n"
    f"- bindings scanned: {R.get('configured_rule_bindings')} | alive: {R.get('already_ok')}\n"
    f"- RECREATED ({len(R['recreated'])}): {[(x['rule'],x['expr']) for x in R['recreated']][:25]}\n"
    f"- manifest-covered missing (no per-rule needed): {len(R['manifest_covered_missing'])}\n"
    f"- ambiguous: {[x['rule'] for x in R['missing_ambiguous']][:15]}\n"
    f"- errors: {R['errors'][:6]}\n"
    f"- insider-trades rule: {json.dumps(R.get('insider_trades_rule'))}\n"
    f"- feeds: trades={json.dumps(R.get('insider_trades_feed'))} clusters={json.dumps(R.get('insider_clusters_feed'))}\n"
    f"- fatal: {R.get('fatal_traceback','none')[:400]}\n")
print(json.dumps({k:v for k,v in R.items() if k!='central_scheduler'},indent=1,default=str)[:4000])
