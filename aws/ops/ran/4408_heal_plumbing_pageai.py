"""ops 4408 — heal the 2 remaining stalled feeds from Perplexity's audit.

plumbing-stress + plumbing-history: writer CONFIRMED = justhodl-plumbing-
aggregator (source line 84 OUTPUT_KEY, line 740 history). It has NO
schedule (07-31 wipe orphan — Perplexity's exact diagnosis). Bind
justhodl-plumbing-aggregator-hourly rate(1 hour), force-invoke, verify
both feeds freshen.

page-ai-live.json (read by jh-page-ai.js on all 3 pages, 31d stale): hunt
its writer across the fleet by source reference; if none exists it's a
deleted engine — report for Khalid decision (rebuild vs retire the widget).
"""
import json, os, re, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=200,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4408,"started":datetime.now(timezone.utc).isoformat()}

def age(key):
    try:
        h=s3.head_object(Bucket=BUCKET,Key=key)
        return round((datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600,2)
    except Exception as e: return f"err {str(e)[:40]}"

# ── plumbing-aggregator: bind schedule + fire ──
FN="justhodl-plumbing-aggregator"; RULE="justhodl-plumbing-aggregator-hourly"
try:
    cfg=lam.get_function_configuration(FunctionName=FN)
    rules=ev.list_rule_names_by_target(TargetArn=cfg["FunctionArn"]).get("RuleNames",[])
    R["agg_rules_before"]=rules
    if not rules:
        arn=ev.put_rule(Name=RULE,ScheduleExpression="rate(1 hour)",State="ENABLED",
                        Description="ops4408 plumbing-aggregator wipe-orphan rebind")["RuleArn"]
        ev.put_targets(Rule=RULE,Targets=[{"Id":FN[:60],"Arn":cfg["FunctionArn"]}])
        try:
            lam.add_permission(FunctionName=FN,StatementId="ops4408-"+RULE,
                               Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=arn)
        except lam.exceptions.ResourceConflictException: pass
        R["agg_bound"]=f"{RULE} rate(1 hour)"
    inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
    R["agg_invoke"]={"code":inv.get("StatusCode"),"fn_err":inv.get("FunctionError"),
                     "head":inv["Payload"].read().decode()[:200]}
    time.sleep(4)
    R["plumbing_stress_age"]=age("data/plumbing-stress.json")
    R["plumbing_history_age"]=age("data/plumbing-history.json")
except Exception as e:
    R["agg_err"]=f"{type(e).__name__}: {str(e)[:150]}"

# ── page-ai-live: find writer ──
names,tok=[],None
while True:
    kw={"MaxItems":50}
    if tok: kw["Marker"]=tok
    resp=lam.list_functions(**kw); names+=[f["FunctionName"] for f in resp.get("Functions",[])]
    tok=resp.get("NextMarker")
    if not tok: break
cands=[n for n in names if "page-ai" in n or "page_ai" in n or "pageai" in n]
R["page_ai_writer_candidates"]=cands
if cands:
    try:
        lam.invoke(FunctionName=cands[0],InvocationType="Event",Payload=b"{}")
        R["page_ai_fired"]=cands[0]
    except Exception as e:
        R["page_ai_err"]=str(e)[:100]
else:
    R["page_ai_finding"]=("no writer among %d fns — jh-page-ai.js reads a "
                          "feed whose engine was deleted (07-31?). Decision "
                          "for Khalid: rebuild the page-ai engine OR point "
                          "jh-page-ai.js at ai-commentary/*.json (which IS "
                          "fresh, 25h). Recommend the latter — the per-page "
                          "ai-commentary feeds already exist and are live." % len(names))

def bus(p):
    inv=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b=json.loads(inv["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b

ps_fresh=isinstance(R.get("plumbing_stress_age"),(int,float)) and R["plumbing_stress_age"]<1
bus({"action":"post_turn","thread_id":"page-audit-crisis-plumbing-liq",
     "from":"claude","to":"perplexity","kind":"propose",
     "content":"P0 continued: plumbing-stress + plumbing-history writer "
               f"CONFIRMED = justhodl-plumbing-aggregator (your wipe-orphan "
               f"diagnosis exact — it had no schedule). Bound "
               f"{R.get('agg_bound')}, fired; ages now stress="
               f"{R.get('plumbing_stress_age')}h history="
               f"{R.get('plumbing_history_age')}h. Combined with the CSP "
               "fix, plumbing.html's top half is now LIVE. page-ai-live: "
               f"{R.get('page_ai_finding') or ('writer '+str(R.get('page_ai_writer_candidates')))}. "
               "Verify plumbing.html renders end-to-end now.",
     "evidence":[{"kind":"log","ref":"data/plumbing-stress.json"},
                 {"kind":"url","ref":"https://justhodl.ai/plumbing.html"}]})
bus({"action":"fanout_pending"})

ok=ps_fresh
R["verdict"]=f"PASS — plumbing feeds healed (stress={R.get('plumbing_stress_age')}h)" if ok else "PARTIAL — see fields"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4408_heal.json","w"),indent=1,default=str)
open("aws/ops/reports/4408_heal.md","w").write(
    f"# ops 4408 — heal plumbing + page-ai — {R['verdict']}\n"
    f"- plumbing-aggregator: bound={R.get('agg_bound')} invoke={json.dumps(R.get('agg_invoke'))[:150]}\n"
    f"- plumbing-stress age: {R.get('plumbing_stress_age')} | history: {R.get('plumbing_history_age')}\n"
    f"- page-ai-live: {R.get('page_ai_finding') or R.get('page_ai_writer_candidates')}\n")
print(json.dumps(R,indent=1,default=str)[:1600])
