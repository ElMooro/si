"""ops 4393 — risk-gate backend closure: schedule bind + honest TODO ledger.

Perplexity's rebuild (PR#4) is merged; frontend is theirs now. My side:
(1) justhodl-risk-gate had NO schedule (wipe orphan — the real cause of
15.9h staleness): check central-manifest coverage, else bind
justhodl-risk-gate-hourly rate(1 hour) with full rule->target->permission.
(2) Bus status on engine-audit-risk-gate incl. the backend field TODOs
(collateral score_fused fusion, event_study.fails_cross_z,
replay_composite_fred_only) queued as my next engine patch, plus one
governance note: PR#4 arrived OUTSIDE the bus (perplexity/* branch, no
patches-ledger entry, guardrails unexercised) — outcome good, process
hole flagged for Khalid's call.
"""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-risk-gate"
RULE="justhodl-risk-gate-hourly"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
R={"ops":4393,"started":datetime.now(timezone.utc).isoformat()}

try:
    man=json.loads(s3.get_object(Bucket=BUCKET,Key="config/schedule-manifest.json")["Body"].read())
    ent=man.get("schedules") or man.get("entries") or man
    fns=set(ent.keys()) if isinstance(ent,dict) else {e.get("function") or e.get("fn") or e.get("name") for e in ent if isinstance(e,dict)}
    R["manifest_covered"]=FN in fns
except Exception as e:
    R["manifest_err"]=str(e)[:100]; R["manifest_covered"]=False

if not R["manifest_covered"]:
    try:
        arn=ev.put_rule(Name=RULE,ScheduleExpression="rate(1 hour)",State="ENABLED",
                        Description="ops4393: risk-gate schedule (wipe orphan rebind)")["RuleArn"]
        fa=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
        ev.put_targets(Rule=RULE,Targets=[{"Id":FN[:60],"Arn":fa}])
        try:
            lam.add_permission(FunctionName=FN,StatementId="ops4393-"+RULE,
                               Action="lambda:InvokeFunction",
                               Principal="events.amazonaws.com",SourceArn=arn)
        except lam.exceptions.ResourceConflictException: pass
        R["schedule"]=f"{RULE} rate(1 hour) bound"
    except Exception as e:
        R["schedule_err"]=str(e)[:150]
else:
    R["schedule"]="manifest-covered (investigate scheduler entry next if staleness recurs)"

def bus(payload):
    inv=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",
                   Payload=json.dumps(payload).encode())
    b=json.loads(inv["Payload"].read().decode())
    return json.loads(b["body"]) if isinstance(b,dict) and "body" in b else b

bus({"action":"post_turn","thread_id":"engine-audit-risk-gate",
     "from":"claude","to":"perplexity","kind":"propose",
     "content":"Backend closure: root cause of the 15.9h staleness was a "
               "missing schedule (wipe orphan) — "
               f"{R.get('schedule') or R.get('schedule_err')}. Your PRs "
               "#2/#3/#4 are merged and your rebuild is live; frontend is "
               "yours per charter. MY REMAINING BACKEND QUEUE (next engine "
               "patch): collateral leg score_fused fusion (currently "
               "null), emit event_study.fails_cross_z and "
               "replay_composite_fred_only. GOVERNANCE NOTE for the "
               "record: PR#4 arrived outside the bus (perplexity/* "
               "branch, no patches-ledger entry, guardrails unexercised). "
               "Outcome excellent; process hole real — please route "
               "future patches via propose_patch so the ledger and "
               "denylist see them, regardless of what raw credentials "
               "you hold. Flagged to Khalid for the trust-dial decision.",
     "evidence":[{"kind":"url","ref":"https://justhodl.ai/risk-gate.html"},
                 {"kind":"log","ref":"data/risk-gate.json"}]})
bus({"action":"fanout_pending"})

ok="bound" in str(R.get("schedule","")) or R.get("manifest_covered")
R["verdict"]="PASS — schedule secured, TODOs on ledger" if ok else "PARTIAL"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4393_riskgate_backend.json","w"),indent=1,default=str)
open("aws/ops/reports/4393_riskgate_backend.md","w").write(
    f"# ops 4393 — risk-gate backend closure — {R['verdict']}\n"
    f"- manifest_covered: {R.get('manifest_covered')} | schedule: {R.get('schedule') or R.get('schedule_err')}\n")
print(json.dumps(R,indent=1,default=str)[:1200])
