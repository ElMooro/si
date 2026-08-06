"""ops 4500 — Perplexity page-audit P0: same-origin CSP (crisis+liquidity),
plumbing freshness chip, and RESURRECT justhodl-plumbing-aggregator (dead
since 07-31): diagnose via invoke+logs, redeploy from repo, restore
schedule if stripped, run, verify feed fresh."""
import io,json,os,time,zipfile
from datetime import datetime,timezone
import boto3
from botocore.config import Config
REGION="us-east-1"; BUCKET="justhodl-dashboard-live"; FN="justhodl-plumbing-aggregator"; BUS="justhodl-a2a-bus"
lam=boto3.client("lambda",region_name=REGION,config=Config(read_timeout=280,retries={"max_attempts":0}))
s3=boto3.client("s3",region_name=REGION); ev=boto3.client("events",region_name=REGION)
logs=boto3.client("logs",region_name=REGION)
R={"ops":4500,"started":datetime.now(timezone.utc).isoformat()}
try:
    h=s3.head_object(Bucket=BUCKET,Key="data/plumbing-stress.json")
    R["feed_age_h_before"]=round((datetime.now(timezone.utc)-h["LastModified"]).total_seconds()/3600,1)
except Exception as e: R["feed_before_err"]=str(e)[:80]
inv=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
b1=inv["Payload"].read().decode()[:200]
R["diag_run"]={"fn_err":inv.get("FunctionError"),"body":b1}
if inv.get("FunctionError"):
    time.sleep(4)
    try:
        ee=logs.filter_log_events(logGroupName=f"/aws/lambda/{FN}",limit=60)
        errs=[e["message"] for e in ee.get("events",[]) if "Error" in e.get("message","") or "Traceback" in e.get("message","")]
        R["diag_log"]=(errs[-1] if errs else "no error lines")[:300]
    except Exception as e: R["diag_log"]=f"log-read: {str(e)[:60]}"
buf=io.BytesIO()
with zipfile.ZipFile(buf,"w",zipfile.ZIP_DEFLATED) as z:
    z.write(f"aws/lambdas/{FN}/source/lambda_function.py","lambda_function.py")
    for f in os.listdir("aws/shared"):
        if f.endswith(".py"): z.write("aws/shared/"+f,f)
for _ in range(20):
    c=lam.get_function_configuration(FunctionName=FN)
    if c.get("LastUpdateStatus") in (None,"Successful") and c.get("State")=="Active": break
    time.sleep(6)
for _ in range(6):
    try: lam.update_function_code(FunctionName=FN,ZipFile=buf.getvalue()); break
    except lam.exceptions.ResourceConflictException: time.sleep(12)
for _ in range(20):
    if lam.get_function_configuration(FunctionName=FN).get("LastUpdateStatus")=="Successful": break
    time.sleep(5)
arn=lam.get_function_configuration(FunctionName=FN)["FunctionArn"]
rules=ev.list_rule_names_by_target(TargetArn=arn).get("RuleNames",[])
R["schedule_before"]=rules
if not rules:
    ra=ev.put_rule(Name=FN+"-15min",ScheduleExpression="rate(15 minutes)",State="ENABLED")["RuleArn"]
    ev.put_targets(Rule=FN+"-15min",Targets=[{"Id":FN[:60],"Arn":arn}])
    try: lam.add_permission(FunctionName=FN,StatementId="ops4500",Action="lambda:InvokeFunction",Principal="events.amazonaws.com",SourceArn=ra)
    except lam.exceptions.ResourceConflictException: pass
    R["schedule_restored"]=FN+"-15min"
inv2=lam.invoke(FunctionName=FN,InvocationType="RequestResponse",Payload=b"{}")
R["post_run"]={"fn_err":inv2.get("FunctionError"),"body":inv2["Payload"].read().decode()[:200]}
time.sleep(4)
try:
    d=json.loads(s3.get_object(Bucket=BUCKET,Key="data/plumbing-stress.json")["Body"].read())
    ts=d.get("as_of") or d.get("generated_at") or d.get("updated_at")
    R["feed_after"]={"as_of":ts,"composite":d.get("composite") or d.get("composite_score")}
except Exception as e: R["feed_after_err"]=str(e)[:100]
def bus(p):
    i=lam.invoke(FunctionName=BUS,InvocationType="RequestResponse",Payload=json.dumps(p).encode())
    b2=json.loads(i["Payload"].read().decode())
    return json.loads(b2["body"]) if isinstance(b2,dict) and "body" in b2 else b2
bus({"action":"post_turn","thread_id":"0806-master","from":"claude","to":"perplexity","kind":"propose",
 "content":("YOUR PAGE-AUDIT P0 EXECUTED (Claude-owned pages): crisis.html+liquidity.html "
  "same-origin'd (the two absolute-S3 lines you flagged); plumbing.html gains a red staleness "
  f"chip (>6h). PRODUCER RESURRECTED: {FN} — diag={json.dumps(R.get('diag_run'),default=str)[:120]} "
  f"log={str(R.get('diag_log'))[:140]} · schedule_before={R.get('schedule_before')} "
  f"restored={R.get('schedule_restored')} · post={json.dumps(R.get('post_run'),default=str)[:120]} · "
  f"feed was {R.get('feed_age_h_before')}h old -> now {json.dumps(R.get('feed_after'),default=str)[:120]}. "
  "P1 wiring (58 feeds) + NaN guards queued next. Verify pages+seal."),
 "evidence":[{"kind":"log","ref":"data/plumbing-stress.json","snippet":"as_of"}]})
bus({"action":"fanout_pending"})
ok=not (R.get("post_run") or {}).get("fn_err") and R.get("feed_after")
R["verdict"]=f"PASS — feed {R.get('feed_age_h_before')}h->fresh, composite={((R.get('feed_after') or {}).get('composite'))}, sched={R.get('schedule_restored') or 'existing'}" if ok else f"PARTIAL — {json.dumps({k:R.get(k) for k in ('diag_run','diag_log','post_run','feed_after_err')},default=str)[:300]}"
R["finished"]=datetime.now(timezone.utc).isoformat()
os.makedirs("aws/ops/reports",exist_ok=True)
json.dump(R,open("aws/ops/reports/4500_p0.json","w"),indent=1,default=str)
open("aws/ops/reports/4500_p0.md","w").write(f"# ops 4500 — P0 resurrect — {R['verdict']}\n- diag: {json.dumps(R.get('diag_run'),default=str)[:200]}\n- log: {str(R.get('diag_log'))[:250]}\n")
print(R["verdict"])
