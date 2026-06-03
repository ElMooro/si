"""1242 — Deploy political-ai-investigation, invoke, verify AI theses +
confirm prediction-snapshotter now emits politician features."""
import json, os, time, zipfile, io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1242_political_ai_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-political-ai-investigation"
SOURCE_DIR = "aws/lambdas/justhodl-political-ai-investigation/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE = "justhodl-political-ai-investigation-daily"
SCHEDULE = "cron(30 11 * * ? *)"
cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)
out = {"started": datetime.now(timezone.utc).isoformat()}

def zipit():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root,_,files in os.walk(SOURCE_DIR):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"): continue
                fp=os.path.join(root,f); zf.write(fp, arcname=os.path.relpath(fp,SOURCE_DIR))
    return buf.getvalue()

print("[1242] 1. Deploy AI investigation Lambda")
try:
    zb=zipit()
    try:
        lam.get_function_configuration(FunctionName=LAMBDA)
        lam.update_function_code(FunctionName=LAMBDA, ZipFile=zb); action="updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
            Description="Political AI investigation", Timeout=300, MemorySize=512,
            Architectures=["x86_64"], Publish=False); action="created"
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["deploy"]={"action":action,"sha":c.get("CodeSha256")[:16]}
    print(f"  ✓ {action}")
except Exception as e:
    out["deploy_err"]=str(e)[:300]; print(f"  ❌ {e}")

print("\n[1242] 2. Schedule")
try:
    events.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED", Description="Daily political AI investigation")
    fn=lam.get_function(FunctionName=LAMBDA)
    events.put_targets(Rule=RULE, Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try:
        lam.add_permission(FunctionName=LAMBDA, StatementId=f"EB-{RULE}", Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE}")
    except lam.exceptions.ResourceConflictException: pass
    out["schedule"]=SCHEDULE; print("  ✓ scheduled")
except Exception as e:
    out["schedule_err"]=str(e)[:300]

print("\n[1242] 3. Invoke AI investigation")
try:
    t0=time.time()
    resp=lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    payload=resp.get("Payload").read().decode()
    out["invoke"]={"status":resp.get("StatusCode"),"elapsed_s":round(time.time()-t0,1),
                    "function_error":resp.get("FunctionError"),"body":payload[:800]}
    print(f"  status={resp.get('StatusCode')} body={payload[:200]}")
except Exception as e:
    out["invoke"]={"error":str(e)[:300]}

print("\n[1242] 4. Verify investigation theses")
try:
    doc=json.loads(s3.get_object(Bucket=BUCKET, Key="data/political-ai-investigation.json")["Body"].read())
    bt=doc.get("by_ticker",{})
    out["investigation"]={"n":len(bt),"model":doc.get("model"),
        "samples":[{"ticker":k,"committee":v.get("committee_relevant"),"conviction":v.get("conviction_score"),
                     "thesis":v.get("thesis","")[:280]} for k,v in list(bt.items())[:5]]}
    print(f"  ✓ {len(bt)} investigations")
    for k,v in list(bt.items())[:4]:
        print(f"\n  {k} (conviction {v.get('conviction_score')}, committee={v.get('committee_relevant')}):")
        print(f"    {v.get('thesis','')[:300]}")
except Exception as e:
    out["investigation"]={"error":str(e)[:300]}

print("\n[1242] 5. Re-run snapshotter → confirm politician features + tiers")
try:
    resp=lam.invoke(FunctionName="justhodl-prediction-snapshotter", InvocationType="RequestResponse", Payload=b"{}")
    payload=resp.get("Payload").read().decode()
    try:
        inner=json.loads(json.loads(payload).get("body","{}"))
        out["snapshotter"]={"alert_distribution":inner.get("alert_distribution")}
        ad=inner.get("alert_distribution",{})
        pol_tiers={k:v for k,v in ad.items() if "POLITICIAN" in k}
        print(f"  ✓ POLITICIAN tiers in snapshot: {pol_tiers}")
    except: out["snapshotter"]={"body":payload[:400]}
except Exception as e:
    out["snapshotter"]={"error":str(e)[:300]}

out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str))
print("\n[1242] DONE")
