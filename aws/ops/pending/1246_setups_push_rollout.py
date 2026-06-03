"""1246 — Deploy setups-push + schedule + invoke + confirm Telegram brief."""
import json, os, time, zipfile, io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1246_setups_push_rollout.json"
LAMBDA = "justhodl-setups-push"
SOURCE_DIR = "aws/lambdas/justhodl-setups-push/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE = "justhodl-setups-push-daily"
SCHEDULE = "cron(45 12 * * ? *)"
cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)
out = {"started": datetime.now(timezone.utc).isoformat()}

def zipit():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root,_,files in os.walk(SOURCE_DIR):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root: continue
                fp=os.path.join(root,f); zf.write(fp, arcname=os.path.relpath(fp,SOURCE_DIR))
    return buf.getvalue()

print(f"[1246] 1. Deploy {LAMBDA}")
try:
    zb=zipit()
    try:
        lam.get_function_configuration(FunctionName=LAMBDA)
        lam.update_function_code(FunctionName=LAMBDA, ZipFile=zb); action="updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(FunctionName=LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler", Code={"ZipFile": zb},
            Description="Morning setups push + custom alerts", Timeout=120, MemorySize=256,
            Architectures=["x86_64"], Publish=False); action="created"
    for _ in range(30):
        time.sleep(2); c=lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("State")=="Active" and c.get("LastUpdateStatus") in ("Successful",None): break
    out["deploy"]={"action":action,"sha":c.get("CodeSha256")[:16]}; print(f"  ✓ {action}")
except Exception as e:
    out["deploy_err"]=str(e)[:300]; print(f"  ❌ {e}")

print("\n[1246] 2. Schedule")
try:
    events.put_rule(Name=RULE, ScheduleExpression=SCHEDULE, State="ENABLED", Description="Daily setups push")
    fn=lam.get_function(FunctionName=LAMBDA)
    events.put_targets(Rule=RULE, Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try:
        lam.add_permission(FunctionName=LAMBDA, StatementId=f"EB-{RULE}", Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com", SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE}")
    except lam.exceptions.ResourceConflictException: pass
    out["schedule"]=SCHEDULE; print("  ✓ scheduled")
except Exception as e:
    out["schedule_err"]=str(e)[:300]

print("\n[1246] 3. Invoke (sends morning brief to Telegram)")
try:
    t0=time.time()
    resp=lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    payload=resp.get("Payload").read().decode()
    out["invoke"]={"status":resp.get("StatusCode"),"elapsed_s":round(time.time()-t0,1),
                    "function_error":resp.get("FunctionError"),"body":payload[:500]}
    print(f"  status={resp.get('StatusCode')} body={payload[:300]}")
    if resp.get("FunctionError"): print(f"  ⚠ {payload[:400]}")
except Exception as e:
    out["invoke"]={"error":str(e)[:300]}

out["finished"]=datetime.now(timezone.utc).isoformat()
open(REPORT,"w").write(json.dumps(out,indent=2,default=str))
print("\n[1246] DONE — check @Justhodl_bot for the morning setups brief")
