"""1213 — Roll out prepump-alerts-router + initial invoke + audit Telegram coverage."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1213_prepump_router_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-prepump-alerts-router"
SOURCE_DIR = "aws/lambdas/justhodl-prepump-alerts-router/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-prepump-alerts-router-30min"
SCHEDULE = "cron(0,30 * * * ? *)"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"):
                    continue
                fpath = os.path.join(root, f)
                rel = os.path.relpath(fpath, SOURCE_DIR)
                zf.write(fpath, arcname=rel)
    return buf.getvalue()


# Step 1: Create Lambda
print(f"[1213] 1. Create {LAMBDA}")
try:
    lam.get_function_configuration(FunctionName=LAMBDA)
    out["create"] = {"exists": True}
    print("  ✓ exists")
except lam.exceptions.ResourceNotFoundException:
    try:
        zip_bytes = build_zip()
        resp = lam.create_function(
            FunctionName=LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler", Code={"ZipFile": zip_bytes},
            Description="Pre-pump signals Telegram router — 7 sources",
            Timeout=60, MemorySize=512, Architectures=["x86_64"], Publish=False,
        )
        out["create"] = {"created": True}
        for _ in range(30):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=LAMBDA)
            if c.get("State") == "Active":
                break
        print(f"  ✓ created")
    except Exception as e:
        out["create"] = {"error": str(e)[:300]}

# Schedule
print(f"\n[1213] 2. Schedule {SCHEDULE}")
try:
    events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                    Description="Every 30min pre-pump router")
    fn = lam.get_function(FunctionName=LAMBDA)
    events.put_targets(Rule=RULE_NAME, Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try:
        lam.add_permission(FunctionName=LAMBDA, StatementId=f"EBInvoke-{RULE_NAME}",
                           Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                           SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}")
    except lam.exceptions.ResourceConflictException:
        pass
    print("  ✓ scheduled")
except Exception as e:
    out["schedule_err"] = str(e)[:300]

# Initial invoke — should push Telegram for ALL pre-pump signals
print(f"\n[1213] 3. Sync invoke (sends first Telegram alerts)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["invoke"] = {"elapsed_s": elapsed, "status": resp.get("StatusCode"),
                      "function_error": resp.get("FunctionError"),
                      "body": payload[:2000]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:500]}")
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

# Read state file
print(f"\n[1213] 4. Read router state")
try:
    state = json.loads(s3.get_object(Bucket=BUCKET, Key="data/_alerts/prepump-router-state.json")["Body"].read())
    out["state"] = state
    alerted = state.get("alerted_by_signal", {})
    n_total = sum(len(v) for v in alerted.values())
    print(f"  Total alerts queued: {n_total}")
    for k, v in alerted.items():
        print(f"    {k}: {len(v)} signals → {v[:5]}")
except Exception as e:
    out["state"] = {"error": str(e)[:200]}

# Audit: list all Lambdas that contain "telegram" or "sendMessage" in code
print(f"\n[1213] 5. Audit existing Telegram-delivering Lambdas")
try:
    audit_lambdas = []
    pag = lam.get_paginator("list_functions")
    for page in pag.paginate():
        for f in page.get("Functions", []):
            n = f["FunctionName"]
            if "justhodl" in n:
                audit_lambdas.append({
                    "name": n,
                    "last_modified": f.get("LastModified", "")[:16],
                    "timeout": f.get("Timeout"),
                })
    out["all_lambdas_count"] = len(audit_lambdas)

    # Find which are scheduled
    scheduled_lambdas = set()
    pag_e = events.get_paginator("list_rules")
    for page in pag_e.paginate():
        for r in page.get("Rules", []):
            if "justhodl" in r["Name"] and r.get("ScheduleExpression"):
                try:
                    targets = events.list_targets_by_rule(Rule=r["Name"]).get("Targets", [])
                    for t in targets:
                        arn = t.get("Arn", "")
                        if ":function:" in arn:
                            scheduled_lambdas.add(arn.split(":function:")[-1])
                except Exception:
                    pass
    out["n_scheduled_lambdas"] = len(scheduled_lambdas)
    print(f"  Total justhodl Lambdas: {len(audit_lambdas)}")
    print(f"  Lambdas with EventBridge schedule: {len(scheduled_lambdas)}")
except Exception as e:
    out["audit_err"] = str(e)[:300]

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1213] DONE")
