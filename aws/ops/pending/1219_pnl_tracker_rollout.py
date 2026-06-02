"""1219 — Roll out justhodl-pnl-tracker + invoke + show first portfolio + daily digest."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1219_pnl_tracker_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-pnl-tracker"
SOURCE_DIR = "aws/lambdas/justhodl-pnl-tracker/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-pnl-tracker-daily"
SCHEDULE = "cron(30 21 * * MON-FRI *)"

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


# Create
print(f"[1219] 1. Create {LAMBDA}")
try:
    lam.get_function_configuration(FunctionName=LAMBDA)
    out["create"] = "exists"
    print("  ✓ exists")
except lam.exceptions.ResourceNotFoundException:
    try:
        zip_bytes = build_zip()
        resp = lam.create_function(
            FunctionName=LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler", Code={"ZipFile": zip_bytes},
            Description="Simulated portfolio P&L tracker — daily digest",
            Timeout=60, MemorySize=512, Architectures=["x86_64"], Publish=False,
        )
        out["create"] = "created"
        for _ in range(30):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=LAMBDA)
            if c.get("State") == "Active":
                break
        print(f"  ✓ created")
    except Exception as e:
        out["create_error"] = str(e)[:300]

# Schedule
print(f"\n[1219] 2. Schedule {SCHEDULE}")
try:
    events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                    Description="Daily P&L tracker after market close")
    fn = lam.get_function(FunctionName=LAMBDA)
    events.put_targets(Rule=RULE_NAME, Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try:
        lam.add_permission(FunctionName=LAMBDA, StatementId=f"EBInvoke-{RULE_NAME}",
                           Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                           SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}")
    except lam.exceptions.ResourceConflictException:
        pass
    out["schedule"] = SCHEDULE
    print("  ✓ scheduled")
except Exception as e:
    out["schedule_err"] = str(e)[:300]

# Sync invoke
print(f"\n[1219] 3. Sync invoke (opens simulated positions for today's alerts + sends digest)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["invoke"] = {"elapsed_s": elapsed, "status": resp.get("StatusCode"),
                      "function_error": resp.get("FunctionError"),
                      "body": payload[:2500]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:600]}")
    else:
        try:
            outer = json.loads(payload)
            inner = json.loads(outer.get("body", "{}"))
            print(f"  newly_opened: {inner.get('newly_opened')}")
            print(f"  exits: {inner.get('n_exits')}")
            print(f"  stats: {inner.get('stats')}")
            print(f"  telegram: {inner.get('telegram_status')}")
        except Exception as e:
            print(f"  parse err: {e}")
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

# Read portfolio
print(f"\n[1219] 4. Read simulated portfolio")
try:
    p = json.loads(s3.get_object(Bucket=BUCKET, Key="data/simulated-portfolio.json")["Body"].read())
    out["portfolio"] = {
        "schema_version": p.get("schema_version"),
        "last_updated": p.get("last_updated"),
        "realized_pnl_total_usd": p.get("realized_pnl_total_usd"),
        "stats": p.get("stats"),
        "n_open": len(p.get("open_positions") or []),
        "n_closed": len(p.get("closed_positions") or []),
        "open_positions_top10": (p.get("open_positions") or [])[:10],
        "closed_positions_top5": (p.get("closed_positions") or [])[:5],
    }
    print(f"  ✓ portfolio: {out['portfolio']['n_open']} open, {out['portfolio']['n_closed']} closed")
    print(f"  realized: ${out['portfolio']['realized_pnl_total_usd']}")
except Exception as e:
    out["portfolio"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1219] DONE")
