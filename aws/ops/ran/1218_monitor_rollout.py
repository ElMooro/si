"""1218 — Roll out justhodl-trade-ticket-monitor + invoke + verify."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1218_monitor_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-trade-ticket-monitor"
SOURCE_DIR = "aws/lambdas/justhodl-trade-ticket-monitor/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-trade-ticket-monitor-10min"
SCHEDULE = "cron(0/10 14-20 * * MON-FRI *)"

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
print(f"[1218] 1. Create {LAMBDA}")
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
            Description="Real-time trade ticket monitor — stop/TP alerts",
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
print(f"\n[1218] 2. Schedule {SCHEDULE}")
try:
    events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                    Description="Trade ticket monitor every 10 min trading hours")
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

# Invoke
print(f"\n[1218] 3. Sync invoke (will check all active tickets for current price vs levels)")
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
    else:
        try:
            outer = json.loads(payload)
            inner = json.loads(outer.get("body", "{}"))
            print(f"  watched={inner.get('n_watched')} priced={inner.get('n_priced')} "
                  f"alerts={inner.get('n_alerts')}")
            print(f"  telegram_status={inner.get('telegram_status')}")
            if inner.get('alerts_by_type'):
                print(f"  alerts_by_type: {inner['alerts_by_type']}")
        except Exception:
            pass
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

# Read snapshot
print(f"\n[1218] 4. Read snapshot file")
try:
    snap = json.loads(s3.get_object(Bucket=BUCKET, Key="data/trade-monitor-snapshots.json")["Body"].read())
    out["snapshot_doc"] = {
        "generated_at": snap.get("generated_at"),
        "n_watched": snap.get("n_watched"),
        "n_priced": snap.get("n_priced"),
        "n_alerts": snap.get("n_alerts"),
        "snapshots_top10": snap.get("snapshots", [])[:10],
        "alerts_just_fired": snap.get("alerts_just_fired", []),
    }
    print(f"  ✓ snapshot saved with {snap.get('n_watched')} watched · "
          f"{snap.get('n_priced')} priced · {snap.get('n_alerts')} alerts")
except Exception as e:
    out["snapshot_doc"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1218] DONE")
