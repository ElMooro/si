"""1216 — Roll out justhodl-trade-tickets + invoke + show generated tickets."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1216_trade_tickets_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-trade-tickets"
SOURCE_DIR = "aws/lambdas/justhodl-trade-tickets/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-trade-tickets-hourly"
SCHEDULE = "cron(25 14,15,16,17,18,19 * * MON-FRI *)"

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


# Step 1: Create
print(f"[1216] 1. Create {LAMBDA}")
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
            Description="Trade tickets: ATR stops + R-multiple TPs from cascade",
            Timeout=180, MemorySize=512, Architectures=["x86_64"], Publish=False,
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
        print(f"  ❌ {e}")

# Step 2: Schedule
print(f"\n[1216] 2. Schedule {SCHEDULE}")
try:
    events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                    Description="Hourly trading hours — regenerate tickets")
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

# Step 3: Invoke
print(f"\n[1216] 3. Sync invoke")
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

# Step 4: Read tickets
print(f"\n[1216] 4. Read tickets")
try:
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/trade-tickets.json")["Body"].read())
    tickets = doc.get("tickets") or []
    out["tickets_doc"] = {
        "generated_at": doc.get("generated_at"),
        "n_tickets": doc.get("n_tickets"),
        "n_errors": doc.get("n_errors"),
        "portfolio_usd": doc.get("portfolio_usd"),
        "tickets_top15": tickets[:15],
        "errors": doc.get("errors") or [],
        "methodology": doc.get("sizing_methodology"),
    }
    print(f"  ✓ generated {doc.get('n_tickets')} tickets ({doc.get('n_errors')} errors)")
    for t in tickets[:5]:
        print(f"    {t['ticker']:<6s} entry=${t['entry']} stop=${t['stop_loss']} "
              f"TP3=${t['tp3']} ({t['tp3_pct']}%) RR={t['rr_tp3']}:1 "
              f"shares={t['shares']} maxloss=${t['max_loss_usd']:.0f}")
except Exception as e:
    out["tickets_doc"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1216] DONE")
