"""1208 — Roll out theme-cascade-backtest + invoke cascade (with Telegram) + verify."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1208_backtest_telegram_rollout.json"
BUCKET = "justhodl-dashboard-live"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}


def build_zip(source_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(source_dir):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"):
                    continue
                fpath = os.path.join(root, f)
                rel = os.path.relpath(fpath, source_dir)
                zf.write(fpath, arcname=rel)
    return buf.getvalue()


# Step 1: Create backtest Lambda
print("[1208] 1. Create justhodl-theme-cascade-backtest Lambda")
BT_LAMBDA = "justhodl-theme-cascade-backtest"
BT_SOURCE = "aws/lambdas/justhodl-theme-cascade-backtest/source"
BT_RULE = "justhodl-theme-cascade-backtest-daily"
BT_SCHEDULE = "cron(45 22 * * ? *)"

try:
    lam.get_function_configuration(FunctionName=BT_LAMBDA)
    out["backtest_create"] = {"exists": True}
    print(f"  ✓ exists")
except lam.exceptions.ResourceNotFoundException:
    try:
        zip_bytes = build_zip(BT_SOURCE)
        resp = lam.create_function(
            FunctionName=BT_LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler", Code={"ZipFile": zip_bytes},
            Description="Backtest theme cascade: do recent pumpers cluster in hot themes?",
            Timeout=30, MemorySize=256, Architectures=["x86_64"], Publish=False,
        )
        out["backtest_create"] = {"created": True}
        for _ in range(30):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=BT_LAMBDA)
            if c.get("State") == "Active":
                break
        print(f"  ✓ created")
    except Exception as e:
        out["backtest_create"] = {"error": str(e)[:300]}

# Step 2: Schedule
print(f"\n[1208] 2. Schedule backtest daily")
try:
    events.put_rule(Name=BT_RULE, ScheduleExpression=BT_SCHEDULE, State="ENABLED",
                    Description="Daily theme cascade backtest")
    fn = lam.get_function(FunctionName=BT_LAMBDA)
    events.put_targets(Rule=BT_RULE, Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
    try:
        lam.add_permission(FunctionName=BT_LAMBDA, StatementId=f"EBInvoke-{BT_RULE}",
                           Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                           SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{BT_RULE}")
    except lam.exceptions.ResourceConflictException:
        pass
    print(f"  ✓ scheduled")
except Exception as e:
    out["schedule_err"] = str(e)[:300]

# Step 3: Invoke backtest
print(f"\n[1208] 3. Invoke backtest")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName=BT_LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["backtest_invoke"] = {"elapsed_s": elapsed, "status": resp.get("StatusCode"),
                                "function_error": resp.get("FunctionError"),
                                "body": payload[:3000]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:500]}")
except Exception as e:
    out["backtest_invoke"] = {"error": str(e)[:300]}

# Step 4: Read backtest output
print(f"\n[1208] 4. Read data/theme-cascade-backtest.json")
try:
    bt = json.loads(s3.get_object(Bucket=BUCKET, Key="data/theme-cascade-backtest.json")["Body"].read())
    out["backtest_doc"] = {
        "schema_version": bt.get("schema_version"),
        "n_momentum_leaders": bt.get("n_momentum_leaders"),
        "n_etf_themes": bt.get("n_etf_themes"),
        "big_pumpers_5d_stats": bt.get("big_pumpers_5d_stats"),
        "pumpers_5d_stats": bt.get("pumpers_5d_stats"),
        "control_stats": bt.get("control_stats"),
        "laggards_hot_stats": bt.get("laggards_hot_stats"),
        "lift_metrics": bt.get("lift_metrics"),
        "big_pumpers_detail_top10": (bt.get("big_pumpers_detail") or [])[:10],
        "pumpers_detail_top15": (bt.get("pumpers_detail") or [])[:15],
        "laggards_hot_detail_top15": (bt.get("laggards_hot_detail") or [])[:15],
    }
    print(f"  ✓ backtest loaded")
    print(f"  Interpretation: {bt.get('lift_metrics', {}).get('interpretation')}")
except Exception as e:
    out["backtest_doc"] = {"error": str(e)[:300]}

# Step 5: Re-invoke theme-cascade so Telegram alert fires
print(f"\n[1208] 5. Re-invoke justhodl-theme-cascade (will push Telegram alerts)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-theme-cascade", InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["cascade_invoke"] = {"elapsed_s": elapsed, "status": resp.get("StatusCode"),
                              "function_error": resp.get("FunctionError"),
                              "body": payload[:2000]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:500]}")
except Exception as e:
    out["cascade_invoke"] = {"error": str(e)[:300]}

# Step 6: Check Telegram alert state file was created
print(f"\n[1208] 6. Verify Telegram alert state file")
try:
    state = json.loads(s3.get_object(Bucket=BUCKET, Key="data/_alerts/theme-cascade-alerted.json")["Body"].read())
    out["telegram_alert_state"] = state
    print(f"  ✓ alerted_tickers: {state.get('alerted_tickers', [])}")
    print(f"  ✓ last_send_result: {state.get('last_send_result')}")
except Exception as e:
    out["telegram_alert_state"] = {"error": str(e)[:300]}


out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1208] DONE")
