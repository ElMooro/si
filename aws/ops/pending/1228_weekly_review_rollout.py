"""1228 — Deploy weekly-ai-review + invoke + show generated memo."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1228_weekly_review_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-weekly-ai-review"
SOURCE_DIR = "aws/lambdas/justhodl-weekly-ai-review/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-weekly-ai-review-sunday"
SCHEDULE = "cron(0 14 ? * SUN *)"

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


# Deploy
print(f"[1228] 1. Deploy {LAMBDA}")
try:
    lam.get_function_configuration(FunctionName=LAMBDA)
    out["create"] = "exists"
    print("  ✓ exists (updating)")
    zip_bytes = build_zip()
    lam.update_function_code(FunctionName=LAMBDA, ZipFile=zip_bytes)
    for _ in range(15):
        time.sleep(2)
        c = lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("LastUpdateStatus") == "Successful":
            break
    out["create"] = "updated"
except lam.exceptions.ResourceNotFoundException:
    try:
        zip_bytes = build_zip()
        lam.create_function(
            FunctionName=LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler", Code={"ZipFile": zip_bytes},
            Description="Weekly AI review memo for self-improvement loop",
            Timeout=180, MemorySize=768, Architectures=["x86_64"], Publish=False,
        )
        for _ in range(30):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=LAMBDA)
            if c.get("State") == "Active":
                break
        out["create"] = "created"
        print("  ✓ created")
    except Exception as e:
        out["create_err"] = str(e)[:300]

# Schedule
print(f"\n[1228] 2. Schedule {SCHEDULE}")
try:
    events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                    Description="Weekly AI review Sundays 10:00 ET")
    fn = lam.get_function(FunctionName=LAMBDA)
    events.put_targets(Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": fn["Configuration"]["FunctionArn"]}])
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

# Invoke (inaugural run — will use bootstrap memo since no scored data yet)
print(f"\n[1228] 3. Inaugural invoke")
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
        print(f"  ⚠ {payload[:600]}")
    else:
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  n_predictions={inner.get('n_predictions')} hit_rate={inner.get('hit_rate_pct')}% trust={inner.get('trust_level')}")
            print(f"  headline: {inner.get('headline','')[:200]}")
            print(f"  telegram: {inner.get('telegram_status')}")
        except Exception:
            pass
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

# Read the memo
print(f"\n[1228] 4. Read weekly memo")
try:
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/weekly-review-latest.json")["Body"].read())
    memo = doc.get("memo", {})
    out["memo"] = {
        "week_ending": doc.get("week_ending"),
        "model": doc.get("model"),
        "headline": memo.get("headline"),
        "performance_recap": memo.get("performance_recap"),
        "calibration_progress": memo.get("calibration_progress"),
        "system_improvements": memo.get("system_improvements"),
        "next_week_setup": memo.get("next_week_setup"),
        "trust_level": memo.get("trust_level"),
        "key_metric": memo.get("key_metric"),
    }
    print(f"\n  ━━ WEEKLY MEMO (model: {doc.get('model')}) ━━")
    print(f"  HEADLINE: {memo.get('headline','')[:200]}")
    print(f"  KEY METRIC: {memo.get('key_metric','')[:200]}")
    print(f"  TRUST: {memo.get('trust_level')}/100")
except Exception as e:
    out["memo"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1228] DONE")
