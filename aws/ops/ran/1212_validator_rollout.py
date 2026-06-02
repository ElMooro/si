"""1212 — Roll out cascade-validator Lambda + initial invoke."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1212_validator_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-cascade-validator"
SOURCE_DIR = "aws/lambdas/justhodl-cascade-validator/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-cascade-validator-eod"
SCHEDULE = "cron(0 21 * * MON-FRI *)"

cfg = Config(read_timeout=720, retries={"max_attempts": 1})
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


# Create Lambda
print(f"[1212] 1. Create {LAMBDA}")
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
            Description="Cascade prediction validator — forward-looking track record",
            Timeout=600, MemorySize=1024, Architectures=["x86_64"], Publish=False,
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
print(f"\n[1212] 2. Schedule {SCHEDULE}")
try:
    events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                    Description="Cascade validator weekdays 16:00 ET")
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

# Sync invoke
print(f"\n[1212] 3. Sync invoke")
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

# Read output
print(f"\n[1212] 4. Read validation log")
try:
    doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cascade-validation-log.json")["Body"].read())
    out["log"] = {
        "generated_at": doc.get("generated_at"),
        "n_predictions_validated": doc.get("n_predictions_validated"),
        "outcome_counts": doc.get("outcome_counts"),
        "by_tier_stats": doc.get("by_tier_stats"),
        "best_calls_top10": doc.get("best_calls", [])[:10],
        "worst_calls_top5": doc.get("worst_calls", [])[:5],
    }
    print(f"  ✓ validated {doc.get('n_predictions_validated')} predictions")
    print(f"  outcomes: {doc.get('outcome_counts')}")
    for tier, stats in (doc.get('by_tier_stats') or {}).items():
        if stats.get('n', 0) > 0:
            print(f"    {tier:12s}: n={stats['n']}  hit_rate={stats.get('hit_rate_pct')}%  "
                  f"mean_max={stats.get('mean_max_return_pct')}%")
except Exception as e:
    out["log"] = {"error": str(e)[:300]}


out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1212] DONE")
