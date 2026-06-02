"""1225 — Deploy cascade-recalibrator + invoke + verify loop closed."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1225_recalibrator_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-cascade-recalibrator"
SOURCE_DIR = "aws/lambdas/justhodl-cascade-recalibrator/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-cascade-recalibrator-daily"
SCHEDULE = "cron(5 13 * * MON-FRI *)"

cfg = Config(read_timeout=300, retries={"max_attempts": 1})
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
print(f"[1225] 1. Deploy {LAMBDA}")
try:
    lam.get_function_configuration(FunctionName=LAMBDA)
    out["create"] = "exists"
    print("  ✓ exists")
except lam.exceptions.ResourceNotFoundException:
    try:
        zip_bytes = build_zip()
        lam.create_function(
            FunctionName=LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler", Code={"ZipFile": zip_bytes},
            Description="Cascade recalibrator — applies learned weights to re-rank",
            Timeout=60, MemorySize=512, Architectures=["x86_64"], Publish=False,
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
print(f"\n[1225] 2. Schedule {SCHEDULE}")
try:
    events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                    Description="Daily cascade recalibration 9:05 ET")
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

# Invoke
print(f"\n[1225] 3. Sync invoke — closes self-improvement loop")
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
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  n_total_candidates: {inner.get('n_total_candidates')}")
            print(f"  n_weights_applied: {inner.get('n_weights_applied')}")
            blend = inner.get("blend") or {}
            print(f"  confidence: {blend.get('confidence')}  blend: {blend.get('original',0):.0%}/{blend.get('calibrated',0):.0%}")
        except Exception:
            pass
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

# Read audit
print(f"\n[1225] 4. Read recalibration audit")
try:
    audit = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cascade-recalibration-audit.json")["Body"].read())
    out["audit"] = {
        "blend": audit.get("blend"),
        "n_predictions": audit.get("calibration_n_predictions"),
        "n_weights": audit.get("n_weights"),
        "top_weights": audit.get("top_weights"),
        "rank_changes": audit.get("rank_changes"),
        "methodology": audit.get("methodology"),
    }
    print(f"  ✓ confidence: {audit.get('blend',{}).get('confidence')}")
    print(f"  ✓ n_weights: {audit.get('n_weights')}")
    rc = audit.get("rank_changes") or {}
    for tier, info in rc.items():
        if isinstance(info, dict):
            print(f"  {tier}: retention {info.get('top_10_retention_pct')}%, "
                  f"avg_delta {info.get('avg_rank_delta')}, max_delta {info.get('max_rank_delta')}")
except Exception as e:
    out["audit"] = {"error": str(e)[:200]}

# Read calibrated cascade
print(f"\n[1225] 5. Read calibrated cascade top picks")
try:
    cal = json.loads(s3.get_object(Bucket=BUCKET, Key="data/theme-cascade-calibrated.json")["Body"].read())
    out["calibrated_cascade"] = {
        "generated_at": cal.get("generated_at"),
        "blend": cal.get("blend"),
        "alert_tier_top5": [{"ticker": c.get("ticker"),
                              "combined": c.get("combined_score"),
                              "orig": c.get("original_combined_score"),
                              "cal": c.get("calibrated_combined_score"),
                              "adj": c.get("calibration_adjustment")}
                             for c in (cal.get("alert_tier") or [])[:5]],
        "laggards_top5": [{"ticker": c.get("ticker"),
                            "combined": c.get("combined_score"),
                            "orig": c.get("original_combined_score"),
                            "cal": c.get("calibrated_combined_score"),
                            "adj": c.get("calibration_adjustment")}
                           for c in (cal.get("laggards_hot_themes") or [])[:5]],
    }
    print(f"  ✓ calibrated cascade output ready")
except Exception as e:
    out["calibrated_cascade"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1225] DONE")
