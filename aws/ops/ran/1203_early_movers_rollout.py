"""1203 — Roll out justhodl-early-movers Lambda + verify."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1203_early_movers_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-early-movers"
SOURCE_DIR = "aws/lambdas/justhodl-early-movers/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-early-movers-twice-per-hour"
SCHEDULE = "cron(15,45 * * * ? *)"

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


# Step 1: Create or update
print("[1203] 1. Check / create Lambda")
exists = False
try:
    lam.get_function_configuration(FunctionName=LAMBDA)
    exists = True
    out["create"] = {"exists": True}
    print(f"  ✓ exists")
except lam.exceptions.ResourceNotFoundException:
    try:
        zip_bytes = build_zip()
        resp = lam.create_function(
            FunctionName=LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler", Code={"ZipFile": zip_bytes},
            Description="Early Movers extractor — acceleration-based pump detection (response to MRVL +29% case study).",
            Timeout=60, MemorySize=512, Architectures=["x86_64"], Publish=False,
        )
        out["create"] = {"created": True}
        for _ in range(30):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=LAMBDA)
            if c.get("State") == "Active":
                break
        exists = True
        print(f"  ✓ created")
    except Exception as e:
        out["create"] = {"error": str(e)[:400]}

# Step 2: Function URL + schedule
if exists:
    print(f"\n[1203] 2. URL + schedule")
    try:
        try:
            url = lam.get_function_url_config(FunctionName=LAMBDA)["FunctionUrl"]
        except lam.exceptions.ResourceNotFoundException:
            r = lam.create_function_url_config(
                FunctionName=LAMBDA, AuthType="NONE",
                Cors={"AllowOrigins": ["*"], "AllowMethods": ["GET","POST"],
                      "AllowHeaders": ["Content-Type"], "MaxAge": 86400},
            )
            url = r["FunctionUrl"]
            try:
                lam.add_permission(FunctionName=LAMBDA, StatementId="FunctionURLAllowPublicAccess",
                                   Action="lambda:InvokeFunctionUrl", Principal="*",
                                   FunctionUrlAuthType="NONE")
            except lam.exceptions.ResourceConflictException:
                pass
        out["url"] = url
        events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                        Description="Every 30min early movers extraction")
        fn = lam.get_function(FunctionName=LAMBDA)
        events.put_targets(Rule=RULE_NAME, Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
        try:
            lam.add_permission(FunctionName=LAMBDA, StatementId=f"EBInvoke-{RULE_NAME}",
                               Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                               SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}")
        except lam.exceptions.ResourceConflictException:
            pass
        out["schedule"] = {"expr": SCHEDULE}
        print(f"  ✓ schedule + url")
    except Exception as e:
        out["url_schedule_err"] = str(e)[:300]

# Step 3: Sync invoke
if exists:
    print(f"\n[1203] 3. Sync invoke")
    try:
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
        elapsed = round(time.time() - t0, 1)
        payload = resp.get("Payload").read().decode()
        out["invoke"] = {
            "elapsed_s": elapsed,
            "status": resp.get("StatusCode"),
            "function_error": resp.get("FunctionError"),
            "body": payload[:2000],
        }
        print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")

        # Read full output
        try:
            doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/early-movers.json")["Body"].read())
            out["early_movers_doc"] = {
                "generated_at": doc.get("generated_at"),
                "n_radar_items": doc.get("n_radar_items"),
                "n_early_movers": doc.get("n_early_movers"),
                "categories": doc.get("categories"),
                "alert_tier_count": len(doc.get("alert_tier") or []),
                "top_15": [
                    {
                        "ticker": m.get("ticker"),
                        "early_score": m.get("early_score"),
                        "pump_category": m.get("pump_category"),
                        "convergence_score": m.get("convergence_score"),
                        "n_engines": m.get("n_engines"),
                        "prior_n_engines": m.get("prior_n_engines"),
                        "factors": m.get("factors"),
                        "earliness_signatures": m.get("earliness_signatures"),
                    }
                    for m in (doc.get("top_early_movers") or [])[:15]
                ],
                "would_mrvl_have_been_visible": (
                    "MRVL" in [m["ticker"] for m in (doc.get("top_early_movers") or [])[:5]]
                ),
                "mrvl_rank_in_early_movers": next(
                    (i + 1 for i, m in enumerate(doc.get("top_early_movers") or [])
                     if m.get("ticker") == "MRVL"), None,
                ),
            }
            print(f"\n  📊 RESULTS:")
            print(f"    n_early_movers: {doc.get('n_early_movers')}")
            print(f"    alert_tier (score>=35): {len(doc.get('alert_tier') or [])}")
            print(f"    Would MRVL have been visible in top 5? {out['early_movers_doc']['would_mrvl_have_been_visible']}")
            print(f"    MRVL rank in early-movers list: #{out['early_movers_doc']['mrvl_rank_in_early_movers']}")
        except Exception as e:
            out["early_movers_doc"] = {"error": str(e)[:300]}
    except Exception as e:
        out["invoke"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1203] DONE")
