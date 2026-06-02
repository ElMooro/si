"""1205 — Theme cascade Lambda rollout + verify."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1205_theme_cascade_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-theme-cascade"
SOURCE_DIR = "aws/lambdas/justhodl-theme-cascade/source"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-theme-cascade-twice-per-hour"
SCHEDULE = "cron(20,50 * * * ? *)"

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
print("[1205] 1. Check / create Lambda")
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
            Description="Theme cascade synthesizer — theme heat × accumulation × ETF flow",
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
    print(f"\n[1205] 2. URL + schedule")
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
                        Description="Every 30min theme cascade synthesis")
        fn = lam.get_function(FunctionName=LAMBDA)
        events.put_targets(Rule=RULE_NAME, Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
        try:
            lam.add_permission(FunctionName=LAMBDA, StatementId=f"EBInvoke-{RULE_NAME}",
                               Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                               SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}")
        except lam.exceptions.ResourceConflictException:
            pass
        print(f"  ✓ url + schedule")
    except Exception as e:
        out["url_schedule_err"] = str(e)[:300]

# Step 3: Sync invoke
if exists:
    print(f"\n[1205] 3. Sync invoke")
    try:
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
        elapsed = round(time.time() - t0, 1)
        payload = resp.get("Payload").read().decode()
        out["invoke"] = {
            "elapsed_s": elapsed,
            "status": resp.get("StatusCode"),
            "function_error": resp.get("FunctionError"),
            "body": payload[:1500],
        }
        print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")

        # Read full output
        try:
            doc = json.loads(s3.get_object(Bucket=BUCKET, Key="data/theme-cascade.json")["Body"].read())
            out["theme_cascade_doc"] = {
                "generated_at": doc.get("generated_at"),
                "macro_regime": doc.get("macro_regime"),
                "n_themes_tracked": doc.get("n_themes_tracked"),
                "n_total_ranked": doc.get("n_total_ranked"),
                "n_alert_tier": doc.get("n_alert_tier"),
                "n_medium_tier": doc.get("n_medium_tier"),
                "n_watch_tier": doc.get("n_watch_tier"),
                "top_hot_themes": doc.get("top_hot_themes", [])[:10],
                "alert_tier": doc.get("alert_tier", [])[:15],
                "medium_tier": doc.get("medium_tier", [])[:15],
            }
            print(f"\n  ✓ themes tracked: {doc.get('n_themes_tracked')}")
            print(f"  ✓ alert_tier: {doc.get('n_alert_tier')}  medium: {doc.get('n_medium_tier')}  watch: {doc.get('n_watch_tier')}")
            print(f"  ✓ top hot themes:")
            for t in (doc.get("top_hot_themes") or [])[:5]:
                print(f"    {t.get('theme'):30s} mult=x{t.get('multiplier'):.2f}  rs_rank=#{t.get('rs_rank')}  factors={t.get('factors')}")
        except Exception as e:
            out["theme_cascade_doc"] = {"error": str(e)[:300]}
    except Exception as e:
        out["invoke"] = {"error": str(e)[:300]}


out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1205] DONE")
