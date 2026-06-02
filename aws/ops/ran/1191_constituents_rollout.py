"""1191 — Constituent Pull-Through Lambda rollout + verify."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1191_constituents_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-etf-constituents"
SOURCE_DIR = "aws/lambdas/justhodl-etf-constituents/source"
SHARED_DIR = "aws/shared"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-etf-constituents-daily"
SCHEDULE = "cron(45 22 * * ? *)"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        if os.path.isdir(SHARED_DIR):
            for f in os.listdir(SHARED_DIR):
                if f.endswith(".py") and not f.startswith("__"):
                    fpath = os.path.join(SHARED_DIR, f)
                    if os.path.isfile(fpath):
                        zf.write(fpath, arcname=f)
        for root, _, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"):
                    continue
                fpath = os.path.join(root, f)
                rel = os.path.relpath(fpath, SOURCE_DIR)
                zf.write(fpath, arcname=rel)
    return buf.getvalue()


def pull_polygon():
    try:
        cfg_resp = lam.get_function_configuration(FunctionName="justhodl-etf-fund-flows")
        env = (cfg_resp.get("Environment") or {}).get("Variables", {})
        return env.get("POLYGON_KEY")
    except Exception:
        return None


# Step 1: create or update
print("[1191] 1. Check / create constituents Lambda")
exists = False
try:
    lam.get_function_configuration(FunctionName=LAMBDA)
    exists = True
    print("  ✓ exists")
    out["steps"]["check"] = {"exists": True}
except lam.exceptions.ResourceNotFoundException:
    try:
        zip_bytes = build_zip()
        polygon = pull_polygon()
        env_vars = {"POLYGON_KEY": polygon} if polygon else {}
        resp = lam.create_function(
            FunctionName=LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler", Code={"ZipFile": zip_bytes},
            Description="ETF Constituent Pull-Through. Maps high-z ETF flows to underlying stock pressure via Polygon ETF Global Constituents endpoint.",
            Timeout=300, MemorySize=1024, Environment={"Variables": env_vars},
            Architectures=["x86_64"], Publish=False,
        )
        out["steps"]["create"] = {"created": True, "polygon_set": bool(polygon)}
        for _ in range(30):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=LAMBDA)
            if c.get("State") == "Active":
                break
        exists = True
        print(f"  ✓ created · polygon_set={bool(polygon)}")
    except Exception as e:
        out["steps"]["create"] = {"error": str(e)[:400]}
        print(f"  ❌ {e}")

# Step 2: function URL + schedule
if exists:
    print(f"\n[1191] 2. Function URL + schedule")
    try:
        try:
            url = lam.get_function_url_config(FunctionName=LAMBDA)["FunctionUrl"]
        except lam.exceptions.ResourceNotFoundException:
            r = lam.create_function_url_config(
                FunctionName=LAMBDA, AuthType="NONE",
                Cors={"AllowOrigins": ["*"], "AllowMethods": ["GET","POST"], "AllowHeaders": ["Content-Type"], "MaxAge": 86400},
            )
            url = r["FunctionUrl"]
            try:
                lam.add_permission(FunctionName=LAMBDA, StatementId="FunctionURLAllowPublicAccess",
                                   Action="lambda:InvokeFunctionUrl", Principal="*", FunctionUrlAuthType="NONE")
            except lam.exceptions.ResourceConflictException:
                pass
        out["steps"]["url"] = {"url": url}

        events.put_rule(Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
                        Description="17:45 ET daily constituent pressure")
        fn = lam.get_function(FunctionName=LAMBDA)
        events.put_targets(Rule=RULE_NAME, Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
        try:
            lam.add_permission(FunctionName=LAMBDA, StatementId=f"EBInvoke-{RULE_NAME}",
                               Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                               SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}")
        except lam.exceptions.ResourceConflictException:
            pass
        out["steps"]["schedule"] = {"created": True, "expression": SCHEDULE}
        print(f"  ✓ schedule")
    except Exception as e:
        out["steps"]["url_schedule"] = {"error": str(e)[:300]}

# Step 3: sync invoke
if exists:
    print(f"\n[1191] 3. Sync invoke (fetches ~10 high-z ETFs in parallel)")
    try:
        invoke_t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
        elapsed = round(time.time() - invoke_t0, 1)
        payload = resp.get("Payload").read().decode()
        out["steps"]["invoke"] = {
            "elapsed_s": elapsed,
            "status": resp.get("StatusCode"),
            "function_error": resp.get("FunctionError"),
            "body": payload[:1000],
        }
        print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
        if resp.get("FunctionError"):
            print(f"  ⚠ {payload[:400]}")

        # Read full output
        try:
            doc = json.loads(s3.get_object(Bucket=BUCKET, Key="etf-flows/constituent-pressure.json")["Body"].read())
            out["steps"]["constituent_output"] = {
                "generated_at": doc.get("generated_at"),
                "n_high_z_etfs": doc.get("n_high_z_etfs"),
                "n_etfs_with_constituents": doc.get("n_etfs_with_constituents"),
                "threshold_z": doc.get("threshold_z"),
                "high_z_etfs": doc.get("high_z_etfs", [])[:10],
                "top_20_pressure": doc.get("top_constituents_by_pressure", [])[:20],
            }
            print(f"  ✓ {doc.get('n_high_z_etfs')} high-z ETFs · {len(doc.get('top_constituents_by_pressure') or [])} stocks pressured")
        except Exception as e:
            out["steps"]["constituent_output"] = {"error": str(e)[:300]}
    except Exception as e:
        out["steps"]["invoke"] = {"error": str(e)[:300]}

# Step 4: ALSO try to read 1190 results if it landed
print(f"\n[1191] 4. Check 1190 backtest report if available")
try:
    obj = s3.get_object(Bucket=BUCKET, Key="backtest/report.json")
    doc = json.loads(obj["Body"].read())
    ra = doc.get("regime_attribution") or {}
    out["steps"]["backtest_regime_status"] = {
        "generated_at": doc.get("generated_at"),
        "n_calls_with_alpha": doc.get("n_calls_with_alpha"),
        "n_calls_with_regime_tag": (ra.get("regime_coverage") or {}).get("n_calls_with_regime_tag"),
        "pct_coverage": (ra.get("regime_coverage") or {}).get("pct_coverage"),
        "regimes_observed": (ra.get("regime_coverage") or {}).get("regimes_observed"),
        "by_regime": ra.get("by_regime", [])[:5],
    }
    print(f"  ✓ backtest generated_at={doc.get('generated_at','')[:16]} coverage={ra.get('regime_coverage',{}).get('pct_coverage')}%")
except Exception as e:
    out["steps"]["backtest_regime_status"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1191] DONE")
