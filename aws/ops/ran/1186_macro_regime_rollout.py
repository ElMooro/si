"""1186 — Phase 2 Macro Regime Engine rollout."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1186_macro_regime_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-macro-regime"
SOURCE_DIR = "aws/lambdas/justhodl-macro-regime/source"
SHARED_DIR = "aws/shared"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-macro-regime-daily"
SCHEDULE = "cron(15 22 * * ? *)"

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


# Step 1: create
print("[1186] 1. Check / create macro-regime Lambda")
exists = False
try:
    lam.get_function_configuration(FunctionName=LAMBDA)
    exists = True
    out["steps"]["check"] = {"exists": True}
except lam.exceptions.ResourceNotFoundException:
    try:
        zip_bytes = build_zip()
        polygon = pull_polygon()
        env_vars = {"POLYGON_KEY": polygon} if polygon else {}
        resp = lam.create_function(
            FunctionName=LAMBDA, Runtime="python3.12", Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler", Code={"ZipFile": zip_bytes},
            Description="Multi-asset Macro Regime Engine. VIX + futures + FX, classifies 6 sub-regimes + top-level macro regime tag.",
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

# Step 2: bucket policy for macro/*
print(f"\n[1186] 2. Bucket policy PublicReadMacro")
try:
    cur = s3.get_bucket_policy(Bucket=BUCKET)
    policy = json.loads(cur["Policy"])
    sids = [s.get("Sid", "") for s in policy.get("Statement", [])]
    if "PublicReadMacro" not in sids:
        policy["Statement"].append({
            "Sid": "PublicReadMacro", "Effect": "Allow", "Principal": "*",
            "Action": ["s3:GetObject"], "Resource": [f"arn:aws:s3:::{BUCKET}/macro/*"],
        })
        s3.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps(policy))
        out["steps"]["bucket_policy"] = {"added": True}
    else:
        out["steps"]["bucket_policy"] = {"already": True}
    print(f"  ✓")
except Exception as e:
    out["steps"]["bucket_policy"] = {"error": str(e)[:200]}

# Step 3: Function URL + Schedule
if exists:
    print(f"\n[1186] 3. Function URL + schedule")
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
                        Description="17:15 ET daily macro regime refresh")
        fn = lam.get_function(FunctionName=LAMBDA)
        events.put_targets(Rule=RULE_NAME, Targets=[{"Id":"1","Arn":fn["Configuration"]["FunctionArn"]}])
        try:
            lam.add_permission(FunctionName=LAMBDA, StatementId=f"EBInvoke-{RULE_NAME}",
                               Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                               SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}")
        except lam.exceptions.ResourceConflictException:
            pass
        out["steps"]["schedule"] = {"created": True}
        print(f"  ✓ schedule {SCHEDULE}")
    except Exception as e:
        out["steps"]["url_schedule"] = {"error": str(e)[:300]}

# Step 4: Sync invoke
if exists:
    print(f"\n[1186] 4. Sync invoke (~20-40s for 24 assets in parallel)")
    try:
        invoke_t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
        elapsed = round(time.time() - invoke_t0, 1)
        payload = resp.get("Payload").read().decode()
        out["steps"]["invoke"] = {
            "elapsed_s": elapsed,
            "status_code": resp.get("StatusCode"),
            "function_error": resp.get("FunctionError"),
            "body": payload[:1500],
        }
        print(f"  StatusCode={resp.get('StatusCode')} elapsed={elapsed}s")
        if resp.get('FunctionError'):
            print(f"  ⚠ {payload[:500]}")
        # Read output
        try:
            doc = json.loads(s3.get_object(Bucket=BUCKET, Key="macro/regime.json")["Body"].read())
            out["steps"]["macro_regime_output"] = {
                "top_level_regime": doc.get("top_level_regime"),
                "sub_regimes": {k: {"label": v.get("label"), "score": v.get("score")} for k, v in (doc.get("sub_regimes") or {}).items()},
                "n_ok": doc.get("n_ok"),
                "universe_size": doc.get("universe_size"),
                "sample_metrics": [
                    {k: v for k, v in m.items() if k in ["ticker","name","role","feed","latest_close","ret_1d_pct","ret_5d_pct","ret_21d_pct","zscore_90d"]}
                    for m in (doc.get("asset_metrics") or [])
                ][:8],
                "errors": [m for m in (doc.get("asset_metrics") or []) if m.get("error")][:5],
            }
            print(f"  ✓ regime={doc.get('top_level_regime',{}).get('regime')} confidence={doc.get('top_level_regime',{}).get('confidence')}")
        except Exception as e:
            out["steps"]["macro_regime_output"] = {"error": str(e)[:300]}
    except Exception as e:
        out["steps"]["invoke"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1186] DONE")
