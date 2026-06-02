"""1180 — ETF Capital Flow Engine rollout.

Steps (idempotent):
  1. Check Lambda exists; create via boto3 if not (workaround for deploy
     pipeline gap on new Lambdas, same pattern as ops 1177).
  2. Patch env from justhodl-equity-research (POLYGON_KEY)
  3. Bucket policy: PublicReadETFFlows
  4. Function URL with NONE auth + CORS
  5. EventBridge schedule cron(0 22 * * ? *) = 17:00 ET daily
  6. Async invoke + poll all 5 outputs land in S3
  7. Re-trigger analytics snapshot to flatten etf_flows table
  8. Verify analytics/etf_flows_flat.json populated
"""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1180_etf_flows_rollout.json"
BUCKET = "justhodl-dashboard-live"
FLOWS_LAMBDA = "justhodl-etf-fund-flows"
SNAPSHOT_LAMBDA = "justhodl-analytics-snapshot"
SOURCE_DIR = "aws/lambdas/justhodl-etf-fund-flows/source"
SHARED_DIR = "aws/shared"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-etf-fund-flows-daily"
SCHEDULE = "cron(0 22 * * ? *)"  # 17:00 ET

cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}


def build_deploy_zip():
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


def pull_polygon_key():
    """Pull POLYGON_KEY from equity-research Lambda (known good source)."""
    try:
        cfg_resp = lam.get_function_configuration(FunctionName="justhodl-equity-research")
        env = (cfg_resp.get("Environment") or {}).get("Variables", {})
        return env.get("POLYGON_KEY")
    except Exception as e:
        print(f"  Couldn't pull POLYGON_KEY: {e}")
        return None


# ═══════════════════════════════════════════════════════════════════
# Step 1: Check if Lambda exists; create if not
# ═══════════════════════════════════════════════════════════════════
print(f"[1180] 1. Check {FLOWS_LAMBDA} exists")
lambda_exists = False
try:
    fn_cfg = lam.get_function_configuration(FunctionName=FLOWS_LAMBDA)
    lambda_exists = True
    out["steps"]["check"] = {
        "exists": True,
        "memory": fn_cfg.get("MemorySize"),
        "timeout": fn_cfg.get("Timeout"),
        "env_keys": list((fn_cfg.get("Environment") or {}).get("Variables", {}).keys()),
    }
    print(f"   ✓ exists")
except lam.exceptions.ResourceNotFoundException:
    print("   ✗ does not exist — creating")
    out["steps"]["check"] = {"exists": False}

if not lambda_exists:
    print(f"\n[1180] 1b. Create {FLOWS_LAMBDA} via boto3")
    try:
        zip_bytes = build_deploy_zip()
        polygon_key = pull_polygon_key()
        env_vars = {"POLYGON_KEY": polygon_key} if polygon_key else {}

        resp = lam.create_function(
            FunctionName=FLOWS_LAMBDA,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            Description="ETF Capital Flow Intelligence — Polygon ETF Global Fund Flows ($99/mo). Daily 80+ ETF flows, z-scores, persistence, 6 institutional composite signals (defensive_rotation, smart_vs_dumb, risk_on_off, domestic_vs_intl, growth_vs_value, credit_stress), regime classifier.",
            Timeout=300,
            MemorySize=1024,
            Environment={"Variables": env_vars},
            Architectures=["x86_64"],
            Publish=False,
        )
        out["steps"]["create"] = {
            "created": True,
            "arn": resp.get("FunctionArn"),
            "polygon_key_set": bool(polygon_key),
        }
        print(f"   ✓ created · polygon_key_set={bool(polygon_key)}")
        for _ in range(30):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=FLOWS_LAMBDA)
            if c.get("State") == "Active":
                break
        lambda_exists = True
    except Exception as e:
        out["steps"]["create"] = {"error": str(e)[:400]}
        print(f"   ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 2: Ensure POLYGON_KEY in env
# ═══════════════════════════════════════════════════════════════════
if lambda_exists:
    print(f"\n[1180] 2. Ensure POLYGON_KEY env")
    try:
        fn_cfg = lam.get_function_configuration(FunctionName=FLOWS_LAMBDA)
        cur_env = (fn_cfg.get("Environment") or {}).get("Variables", {})
        if not cur_env.get("POLYGON_KEY"):
            polygon_key = pull_polygon_key()
            if polygon_key:
                new_env = {**cur_env, "POLYGON_KEY": polygon_key}
                lam.update_function_configuration(
                    FunctionName=FLOWS_LAMBDA, Environment={"Variables": new_env},
                )
                for _ in range(15):
                    time.sleep(2)
                    c = lam.get_function_configuration(FunctionName=FLOWS_LAMBDA)
                    if c.get("LastUpdateStatus") == "Successful":
                        break
                out["steps"]["env_patch"] = {"patched": True}
                print(f"   ✓ patched POLYGON_KEY")
            else:
                out["steps"]["env_patch"] = {"error": "POLYGON_KEY not findable"}
        else:
            out["steps"]["env_patch"] = {"patched": False, "note": "already set"}
            print(f"   ✓ already set (len {len(cur_env['POLYGON_KEY'])})")
    except Exception as e:
        out["steps"]["env_patch"] = {"error": str(e)[:300]}
        print(f"   ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 3: Bucket policy for etf-flows/*
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1180] 3. Bucket policy PublicReadETFFlows")
try:
    cur = s3.get_bucket_policy(Bucket=BUCKET)
    policy = json.loads(cur["Policy"])
    sids = [s.get("Sid", "") for s in policy.get("Statement", [])]
    if "PublicReadETFFlows" not in sids:
        policy["Statement"].append({
            "Sid": "PublicReadETFFlows",
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject"],
            "Resource": [f"arn:aws:s3:::{BUCKET}/etf-flows/*"],
        })
        s3.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps(policy))
        out["steps"]["bucket_policy"] = {"added": True}
        print("   ✓ added")
    else:
        out["steps"]["bucket_policy"] = {"added": False, "note": "already present"}
        print("   ✓ already present")
except Exception as e:
    out["steps"]["bucket_policy"] = {"error": str(e)[:300]}
    print(f"   ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 4: Function URL
# ═══════════════════════════════════════════════════════════════════
if lambda_exists:
    print(f"\n[1180] 4. Function URL")
    try:
        try:
            existing = lam.get_function_url_config(FunctionName=FLOWS_LAMBDA)
            url = existing["FunctionUrl"]
            out["steps"]["function_url"] = {"created": False, "url": url}
        except lam.exceptions.ResourceNotFoundException:
            resp = lam.create_function_url_config(
                FunctionName=FLOWS_LAMBDA,
                AuthType="NONE",
                Cors={
                    "AllowOrigins": ["*"],
                    "AllowMethods": ["GET", "POST"],
                    "AllowHeaders": ["Content-Type"],
                    "MaxAge": 86400,
                },
            )
            url = resp["FunctionUrl"]
            try:
                lam.add_permission(
                    FunctionName=FLOWS_LAMBDA,
                    StatementId="FunctionURLAllowPublicAccess",
                    Action="lambda:InvokeFunctionUrl",
                    Principal="*",
                    FunctionUrlAuthType="NONE",
                )
            except lam.exceptions.ResourceConflictException:
                pass
            out["steps"]["function_url"] = {"created": True, "url": url}
        print(f"   ✓ {out['steps']['function_url']['url']}")
    except Exception as e:
        out["steps"]["function_url"] = {"error": str(e)[:300]}
        print(f"   ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 5: Schedule
# ═══════════════════════════════════════════════════════════════════
if lambda_exists:
    print(f"\n[1180] 5. EventBridge schedule")
    try:
        events.put_rule(
            Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
            Description="17:00 ET daily ETF capital flow refresh",
        )
        fn = lam.get_function(FunctionName=FLOWS_LAMBDA)
        events.put_targets(Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": fn["Configuration"]["FunctionArn"]}])
        try:
            lam.add_permission(
                FunctionName=FLOWS_LAMBDA,
                StatementId=f"EBInvoke-{RULE_NAME}",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}",
            )
        except lam.exceptions.ResourceConflictException:
            pass
        out["steps"]["schedule"] = {"created": True, "expression": SCHEDULE}
        print(f"   ✓ {SCHEDULE}")
    except Exception as e:
        out["steps"]["schedule"] = {"error": str(e)[:300]}
        print(f"   ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 6: Initial invoke + poll
# ═══════════════════════════════════════════════════════════════════
if lambda_exists:
    print(f"\n[1180] 6. Invoke + poll outputs")

    def head_lm(k):
        try:
            return s3.head_object(Bucket=BUCKET, Key=k)["LastModified"]
        except Exception:
            return None

    try:
        invoke_t0 = time.time()
        resp = lam.invoke(FunctionName=FLOWS_LAMBDA, InvocationType="Event", Payload=b"{}")
        invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)
        print(f"   async status={resp['StatusCode']}, polling 5 outputs...")

        expected_keys = [
            "etf-flows/daily.json",
            "etf-flows/composite.json",
            "etf-flows/rotation.json",
            "etf-flows/per-ticker-context.json",
        ]
        seen = set()
        for i in range(80):
            time.sleep(3)
            for k in expected_keys:
                lm = head_lm(k)
                if lm and lm > invoke_dt and k not in seen:
                    seen.add(k)
                    print(f"   ✓ {k} appeared after {round(time.time()-invoke_t0,1)}s")
            if len(seen) == len(expected_keys):
                break

        # Read the 3 main outputs
        elapsed = round(time.time() - invoke_t0, 1)
        invoke_report = {"elapsed_s": elapsed, "outputs_seen": list(seen)}

        try:
            obj = s3.get_object(Bucket=BUCKET, Key="etf-flows/daily.json")
            daily = json.loads(obj["Body"].read())
            invoke_report["daily_summary"] = {
                "universe_size": daily.get("universe_size"),
                "n_ok": daily.get("n_ok"),
                "n_failed": daily.get("n_failed"),
                "elapsed_s": daily.get("elapsed_s"),
                "sample_top_inflows": sorted(
                    [m for m in daily.get("metrics", []) if m.get("flow_zscore_90d") is not None],
                    key=lambda x: x["flow_zscore_90d"] or 0, reverse=True
                )[:5],
                "sample_top_outflows": sorted(
                    [m for m in daily.get("metrics", []) if m.get("flow_zscore_90d") is not None],
                    key=lambda x: x["flow_zscore_90d"] or 0
                )[:5],
                "errors_sample": [m for m in daily.get("metrics", []) if m.get("error")][:3],
            }
        except Exception as e:
            invoke_report["daily_read_error"] = str(e)[:200]

        try:
            obj = s3.get_object(Bucket=BUCKET, Key="etf-flows/composite.json")
            comp = json.loads(obj["Body"].read())
            cc = (comp.get("composite") or {})
            invoke_report["composite_summary"] = {
                "regime": cc.get("regime"),
                "defensive_rotation": cc.get("defensive_rotation", {}).get("score"),
                "smart_vs_dumb": cc.get("smart_vs_dumb", {}).get("score"),
                "risk_on_off": cc.get("risk_on_off", {}).get("score"),
                "domestic_vs_intl": cc.get("domestic_vs_intl", {}).get("score"),
                "growth_vs_value": cc.get("growth_vs_value", {}).get("score"),
                "credit_stress": cc.get("credit_stress", {}).get("score"),
            }
        except Exception as e:
            invoke_report["composite_read_error"] = str(e)[:200]

        out["steps"]["invoke"] = invoke_report
    except Exception as e:
        out["steps"]["invoke"] = {"error": str(e)[:300]}
        print(f"   ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 7: Re-invoke snapshot to flatten etf_flows
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1180] 7. Re-invoke snapshot")
try:
    invoke_t0 = time.time()
    resp = lam.invoke(FunctionName=SNAPSHOT_LAMBDA, InvocationType="Event", Payload=b"{}")
    invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)
    for i in range(40):
        time.sleep(3)
        try:
            lm = s3.head_object(Bucket=BUCKET, Key="analytics/etf_flows_flat.json")["LastModified"]
            if lm > invoke_dt:
                elapsed = round(time.time() - invoke_t0, 1)
                obj = s3.get_object(Bucket=BUCKET, Key="analytics/etf_flows_flat.json")
                doc = json.loads(obj["Body"].read())
                out["steps"]["snapshot_rerun"] = {
                    "elapsed_s": elapsed,
                    "n_rows": len(doc.get("rows", [])),
                    "schema_version": doc.get("schema_version"),
                    "sample_rows": doc.get("rows", [])[:3],
                }
                print(f"   ✓ etf_flows_flat.json: {len(doc.get('rows', []))} rows")
                break
        except Exception:
            pass
    else:
        out["steps"]["snapshot_rerun"] = {"error": "poll timeout"}
except Exception as e:
    out["steps"]["snapshot_rerun"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1180] DONE")
