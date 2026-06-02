"""1196 — Final institutional stack rollout.

Steps:
  1. Pull TELEGRAM_TOKEN from existing justhodl-anomaly-detector Lambda
  2. Set TELEGRAM_TOKEN env on justhodl-flows-ai-analysis (so digest works)
  3. Create justhodl-flow-anomaly-detector Lambda (new)
  4. Set up function URL + EventBridge schedule for new Lambda
  5. Sync invoke flow-anomaly to verify it works
  6. Sync invoke flows-ai to verify Telegram digest delivery
  7. Show anomaly output + Telegram result
"""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1196_final_stack_rollout.json"
BUCKET = "justhodl-dashboard-live"

# New Lambda
ANOMALY_LAMBDA = "justhodl-flow-anomaly-detector"
ANOMALY_SOURCE = "aws/lambdas/justhodl-flow-anomaly-detector/source"
ANOMALY_RULE = "justhodl-flow-anomaly-detector-daily"
ANOMALY_SCHEDULE = "cron(0 23 * * ? *)"

# AI Lambda needs TELEGRAM_TOKEN
AI_LAMBDA = "justhodl-flows-ai-analysis"

ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"

cfg = Config(read_timeout=420, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}


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


# Step 1: Pull TELEGRAM_TOKEN from anomaly-detector v2
print("[1196] 1. Pull TELEGRAM_TOKEN from justhodl-anomaly-detector v2")
try:
    c = lam.get_function_configuration(FunctionName="justhodl-anomaly-detector")
    env = (c.get("Environment") or {}).get("Variables", {})
    telegram_token = env.get("TELEGRAM_TOKEN")
    if not telegram_token:
        out["steps"]["pull_token"] = {"error": "TELEGRAM_TOKEN not in anomaly-detector env"}
        print("  ❌ no TELEGRAM_TOKEN")
    else:
        out["steps"]["pull_token"] = {"ok": True, "len": len(telegram_token)}
        print(f"  ✓ TELEGRAM_TOKEN len={len(telegram_token)}")
except Exception as e:
    out["steps"]["pull_token"] = {"error": str(e)[:300]}
    print(f"  ❌ {e}")
    telegram_token = None


# Step 2: Set TELEGRAM_TOKEN on flows-ai-analysis
if telegram_token:
    print(f"\n[1196] 2. Set TELEGRAM_TOKEN on {AI_LAMBDA}")
    try:
        c = lam.get_function_configuration(FunctionName=AI_LAMBDA)
        cur_env = (c.get("Environment") or {}).get("Variables", {})
        new_env = dict(cur_env)
        new_env["TELEGRAM_TOKEN"] = telegram_token
        lam.update_function_configuration(
            FunctionName=AI_LAMBDA,
            Environment={"Variables": new_env},
        )
        # Wait for update
        for _ in range(20):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=AI_LAMBDA)
            if c.get("LastUpdateStatus") == "Successful":
                break
        out["steps"]["set_token_on_ai"] = {
            "ok": True,
            "env_keys_now": sorted(new_env.keys()),
        }
        print(f"  ✓ env updated · keys: {sorted(new_env.keys())}")
    except Exception as e:
        out["steps"]["set_token_on_ai"] = {"error": str(e)[:300]}
        print(f"  ❌ {e}")


# Step 3: Create flow-anomaly-detector Lambda
print(f"\n[1196] 3. Create / verify {ANOMALY_LAMBDA}")
exists = False
try:
    lam.get_function_configuration(FunctionName=ANOMALY_LAMBDA)
    exists = True
    out["steps"]["create_anomaly"] = {"exists": True}
    print(f"  ✓ exists")
except lam.exceptions.ResourceNotFoundException:
    try:
        zip_bytes = build_zip(ANOMALY_SOURCE)
        resp = lam.create_function(
            FunctionName=ANOMALY_LAMBDA,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            Description="ETF flow-specific anomaly detector. 5 detectors covering extreme flow, persistence, constituent divergence, regime velocity, cross-timeframe divergence.",
            Timeout=120,
            MemorySize=512,
            Architectures=["x86_64"],
            Publish=False,
        )
        out["steps"]["create_anomaly"] = {"created": True, "arn": resp.get("FunctionArn")}
        for _ in range(30):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=ANOMALY_LAMBDA)
            if c.get("State") == "Active":
                break
        exists = True
        print(f"  ✓ created")
    except Exception as e:
        out["steps"]["create_anomaly"] = {"error": str(e)[:400]}
        print(f"  ❌ {e}")


# Step 4: Function URL + schedule for flow-anomaly
if exists:
    print(f"\n[1196] 4. Function URL + schedule for {ANOMALY_LAMBDA}")
    try:
        try:
            url = lam.get_function_url_config(FunctionName=ANOMALY_LAMBDA)["FunctionUrl"]
        except lam.exceptions.ResourceNotFoundException:
            r = lam.create_function_url_config(
                FunctionName=ANOMALY_LAMBDA, AuthType="NONE",
                Cors={"AllowOrigins": ["*"], "AllowMethods": ["GET","POST"],
                      "AllowHeaders": ["Content-Type"], "MaxAge": 86400},
            )
            url = r["FunctionUrl"]
            try:
                lam.add_permission(
                    FunctionName=ANOMALY_LAMBDA,
                    StatementId="FunctionURLAllowPublicAccess",
                    Action="lambda:InvokeFunctionUrl",
                    Principal="*",
                    FunctionUrlAuthType="NONE",
                )
            except lam.exceptions.ResourceConflictException:
                pass
        out["steps"]["anomaly_url"] = {"url": url}

        events.put_rule(
            Name=ANOMALY_RULE,
            ScheduleExpression=ANOMALY_SCHEDULE,
            State="ENABLED",
            Description="18:00 ET daily flow anomaly detection",
        )
        fn = lam.get_function(FunctionName=ANOMALY_LAMBDA)
        events.put_targets(
            Rule=ANOMALY_RULE,
            Targets=[{"Id": "1", "Arn": fn["Configuration"]["FunctionArn"]}],
        )
        try:
            lam.add_permission(
                FunctionName=ANOMALY_LAMBDA,
                StatementId=f"EBInvoke-{ANOMALY_RULE}",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{ANOMALY_RULE}",
            )
        except lam.exceptions.ResourceConflictException:
            pass
        out["steps"]["anomaly_schedule"] = {"created": True, "expression": ANOMALY_SCHEDULE}
        print(f"  ✓ url + schedule")
    except Exception as e:
        out["steps"]["anomaly_url_schedule"] = {"error": str(e)[:300]}


# Step 5: Sync invoke flow-anomaly-detector
if exists:
    print(f"\n[1196] 5. Sync invoke {ANOMALY_LAMBDA}")
    try:
        t0 = time.time()
        resp = lam.invoke(
            FunctionName=ANOMALY_LAMBDA,
            InvocationType="RequestResponse",
            Payload=b"{}",
        )
        elapsed = round(time.time() - t0, 1)
        payload = resp.get("Payload").read().decode()
        out["steps"]["anomaly_invoke"] = {
            "elapsed_s": elapsed,
            "status": resp.get("StatusCode"),
            "function_error": resp.get("FunctionError"),
            "body": payload[:1500],
        }
        print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
        if resp.get("FunctionError"):
            print(f"  ⚠ {payload[:400]}")

        # Read full output
        try:
            doc = json.loads(s3.get_object(
                Bucket=BUCKET, Key="flow-anomalies/daily.json"
            )["Body"].read())
            out["steps"]["anomaly_output"] = {
                "generated_at": doc.get("generated_at"),
                "n_total": doc.get("n_total"),
                "n_alerts_high_sev": doc.get("n_alerts_high_sev"),
                "by_type_count": doc.get("by_type_count"),
                "top_10_anomalies": [
                    {
                        "type": a["type"],
                        "severity": a["severity"],
                        "subject": a["subject"],
                        "description": a["description"][:200],
                    }
                    for a in doc.get("anomalies", [])[:10]
                ],
            }
            print(f"  ✓ {doc.get('n_total')} anomalies · {doc.get('n_alerts_high_sev')} high-sev")
        except Exception as e:
            out["steps"]["anomaly_output"] = {"error": str(e)[:300]}
    except Exception as e:
        out["steps"]["anomaly_invoke"] = {"error": str(e)[:300]}


# Step 6: Sync invoke flows-ai (triggers Telegram digest)
print(f"\n[1196] 6. Sync invoke {AI_LAMBDA} (will push Telegram digest)")
try:
    t0 = time.time()
    resp = lam.invoke(
        FunctionName=AI_LAMBDA,
        InvocationType="RequestResponse",
        Payload=b"{}",
    )
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    # Parse outer body
    try:
        parsed = json.loads(payload)
        body = json.loads(parsed.get("body", "{}"))
    except Exception:
        body = {"raw": payload[:500]}
    out["steps"]["ai_invoke"] = {
        "elapsed_s": elapsed,
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body": body,
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:400]}")
    else:
        tg = body.get("telegram") or {}
        print(f"  Telegram: sent={tg.get('sent')} chars={tg.get('chars')} reason={tg.get('reason')}")
except Exception as e:
    out["steps"]["ai_invoke"] = {"error": str(e)[:300]}


out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1196] DONE")
