"""1173 — Diagnose why critique Lambda is returning 502.

Hypothesis: env vars (ANTHROPIC_API_KEY) weren't populated by the deploy
pipeline since this is a brand-new Lambda. Need to check + fix.
"""
import json
import time
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1173_critique_diagnose.json"
LAMBDA_NAME = "justhodl-research-critique"

lam = boto3.client("lambda", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")

out = {"started": datetime.now(timezone.utc).isoformat()}

# 1. Check Lambda configuration — env vars present?
print("[1173] 1. Lambda env vars")
try:
    fn = lam.get_function(FunctionName=LAMBDA_NAME)
    cfg = fn["Configuration"]
    env_vars = (cfg.get("Environment") or {}).get("Variables", {})
    out["lambda_config"] = {
        "memory_mb": cfg.get("MemorySize"),
        "timeout_s": cfg.get("Timeout"),
        "runtime":   cfg.get("Runtime"),
        "env_keys":  list(env_vars.keys()),
        "has_anthropic_key": "ANTHROPIC_API_KEY" in env_vars,
        "anthropic_key_len": len(env_vars.get("ANTHROPIC_API_KEY", "")),
        "has_openai_key": "OPENAI_API_KEY" in env_vars,
        "last_modified": cfg.get("LastModified"),
    }
    print(f"   env keys: {list(env_vars.keys())}")
    print(f"   has ANTHROPIC_API_KEY: {bool(env_vars.get('ANTHROPIC_API_KEY'))}")
except Exception as e:
    out["lambda_config"] = {"error": str(e)[:200]}

# 2. Tail CloudWatch logs for the actual error
print("\n[1173] 2. Tail CloudWatch logs")
try:
    streams = logs.describe_log_streams(
        logGroupName=f"/aws/lambda/{LAMBDA_NAME}",
        orderBy="LastEventTime", descending=True, limit=3,
    )["logStreams"]
    out["recent_log_events"] = []
    for s in streams[:2]:
        evs = logs.get_log_events(
            logGroupName=f"/aws/lambda/{LAMBDA_NAME}",
            logStreamName=s["logStreamName"],
            limit=30, startFromHead=False,
        )["events"]
        for e in evs[-15:]:
            msg = e["message"].strip()
            if not msg.startswith("REPORT") and not msg.startswith("START"):
                out["recent_log_events"].append({
                    "ts": datetime.fromtimestamp(e["timestamp"]/1000, timezone.utc).isoformat(),
                    "msg": msg[:600],
                })
    print(f"   Found {len(out['recent_log_events'])} log lines")
    for line in out["recent_log_events"][-8:]:
        print(f"     {line['msg'][:200]}")
except Exception as e:
    out["recent_log_events"] = [{"error": str(e)[:200]}]
    print(f"   ❌ logs: {e}")

# 3. If env var is missing, pull it from SSM and patch the Lambda
print("\n[1173] 3. Fix env vars if missing")
has_key = out.get("lambda_config", {}).get("has_anthropic_key", False)
if not has_key:
    print("   ANTHROPIC_API_KEY missing — pulling from SSM /justhodl/anthropic/api-key")
    try:
        # Standard SSM path the other Lambdas use
        param = ssm.get_parameter(
            Name="/justhodl/anthropic/api-key", WithDecryption=True,
        )
        api_key = param["Parameter"]["Value"]
        out["ssm_key"] = {"found": True, "key_len": len(api_key)}

        # Patch the Lambda env
        cur = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
        existing_env = (cur.get("Environment") or {}).get("Variables", {})
        existing_env["ANTHROPIC_API_KEY"] = api_key
        lam.update_function_configuration(
            FunctionName=LAMBDA_NAME,
            Environment={"Variables": existing_env},
        )
        out["env_patched"] = True
        print("   ✓ patched ANTHROPIC_API_KEY into Lambda env")

        # Wait for the update to settle
        time.sleep(5)
        for _ in range(20):
            cfg = lam.get_function_configuration(FunctionName=LAMBDA_NAME)
            if cfg.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        print(f"   update status: {cfg.get('LastUpdateStatus')}")
    except Exception as e:
        out["ssm_key"] = {"error": str(e)[:300]}
        out["env_patched"] = False
        print(f"   ❌ {e}")
else:
    print(f"   ANTHROPIC_API_KEY present (len {out['lambda_config']['anthropic_key_len']})")
    out["env_patched"] = "not_needed"

# 4. Retry smoke
print("\n[1173] 4. Retry smoke after env fix")
try:
    t0 = time.time()
    resp = lam.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
        Payload=json.dumps({"ticker": "AAPL"}).encode(),
    )
    elapsed = round(time.time() - t0, 1)
    payload = json.loads(resp["Payload"].read().decode())
    body = json.loads(payload.get("body", "{}")) if isinstance(payload.get("body"), str) else payload.get("body", {})
    out["smoke_retry"] = {
        "elapsed_s": elapsed,
        "status_code": payload.get("statusCode"),
        "model_used": (body.get("critic") or {}).get("model"),
        "disagreement_score": (body.get("critique") or {}).get("disagreement_score"),
        "alternative_rating": (body.get("critique") or {}).get("alternative_rating"),
        "key_disagreement": (body.get("critique") or {}).get("key_disagreement_1liner"),
        "error_in_body": body.get("error"),
    }
    print(f"   {payload.get('statusCode')} in {elapsed}s")
    if (body.get("critique") or {}).get("disagreement_score") is not None:
        print(f"   ✓ disagreement={body['critique']['disagreement_score']} "
              f"alt={body['critique'].get('alternative_rating')}")
        print(f"     '{(body['critique'] or {}).get('key_disagreement_1liner','')[:150]}'")
    else:
        print(f"   ⚠ no critique data; error: {body.get('error')}")
except Exception as e:
    out["smoke_retry"] = {"error": str(e)[:300]}
    print(f"   ❌ {e}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print("[1173] DONE")
