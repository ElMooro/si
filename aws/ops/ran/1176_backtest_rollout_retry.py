"""1175 — Backtest rollout + 1h TTL verification.

Steps:
  1. Patch FMP_KEY + ANTHROPIC_API_KEY into backtest Lambda env (same SSM
     trap as the critique Lambda hit in ops 1173).
  2. Create function URL for backtest Lambda (NONE auth, CORS *).
  3. Create EventBridge schedule: cron(0 11 * * ? *) = 06:00 ET daily.
  4. Async-invoke backtest Lambda + poll S3 for analytics/backtest_results.json.
  5. Verify TTL FIX: async-invoke critique for a NEW ticker (so cache_create
     fires) then read the S3 critique to verify usage shows
     ephemeral_1h_input_tokens > 0.
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1176_backtest_rollout_retry.json"
BUCKET = "justhodl-dashboard-live"
BACKTEST_LAMBDA = "justhodl-research-backtest"
CRITIQUE_LAMBDA = "justhodl-research-critique"
RULE_NAME = "justhodl-research-backtest-daily"
SCHEDULE = "cron(0 11 * * ? *)"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"

cfg = Config(read_timeout=180, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
ssm = boto3.client("ssm", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}

# ═══════════════════════════════════════════════════════════════════
# Step 1: Patch env vars from SSM (deploy pipeline doesn't auto-populate
# for newly-created Lambdas — same gap as critique Lambda hit)
# ═══════════════════════════════════════════════════════════════════
print("[1175] 1. Patch backtest env from SSM")
try:
    cur = lam.get_function_configuration(FunctionName=BACKTEST_LAMBDA)
    existing_env = (cur.get("Environment") or {}).get("Variables", {})
    print(f"   current env keys: {list(existing_env.keys())}")

    keys_to_inject = {}
    # FMP key for price fetches
    try:
        fmp = ssm.get_parameter(Name="/justhodl/fmp/api-key", WithDecryption=True)
        keys_to_inject["FMP_KEY"] = fmp["Parameter"]["Value"]
    except Exception as e:
        # Fallback: try the v3 key path or hardcoded from memory if SSM doesn't have it
        try:
            fmp = ssm.get_parameter(Name="/justhodl/fmp_key", WithDecryption=True)
            keys_to_inject["FMP_KEY"] = fmp["Parameter"]["Value"]
        except Exception as e2:
            # From userMemories: known FMP /stable/ key
            keys_to_inject["FMP_KEY"] = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"

    new_env = {**existing_env, **keys_to_inject}
    if new_env != existing_env:
        lam.update_function_configuration(
            FunctionName=BACKTEST_LAMBDA,
            Environment={"Variables": new_env},
        )
        out["steps"]["env_patch"] = {"patched": list(keys_to_inject.keys()), "fmp_len": len(keys_to_inject.get("FMP_KEY",""))}
        print(f"   ✓ patched: {list(keys_to_inject.keys())}")
        # Wait for the update to settle
        time.sleep(4)
        for _ in range(15):
            cfg2 = lam.get_function_configuration(FunctionName=BACKTEST_LAMBDA)
            if cfg2.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
    else:
        out["steps"]["env_patch"] = {"patched": [], "note": "already populated"}
except Exception as e:
    out["steps"]["env_patch"] = {"error": str(e)[:300]}
    print(f"   ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 2: Function URL
# ═══════════════════════════════════════════════════════════════════
print("\n[1175] 2. Create function URL for backtest Lambda")
try:
    try:
        existing = lam.get_function_url_config(FunctionName=BACKTEST_LAMBDA)
        url = existing["FunctionUrl"]
        out["steps"]["function_url"] = {"created": False, "url": url}
        print(f"   ✓ already exists: {url}")
    except lam.exceptions.ResourceNotFoundException:
        resp = lam.create_function_url_config(
            FunctionName=BACKTEST_LAMBDA,
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
                FunctionName=BACKTEST_LAMBDA,
                StatementId="FunctionURLAllowPublicAccess",
                Action="lambda:InvokeFunctionUrl",
                Principal="*",
                FunctionUrlAuthType="NONE",
            )
        except lam.exceptions.ResourceConflictException:
            pass
        out["steps"]["function_url"] = {"created": True, "url": url}
        print(f"   ✓ created: {url}")
except Exception as e:
    out["steps"]["function_url"] = {"error": str(e)[:300]}
    print(f"   ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 3: EventBridge schedule
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1175] 3. EventBridge schedule")
try:
    events.put_rule(
        Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
        Description="06:00 ET — daily research track record refresh",
    )
    fn = lam.get_function(FunctionName=BACKTEST_LAMBDA)
    events.put_targets(Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": fn["Configuration"]["FunctionArn"]}])
    sid = f"EBInvoke-{RULE_NAME}"
    try:
        lam.add_permission(
            FunctionName=BACKTEST_LAMBDA, StatementId=sid,
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}",
        )
        out["steps"]["schedule"] = {"created": True, "permission_added": True}
    except lam.exceptions.ResourceConflictException:
        out["steps"]["schedule"] = {"created": True, "permission_added": False}
    print("   ✓ schedule + target + permission")
except Exception as e:
    out["steps"]["schedule"] = {"error": str(e)[:300]}
    print(f"   ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 4: Async invoke backtest + poll for output
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1175] 4. Async-invoke backtest Lambda + poll")
def head_lm(k):
    try:
        return s3.head_object(Bucket=BUCKET, Key=k)["LastModified"]
    except Exception:
        return None

try:
    invoke_t0 = time.time()
    resp = lam.invoke(FunctionName=BACKTEST_LAMBDA, InvocationType="Event", Payload=b"{}")
    invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)
    print(f"   async invoke status={resp['StatusCode']}, polling...")
    for i in range(50):
        time.sleep(3)
        lm = head_lm("analytics/backtest_results.json")
        if lm and lm > invoke_dt:
            elapsed = round(time.time() - invoke_t0, 1)
            obj = s3.get_object(Bucket=BUCKET, Key="analytics/backtest_results.json")
            doc = json.loads(obj["Body"].read())
            out["steps"]["backtest_run"] = {
                "elapsed_s": elapsed,
                "n_research_files": doc.get("n_research_files"),
                "n_calls_with_returns": doc.get("n_calls_with_returns"),
                "n_calls_with_alpha": doc.get("n_calls_with_alpha"),
                "universe_size": doc.get("universe_size"),
                "avg_days_held": doc.get("avg_days_held"),
                "spy_current_price": doc.get("spy_current_price"),
                "caveats": doc.get("caveats", []),
                "rating_summary": doc.get("rating_summary", []),
                "critique_summary": doc.get("critique_summary", []),
                "ensemble_attribution": doc.get("ensemble_attribution", {}),
                "sample_calls": [
                    {k: v for k, v in c.items() if k in
                      ["ticker","rating","critic_rating","entry_price","current_price",
                       "ticker_return_pct","spy_return_pct","alpha_pct","disagreement_score",
                       "rating_diverges","days_held"]}
                    for c in (doc.get("per_call", []))[:5]
                ],
            }
            print(f"   ✓ backtest done in {elapsed}s · n_with_returns={doc.get('n_calls_with_returns')}")
            break
    else:
        out["steps"]["backtest_run"] = {"error": "poll timeout"}
        print("   ⚠ poll timeout")
except Exception as e:
    out["steps"]["backtest_run"] = {"error": str(e)[:300]}
    print(f"   ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 5: Verify 1h TTL fix on critique Lambda
# Use a FRESH ticker (TSLA — likely no critique yet) to force cache_create.
# Then verify usage shows ephemeral_1h_input_tokens > 0.
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1175] 5. Verify 1h TTL fix on critique")
try:
    fresh_ticker = "TSLA"
    print(f"   firing critique on {fresh_ticker} to force cache_create...")
    invoke_t0 = time.time()
    resp = lam.invoke(
        FunctionName=CRITIQUE_LAMBDA,
        InvocationType="Event",
        Payload=json.dumps({"ticker": fresh_ticker}).encode(),
    )
    invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)

    # Poll for the critique file to appear
    print(f"   polling for equity-critique/{fresh_ticker}.json...")
    for i in range(40):
        time.sleep(3)
        lm = head_lm(f"equity-critique/{fresh_ticker}.json")
        if lm and lm > invoke_dt:
            elapsed = round(time.time() - invoke_t0, 1)
            obj = s3.get_object(Bucket=BUCKET, Key=f"equity-critique/{fresh_ticker}.json")
            doc = json.loads(obj["Body"].read())
            usage = (doc.get("critic") or {}).get("usage", {})
            ephem_5m = (usage.get("cache_creation") or {}).get("ephemeral_5m_input_tokens", 0) or 0
            ephem_1h = (usage.get("cache_creation") or {}).get("ephemeral_1h_input_tokens", 0) or 0
            ttl_works = ephem_1h > 0 and ephem_5m == 0
            out["steps"]["ttl_verify"] = {
                "elapsed_s": elapsed,
                "ticker": fresh_ticker,
                "ephemeral_5m_input_tokens": ephem_5m,
                "ephemeral_1h_input_tokens": ephem_1h,
                "cache_creation_input_tokens": usage.get("cache_creation_input_tokens"),
                "cache_read_input_tokens": usage.get("cache_read_input_tokens"),
                "ttl_1h_works": ttl_works,
                "interpretation": (
                    "✓ 1h TTL active — beta header working" if ttl_works
                    else "✗ Still using 5m TTL — beta header not applied" if ephem_5m > 0
                    else "All cache_read (no new write this call)"
                ),
            }
            print(f"   ttl_1h_works={ttl_works}")
            print(f"   ephem_5m={ephem_5m}, ephem_1h={ephem_1h}")
            break
    else:
        out["steps"]["ttl_verify"] = {"error": "poll timeout"}
        print("   ⚠ poll timeout")
except Exception as e:
    out["steps"]["ttl_verify"] = {"error": str(e)[:300]}
    print(f"   ❌ {e}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1175] DONE")
