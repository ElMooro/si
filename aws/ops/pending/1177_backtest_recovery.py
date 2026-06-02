"""1177 — Comprehensive backtest recovery + rollout + TTL verify.

Steps (idempotent — safe to re-run):
  1. Check if justhodl-research-backtest Lambda exists. If not, create it
     directly via boto3 using the source files + standard secrets bundle
     inherited from justhodl-buyback-scanner.
  2. Patch env vars from the buyback-scanner inherit pattern (idempotent).
  3. Create function URL (if not exists).
  4. Create EventBridge schedule (cron(0 11 * * ? *) = 06:00 ET).
  5. Async-invoke + poll for analytics/backtest_results.json.
  6. Verify 1h TTL fix: invoke critique on a TRULY fresh ticker we haven't
     critiqued before (AMZN), poll, read S3 output, check usage telemetry
     for ephemeral_1h_input_tokens > 0.
"""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1177_backtest_recovery.json"
BUCKET = "justhodl-dashboard-live"
BACKTEST_LAMBDA = "justhodl-research-backtest"
CRITIQUE_LAMBDA = "justhodl-research-critique"
SOURCE_DIR = "aws/lambdas/justhodl-research-backtest/source"
SHARED_DIR = "aws/shared"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
RULE_NAME = "justhodl-research-backtest-daily"
SCHEDULE = "cron(0 11 * * ? *)"

# Standard secrets to inherit from buyback-scanner (matches deploy-lambdas.yml)
STANDARD_KEYS = [
    "FMP_KEY", "FRED_KEY", "POLYGON_KEY", "ALPHA_VANTAGE_KEY", "CMC_KEY",
    "ANTHROPIC_API_KEY", "OPENAI_API_KEY", "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CHAT_ID", "TELEGRAM_TOKEN", "NEWSAPI_KEY", "BLS_KEY",
    "BEA_KEY", "CENSUS_KEY",
]

cfg = Config(read_timeout=180, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}


def pull_secrets_from_buyback() -> dict:
    """Pull standard secrets from justhodl-buyback-scanner env."""
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-buyback-scanner")
        src_env = (cfg.get("Environment") or {}).get("Variables", {})
        out_env = {}
        for k in STANDARD_KEYS:
            v = src_env.get(k)
            if v:
                out_env[k] = v
        return out_env
    except Exception as e:
        print(f"  Couldn't pull from buyback-scanner: {e}")
        return {}


def build_deploy_zip() -> bytes:
    """Build deploy zip: aws/shared/*.py overlaid with source/*.py."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        # Shared bundle first
        if os.path.isdir(SHARED_DIR):
            for f in os.listdir(SHARED_DIR):
                if f.endswith(".py") and not f.startswith("__"):
                    fpath = os.path.join(SHARED_DIR, f)
                    if os.path.isfile(fpath):
                        zf.write(fpath, arcname=f)
        # Source overrides — files with same name win
        for root, _, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"):
                    continue
                fpath = os.path.join(root, f)
                rel = os.path.relpath(fpath, SOURCE_DIR)
                # Overwrite if already in zip (source wins over shared)
                zf.write(fpath, arcname=rel)
    return buf.getvalue()


# ═══════════════════════════════════════════════════════════════════
# Step 1: Verify or create Lambda
# ═══════════════════════════════════════════════════════════════════
print("[1177] 1. Verify backtest Lambda exists")
lambda_exists = False
try:
    fn_cfg = lam.get_function_configuration(FunctionName=BACKTEST_LAMBDA)
    lambda_exists = True
    out["steps"]["lambda_check"] = {
        "exists": True,
        "memory": fn_cfg.get("MemorySize"),
        "timeout": fn_cfg.get("Timeout"),
        "runtime": fn_cfg.get("Runtime"),
        "env_keys": list((fn_cfg.get("Environment") or {}).get("Variables", {}).keys()),
        "last_modified": fn_cfg.get("LastModified"),
    }
    print(f"  ✓ exists ({fn_cfg.get('MemorySize')}MB · {fn_cfg.get('Timeout')}s)")
except lam.exceptions.ResourceNotFoundException:
    print("  ✗ does not exist — creating now")
    out["steps"]["lambda_check"] = {"exists": False}

if not lambda_exists:
    print("\n[1177] 1b. Create Lambda directly via boto3")
    try:
        zip_bytes = build_deploy_zip()
        print(f"  built zip: {len(zip_bytes)/1024:.1f} KB")

        env_vars = pull_secrets_from_buyback()
        print(f"  inheriting {len(env_vars)} env vars from buyback-scanner: {list(env_vars.keys())}")

        resp = lam.create_function(
            FunctionName=BACKTEST_LAMBDA,
            Runtime="python3.12",
            Role=ROLE_ARN,
            Handler="lambda_function.lambda_handler",
            Code={"ZipFile": zip_bytes},
            Description="Research track record backtesting. Reads every research + critique file, computes return + SPY alpha, aggregates by rating, attributes ensemble signal.",
            Timeout=240,
            MemorySize=512,
            Environment={"Variables": env_vars},
            Architectures=["x86_64"],
            Publish=False,
        )
        out["steps"]["lambda_create"] = {
            "created": True,
            "function_arn": resp.get("FunctionArn"),
            "env_keys_set": list(env_vars.keys()),
        }
        print(f"  ✓ created: {resp.get('FunctionArn')}")

        # Wait for function to be active
        for _ in range(30):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=BACKTEST_LAMBDA)
            if c.get("State") == "Active":
                break
        print(f"  state: {c.get('State')}")
        lambda_exists = True
    except Exception as e:
        out["steps"]["lambda_create"] = {"error": str(e)[:400]}
        print(f"  ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 2: If exists but env empty, patch from buyback-scanner
# ═══════════════════════════════════════════════════════════════════
if lambda_exists:
    print(f"\n[1177] 2. Ensure env vars populated")
    try:
        fn_cfg = lam.get_function_configuration(FunctionName=BACKTEST_LAMBDA)
        cur_env = (fn_cfg.get("Environment") or {}).get("Variables", {})
        if not cur_env.get("FMP_KEY") or not cur_env.get("ANTHROPIC_API_KEY"):
            inherited = pull_secrets_from_buyback()
            new_env = {**cur_env, **inherited}
            if new_env != cur_env:
                lam.update_function_configuration(
                    FunctionName=BACKTEST_LAMBDA,
                    Environment={"Variables": new_env},
                )
                # Wait for update
                for _ in range(15):
                    time.sleep(2)
                    c = lam.get_function_configuration(FunctionName=BACKTEST_LAMBDA)
                    if c.get("LastUpdateStatus") == "Successful":
                        break
                out["steps"]["env_patch"] = {"patched": True, "keys_added": list(set(inherited.keys()) - set(cur_env.keys()))}
                print(f"  ✓ patched env: {len(new_env)} total keys")
            else:
                out["steps"]["env_patch"] = {"patched": False, "note": "no changes"}
        else:
            out["steps"]["env_patch"] = {"patched": False, "note": "already populated"}
            print(f"  ✓ env already populated ({len(cur_env)} keys)")
    except Exception as e:
        out["steps"]["env_patch"] = {"error": str(e)[:300]}
        print(f"  ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 3: Function URL
# ═══════════════════════════════════════════════════════════════════
if lambda_exists:
    print(f"\n[1177] 3. Function URL")
    try:
        try:
            existing = lam.get_function_url_config(FunctionName=BACKTEST_LAMBDA)
            url = existing["FunctionUrl"]
            out["steps"]["function_url"] = {"created": False, "url": url}
            print(f"  ✓ already exists: {url}")
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
            print(f"  ✓ created: {url}")
    except Exception as e:
        out["steps"]["function_url"] = {"error": str(e)[:300]}
        print(f"  ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 4: Schedule
# ═══════════════════════════════════════════════════════════════════
if lambda_exists:
    print(f"\n[1177] 4. EventBridge schedule")
    try:
        events.put_rule(
            Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
            Description="06:00 ET daily research track record",
        )
        fn = lam.get_function(FunctionName=BACKTEST_LAMBDA)
        events.put_targets(Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": fn["Configuration"]["FunctionArn"]}])
        try:
            lam.add_permission(
                FunctionName=BACKTEST_LAMBDA,
                StatementId=f"EBInvoke-{RULE_NAME}",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}",
            )
        except lam.exceptions.ResourceConflictException:
            pass
        out["steps"]["schedule"] = {"created": True}
        print("  ✓ schedule wired")
    except Exception as e:
        out["steps"]["schedule"] = {"error": str(e)[:300]}
        print(f"  ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 5: Invoke backtest + poll
# ═══════════════════════════════════════════════════════════════════
if lambda_exists:
    print(f"\n[1177] 5. Async-invoke backtest + poll")

    def head_lm(k):
        try:
            return s3.head_object(Bucket=BUCKET, Key=k)["LastModified"]
        except Exception:
            return None

    try:
        invoke_t0 = time.time()
        resp = lam.invoke(FunctionName=BACKTEST_LAMBDA, InvocationType="Event", Payload=b"{}")
        invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)
        print(f"  async invoke status={resp['StatusCode']}, polling...")
        for i in range(60):
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
                    "top5_alpha_calls": [
                        {k: v for k, v in c.items() if k in
                          ["ticker","rating","critic_rating","ticker_return_pct","spy_return_pct","alpha_pct","days_held","rating_diverges"]}
                        for c in (doc.get("per_call", []))[:5]
                    ],
                    "bottom5_alpha_calls": [
                        {k: v for k, v in c.items() if k in
                          ["ticker","rating","critic_rating","ticker_return_pct","spy_return_pct","alpha_pct","days_held","rating_diverges"]}
                        for c in (doc.get("per_call", []))[-5:]
                    ],
                }
                print(f"  ✓ backtest done in {elapsed}s · n_calls={doc.get('n_calls_with_returns')}")
                break
        else:
            out["steps"]["backtest_run"] = {"error": "poll timeout"}
            print("  ⚠ poll timeout")
    except Exception as e:
        out["steps"]["backtest_run"] = {"error": str(e)[:300]}
        print(f"  ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 6: Verify 1h TTL fix with truly fresh ticker (AMZN)
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1177] 6. 1h TTL verify with AMZN")
try:
    fresh_ticker = "AMZN"
    # Check that AMZN has research first (required for critique input)
    has_research = False
    try:
        s3.head_object(Bucket=BUCKET, Key=f"equity-research/{fresh_ticker}.json")
        has_research = True
    except Exception:
        pass

    if not has_research:
        # Use GOOG instead — it's in the prewarm universe
        fresh_ticker = "GOOG"
        try:
            s3.head_object(Bucket=BUCKET, Key=f"equity-research/{fresh_ticker}.json")
            has_research = True
        except Exception:
            pass

    if not has_research:
        out["steps"]["ttl_verify"] = {"error": f"No research for {fresh_ticker} (or AMZN) to critique"}
    else:
        print(f"  firing critique on {fresh_ticker}")
        invoke_t0 = time.time()
        # Delete existing critique first to force fresh write
        try:
            s3.delete_object(Bucket=BUCKET, Key=f"equity-critique/{fresh_ticker}.json")
            print(f"  deleted existing critique to force fresh run")
        except Exception:
            pass

        lam.invoke(
            FunctionName=CRITIQUE_LAMBDA,
            InvocationType="Event",
            Payload=json.dumps({"ticker": fresh_ticker}).encode(),
        )
        invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)

        for i in range(40):
            time.sleep(3)
            try:
                lm = s3.head_object(Bucket=BUCKET, Key=f"equity-critique/{fresh_ticker}.json")["LastModified"]
                if lm > invoke_dt:
                    elapsed = round(time.time() - invoke_t0, 1)
                    obj = s3.get_object(Bucket=BUCKET, Key=f"equity-critique/{fresh_ticker}.json")
                    doc = json.loads(obj["Body"].read())
                    usage = (doc.get("critic") or {}).get("usage", {})
                    cache_create = (usage.get("cache_creation") or {})
                    ephem_5m = cache_create.get("ephemeral_5m_input_tokens", 0) or 0
                    ephem_1h = cache_create.get("ephemeral_1h_input_tokens", 0) or 0
                    cc_total = usage.get("cache_creation_input_tokens", 0) or 0
                    cr_total = usage.get("cache_read_input_tokens", 0) or 0

                    if cc_total == 0:
                        # No new cache write (existing cache hit). Look at what TTL the
                        # READ cache was originally written with — but the response doesn't
                        # tell us that. Best inference: if cache_read happens and the
                        # original write was >5min ago, the TTL must be 1h.
                        interpretation = (
                            f"All cache_read ({cr_total} tokens) — no new write. "
                            "Can't directly observe TTL from this run."
                        )
                        ttl_works = None
                    elif ephem_1h > 0 and ephem_5m == 0:
                        ttl_works = True
                        interpretation = "✓ 1h TTL CONFIRMED active"
                    elif ephem_5m > 0 and ephem_1h == 0:
                        ttl_works = False
                        interpretation = "✗ Still 5m TTL — beta header not active"
                    else:
                        ttl_works = None
                        interpretation = f"Ambiguous: 5m={ephem_5m}, 1h={ephem_1h}"

                    out["steps"]["ttl_verify"] = {
                        "elapsed_s": elapsed,
                        "ticker": fresh_ticker,
                        "ephemeral_5m_input_tokens": ephem_5m,
                        "ephemeral_1h_input_tokens": ephem_1h,
                        "cache_creation_input_tokens": cc_total,
                        "cache_read_input_tokens": cr_total,
                        "ttl_1h_works": ttl_works,
                        "interpretation": interpretation,
                    }
                    print(f"  ttl_1h_works={ttl_works}: {interpretation}")
                    break
            except Exception:
                pass
        else:
            out["steps"]["ttl_verify"] = {"error": "poll timeout"}
except Exception as e:
    out["steps"]["ttl_verify"] = {"error": str(e)[:300]}
    print(f"  ❌ {e}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1177] DONE")
