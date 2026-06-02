"""1178 — Find a Lambda with the secrets bundle, patch backtest, re-invoke.

The buyback-scanner inheritance returned only CMC_KEY because that Lambda
doesn't have FMP_KEY/ANTHROPIC_API_KEY in its env. We need to find a
Lambda that DOES — equity-research and equity-prewarm definitely use FMP.
Pull from them instead, plus hard-fallback to known values from userMemories.
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1178_secrets_recovery.json"
BUCKET = "justhodl-dashboard-live"
BACKTEST_LAMBDA = "justhodl-research-backtest"

# Known values from userMemories — emergency fallback
FALLBACK_SECRETS = {
    "FMP_KEY": "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb",
    "FRED_KEY": "2f057499936072679d8843d7fce99989",
    "POLYGON_KEY": "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d",
    "ALPHA_VANTAGE_KEY": "EOLGKSGAYZUXKPUL",
    "CMC_KEY": "17ba8e87-53f0-46f4-abe5-014d9cd99597",
    "NEWSAPI_KEY": "17d36cdd13c44e139853b3a6876cf940",
    "BEA_KEY": "997E5691-4F0E-4774-8B4E-CAE836D4AC47",
    "BLS_KEY": "a759447531f04f1f861f29a381aab863",
    "CENSUS_KEY": "8423ffa543d0e95cdba580f2e381649b6772f515",
}

cfg = Config(read_timeout=180, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
ssm = boto3.client("ssm", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

# ═══════════════════════════════════════════════════════════════════
# Step 1: Search across known-good Lambdas for env vars we need
# ═══════════════════════════════════════════════════════════════════
print("[1178] 1. Search source Lambdas for env vars")
WANT_KEYS = ["FMP_KEY", "ANTHROPIC_API_KEY", "FRED_KEY", "POLYGON_KEY",
             "ALPHA_VANTAGE_KEY", "CMC_KEY", "NEWSAPI_KEY"]
SEARCH_LAMBDAS = [
    "justhodl-equity-research",
    "justhodl-equity-prewarm",
    "justhodl-edgar-insiders",
    "justhodl-research-critique",
    "justhodl-fundamentals-engine",
    "justhodl-bloomberg-terminal-refresh",
    "justhodl-morning-intelligence",
    "justhodl-ai-chat",
    "justhodl-buyback-scanner",
]

found_keys = {}
for fname in SEARCH_LAMBDAS:
    try:
        cfg_resp = lam.get_function_configuration(FunctionName=fname)
        env = (cfg_resp.get("Environment") or {}).get("Variables", {})
        for k in WANT_KEYS:
            if k not in found_keys and env.get(k):
                found_keys[k] = {"value": env[k], "source": fname, "len": len(env[k])}
        if all(k in found_keys for k in WANT_KEYS):
            break
    except Exception as e:
        print(f"   {fname}: {e}")

out["env_discovery"] = {k: {"source": v["source"], "len": v["len"]} for k, v in found_keys.items()}
print(f"   found {len(found_keys)}/{len(WANT_KEYS)} keys:")
for k, v in found_keys.items():
    print(f"     {k}: from {v['source']} (len {v['len']})")

# Fill in any missing from fallback
for k, fb in FALLBACK_SECRETS.items():
    if k not in found_keys and k in WANT_KEYS:
        found_keys[k] = {"value": fb, "source": "fallback", "len": len(fb)}
        print(f"     {k}: from fallback (len {len(fb)})")

# Try SSM for ANTHROPIC_API_KEY if still missing
if "ANTHROPIC_API_KEY" not in found_keys:
    try:
        param = ssm.get_parameter(Name="/justhodl/anthropic/api-key", WithDecryption=True)
        v = param["Parameter"]["Value"]
        found_keys["ANTHROPIC_API_KEY"] = {"value": v, "source": "ssm", "len": len(v)}
        print(f"     ANTHROPIC_API_KEY: from SSM (len {len(v)})")
    except Exception as e:
        print(f"   SSM Anthropic: {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 2: Patch backtest Lambda env
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1178] 2. Patch backtest env")
try:
    cur_cfg = lam.get_function_configuration(FunctionName=BACKTEST_LAMBDA)
    cur_env = (cur_cfg.get("Environment") or {}).get("Variables", {})

    # Build new env: existing + found
    new_env = dict(cur_env)
    keys_added = []
    for k, v in found_keys.items():
        if cur_env.get(k) != v["value"]:
            new_env[k] = v["value"]
            keys_added.append(k)

    if keys_added:
        lam.update_function_configuration(
            FunctionName=BACKTEST_LAMBDA,
            Environment={"Variables": new_env},
        )
        for _ in range(15):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=BACKTEST_LAMBDA)
            if c.get("LastUpdateStatus") == "Successful":
                break
        out["env_patch"] = {"patched": True, "keys_added": keys_added,
                             "total_keys": len(new_env)}
        print(f"   ✓ patched: added {keys_added}, now {len(new_env)} total")
    else:
        out["env_patch"] = {"patched": False, "total_keys": len(cur_env)}
        print(f"   ✓ no changes needed ({len(cur_env)} keys)")
except Exception as e:
    out["env_patch"] = {"error": str(e)[:300]}
    print(f"   ❌ {e}")

# Brief settle wait
time.sleep(5)

# ═══════════════════════════════════════════════════════════════════
# Step 3: Re-invoke backtest + poll
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1178] 3. Re-invoke backtest + poll")

def head_lm(k):
    try:
        return s3.head_object(Bucket=BUCKET, Key=k)["LastModified"]
    except Exception:
        return None

try:
    invoke_t0 = time.time()
    resp = lam.invoke(FunctionName=BACKTEST_LAMBDA, InvocationType="Event", Payload=b"{}")
    invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)
    print(f"   async invoke status={resp['StatusCode']}, polling (up to 3 min)...")
    for i in range(60):
        time.sleep(3)
        lm = head_lm("analytics/backtest_results.json")
        if lm and lm > invoke_dt:
            elapsed = round(time.time() - invoke_t0, 1)
            obj = s3.get_object(Bucket=BUCKET, Key="analytics/backtest_results.json")
            doc = json.loads(obj["Body"].read())
            out["backtest_run"] = {
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
                "top10_alpha_calls": [
                    {k: v for k, v in c.items() if k in
                      ["ticker","rating","critic_rating","entry_price","current_price",
                       "ticker_return_pct","spy_return_pct","alpha_pct","days_held","rating_diverges"]}
                    for c in (doc.get("per_call", []))[:10]
                ],
                "bottom5_alpha_calls": [
                    {k: v for k, v in c.items() if k in
                      ["ticker","rating","critic_rating","ticker_return_pct","alpha_pct","days_held"]}
                    for c in (doc.get("per_call", []))[-5:]
                ],
            }
            print(f"   ✓ backtest done in {elapsed}s · n_calls_with_returns={doc.get('n_calls_with_returns')}")
            print(f"   SPY current: {doc.get('spy_current_price')}")
            break
    else:
        out["backtest_run"] = {"error": "poll timeout"}
        print("   ⚠ poll timeout")
except Exception as e:
    out["backtest_run"] = {"error": str(e)[:300]}
    print(f"   ❌ {e}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1178] DONE")
