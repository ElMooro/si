"""1174 — Re-smoke critique now that env is fixed + Lambda warm.

Also: check if the earlier invokes (from 1173 retry) actually completed
asynchronously despite the boto3 read timeout. The Lambda has 180s timeout
and the API call probably finished — boto3 just stopped waiting.
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1174_critique_resmoke.json"
LAMBDA_NAME = "justhodl-research-critique"
SNAPSHOT_LAMBDA = "justhodl-analytics-snapshot"
BUCKET = "justhodl-dashboard-live"

# Generous timeouts now
cfg = Config(read_timeout=200, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)
s3  = boto3.client("s3", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

# 1. Check if S3 has any critique files (maybe earlier invokes succeeded async-style?)
print("[1174] 1. Existing critique files in S3")
existing = []
try:
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=BUCKET, Prefix="equity-critique/"):
        for obj in (page.get("Contents") or []):
            existing.append({
                "key": obj["Key"],
                "size_kb": round(obj["Size"]/1024, 1),
                "last_modified": obj["LastModified"].isoformat(),
            })
    out["existing_files"] = existing
    print(f"   Found {len(existing)} existing critique files:")
    for e in existing[:5]:
        print(f"     {e['key']}: {e['size_kb']}KB · {e['last_modified']}")
except Exception as e:
    out["existing_files_err"] = str(e)[:200]

# 2. Async invoke 3 tickers (don't wait — they write to S3)
print(f"\n[1174] 2. Async-invoke critique for 3 tickers")
async_results = {}
for ticker in ["AAPL", "NVDA", "MSFT"]:
    try:
        resp = lam.invoke(
            FunctionName=LAMBDA_NAME,
            InvocationType="Event",  # async — fire and forget
            Payload=json.dumps({"ticker": ticker}).encode(),
        )
        async_results[ticker] = {"status_code": resp["StatusCode"]}
        print(f"   ✓ {ticker}: fired async ({resp['StatusCode']})")
    except Exception as e:
        async_results[ticker] = {"error": str(e)[:200]}
        print(f"   ❌ {ticker}: {e}")
out["async_invokes"] = async_results

# Wait for them to complete (Sonnet 4.6 takes 60-90s each, but parallel)
print(f"\n[1174] 3. Poll S3 for outputs (max 3 min)")
poll_t0 = time.time()
target_set = set(["AAPL", "NVDA", "MSFT"])
seen = set()
for i in range(60):
    time.sleep(3)
    # Check which tickers have fresh files (last modified within the last 3 min)
    cur_files = {}
    try:
        pag = s3.get_paginator("list_objects_v2")
        for page in pag.paginate(Bucket=BUCKET, Prefix="equity-critique/"):
            for obj in (page.get("Contents") or []):
                cur_files[obj["Key"]] = obj["LastModified"]
    except Exception:
        continue
    now = datetime.now(timezone.utc)
    for ticker in target_set:
        key = f"equity-critique/{ticker}.json"
        lm = cur_files.get(key)
        if lm and (now - lm).total_seconds() < 200:
            if ticker not in seen:
                seen.add(ticker)
                print(f"   ✓ {ticker} appeared after {round(time.time()-poll_t0,1)}s")
    if seen == target_set:
        break
    if i % 5 == 4:
        print(f"   ... {round(time.time()-poll_t0,1)}s, seen {seen}")

out["polled_seconds"] = round(time.time()-poll_t0, 1)
out["tickers_completed"] = list(seen)

# 4. Pull and inspect the critique outputs
print(f"\n[1174] 4. Inspect critique outputs")
samples = {}
for ticker in sorted(seen):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"equity-critique/{ticker}.json")
        doc = json.loads(obj["Body"].read())
        c = doc.get("critique", {})
        a = doc.get("analyst_verdict", {})
        critic = doc.get("critic", {})
        samples[ticker] = {
            "analyst_rating":      a.get("rating"),
            "analyst_pt":          a.get("price_target_12m"),
            "alternative_rating":  c.get("alternative_rating"),
            "alternative_pt":      c.get("alternative_pt"),
            "disagreement_score":  c.get("disagreement_score"),
            "key_disagreement":    c.get("key_disagreement_1liner"),
            "anti_thesis_preview": (c.get("anti_thesis") or "")[:250],
            "n_reinterps":         len(c.get("data_reinterpretations") or []),
            "n_risks":             len(c.get("underweighted_risks") or []),
            "n_bear_strength":     len(c.get("bear_case_strengtheners") or []),
            "first_reinterp":      (c.get("data_reinterpretations") or [{}])[0] if c.get("data_reinterpretations") else None,
            "first_risk":          (c.get("underweighted_risks") or [{}])[0] if c.get("underweighted_risks") else None,
            "critic_model":        critic.get("model"),
            "critic_provider":     critic.get("provider"),
            "critic_cost_usd":     critic.get("cost_usd"),
            "critic_elapsed_s":    critic.get("elapsed_s"),
            "critic_usage":        critic.get("usage"),
        }
        print(f"\n   ── {ticker} ──")
        print(f"     verdict: {a.get('rating')} (analyst) → {c.get('alternative_rating')} (critic)")
        print(f"     disagreement: {c.get('disagreement_score')}/100")
        print(f"     key: {c.get('key_disagreement_1liner')}")
        print(f"     cost: ${critic.get('cost_usd')} · elapsed {critic.get('elapsed_s')}s")
        if samples[ticker]["first_reinterp"]:
            r = samples[ticker]["first_reinterp"]
            print(f"     reinterp #1: {r.get('metric')[:80] if r.get('metric') else '?'}")
    except Exception as e:
        samples[ticker] = {"error": str(e)[:200]}
        print(f"   ❌ {ticker}: {e}")
out["samples"] = samples

# 5. Trigger snapshot to flatten critique
print(f"\n[1174] 5. Re-trigger snapshot to flatten critique")
try:
    invoke_t0 = time.time()
    resp = lam.invoke(FunctionName=SNAPSHOT_LAMBDA, InvocationType="Event", Payload=b"{}")
    invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)
    print(f"   async invoke status={resp['StatusCode']}, polling...")
    for i in range(40):
        time.sleep(3)
        try:
            head = s3.head_object(Bucket=BUCKET, Key="analytics/research_critique_flat.json")
            if head["LastModified"] > invoke_dt:
                obj = s3.get_object(Bucket=BUCKET, Key="analytics/research_critique_flat.json")
                doc = json.loads(obj["Body"].read())
                rows = doc.get("rows", [])
                out["flat_critique"] = {
                    "n_rows": len(rows),
                    "generated_at": doc.get("generated_at"),
                    "sample_rows": [
                        {k: v for k, v in r.items() if k in
                          ["ticker","analyst_rating","alternative_rating","disagreement_score",
                           "rating_diverges","pt_spread_pct","critic_provider","critic_cost_usd"]}
                        for r in rows[:5]
                    ],
                }
                elapsed = round(time.time() - invoke_t0, 1)
                print(f"   ✓ research_critique_flat.json updated · {len(rows)} rows · {elapsed}s")
                break
        except Exception:
            pass
    else:
        out["flat_critique"] = {"error": "poll timeout"}
        print("   ⚠ poll timeout")
except Exception as e:
    out["flat_critique"] = {"error": str(e)[:300]}
    print(f"   ❌ {e}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1174] DONE")
