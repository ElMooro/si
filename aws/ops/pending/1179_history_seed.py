"""1179 — Seed today's history snapshot + bucket policy + re-invoke backtest.

The new history-archive code in equity-research Lambda writes ONLY on
fresh runs going forward. Today's existing research wasn't written under
the new code path, so equity-research-history/{today}/ is empty.

We bootstrap by copying ALL current equity-research/*.json to
equity-research-history/{YYYY-MM-DD}/*.json today. Tomorrow's prewarm
will write tomorrow's snapshot under the new code. The backtest will
then have ≥1 day of accumulated history.

Also:
  - Add equity-research-history/* to bucket policy (PublicReadEquityResearch
    statement covers equity-research/* only; need a new one).
  - Re-invoke backtest after seeding so the live data reflects the new
    history-aware code.
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1179_history_seed.json"
BUCKET = "justhodl-dashboard-live"
BACKTEST_LAMBDA = "justhodl-research-backtest"

cfg = Config(read_timeout=120, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

# ═══════════════════════════════════════════════════════════════════
# Step 1: Bucket policy for equity-research-history/*
# ═══════════════════════════════════════════════════════════════════
print("[1179] 1. Bucket policy for equity-research-history/*")
try:
    cur = s3.get_bucket_policy(Bucket=BUCKET)
    policy = json.loads(cur["Policy"])
    sids = [s.get("Sid", "") for s in policy.get("Statement", [])]
    if "PublicReadEquityResearchHistory" not in sids:
        policy["Statement"].append({
            "Sid": "PublicReadEquityResearchHistory",
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject"],
            "Resource": [f"arn:aws:s3:::{BUCKET}/equity-research-history/*"],
        })
        s3.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps(policy))
        out["bucket_policy"] = {"added": True}
        print("   ✓ added PublicReadEquityResearchHistory")
    else:
        out["bucket_policy"] = {"added": False, "note": "already present"}
        print("   ✓ already present")
except Exception as e:
    out["bucket_policy"] = {"error": str(e)[:300]}
    print(f"   ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 2: Seed today's snapshot from current equity-research/*
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1179] 2. Seed equity-research-history/{datetime.now(timezone.utc).strftime('%Y-%m-%d')}/*")
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
copied = []
errors = []
try:
    pag = s3.get_paginator("list_objects_v2")
    for page in pag.paginate(Bucket=BUCKET, Prefix="equity-research/"):
        for obj in (page.get("Contents") or []):
            src_key = obj["Key"]
            if not src_key.endswith(".json") or "manifest" in src_key:
                continue
            # equity-research/AAPL.json → equity-research-history/2026-06-02/AAPL.json
            filename = src_key.split("/")[-1]
            dst_key = f"equity-research-history/{today}/{filename}"
            try:
                # Check if already exists (idempotent — skip if so)
                try:
                    s3.head_object(Bucket=BUCKET, Key=dst_key)
                    continue  # already there
                except Exception:
                    pass

                s3.copy_object(
                    Bucket=BUCKET,
                    CopySource={"Bucket": BUCKET, "Key": src_key},
                    Key=dst_key,
                    MetadataDirective="REPLACE",
                    ContentType="application/json",
                    CacheControl="public, max-age=86400",
                )
                copied.append(filename)
            except Exception as e:
                errors.append({"key": dst_key, "err": str(e)[:200]})
    out["seed"] = {"n_copied": len(copied), "n_errors": len(errors)}
    if errors:
        out["seed"]["sample_errors"] = errors[:3]
    print(f"   ✓ copied {len(copied)} files ({len(errors)} errors)")
except Exception as e:
    out["seed"] = {"error": str(e)[:300]}
    print(f"   ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 3: Re-invoke backtest with new history-aware code
# Note: the backtest Lambda code was UPDATED in this push. Wait for the
# Deploy Lambdas action to have run before invoking. It should have already
# run by now (~80s ago).
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1179] 3. Re-invoke backtest with updated code")
def head_lm(k):
    try:
        return s3.head_object(Bucket=BUCKET, Key=k)["LastModified"]
    except Exception:
        return None

try:
    # Quick sanity check that Lambda has the new code (was updated recently)
    fn_cfg = lam.get_function_configuration(FunctionName=BACKTEST_LAMBDA)
    last_mod = fn_cfg.get("LastModified", "")
    print(f"   Lambda last modified: {last_mod}")

    invoke_t0 = time.time()
    resp = lam.invoke(FunctionName=BACKTEST_LAMBDA, InvocationType="Event", Payload=b"{}")
    invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)
    print(f"   async invoke status={resp['StatusCode']}, polling...")
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
                "avg_days_held": doc.get("avg_days_held"),
                "spy_current_price": doc.get("spy_current_price"),
                "caveats": doc.get("caveats", []),
                "ensemble_attribution": doc.get("ensemble_attribution", {}),
                "rating_summary": doc.get("rating_summary", []),
                "sample_calls": [
                    {k: v for k, v in c.items() if k in
                      ["ticker","rating","critic_rating","entry_price","current_price",
                       "ticker_return_pct","alpha_pct","days_held","n_history_snapshots"]}
                    for c in (doc.get("per_call", []))[:5]
                ],
            }
            print(f"   ✓ backtest done in {elapsed}s · n_calls={doc.get('n_calls_with_returns')} · avg_days={doc.get('avg_days_held')}")
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
print(f"\n[1179] DONE")
