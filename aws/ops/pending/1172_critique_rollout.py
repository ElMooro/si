"""1172 — Critique Lambda rollout orchestration.

Steps:
  1. Add 'equity-critique/*' to bucket policy (PublicReadEquityCritique)
  2. Create function URL for critique Lambda (NONE auth, CORS *)
  3. Patch the function URL into why.html
  4. Smoke test: invoke critique sync for 2 tickers (AAPL, NVDA)
  5. Verify outputs land in S3 + parseable
  6. Re-invoke snapshot Lambda (async) to flatten critique into the table
  7. Verify analytics/research_critique_flat.json appears
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1172_critique_rollout.json"
BUCKET = "justhodl-dashboard-live"
CRITIQUE_LAMBDA = "justhodl-research-critique"
SNAPSHOT_LAMBDA = "justhodl-analytics-snapshot"

cfg = Config(read_timeout=120, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name="us-east-1", config=cfg)
lam = boto3.client("lambda", region_name="us-east-1", config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}

# ═══════════════════════════════════════════════════════════════════
# Step 1: Bucket policy — equity-critique/* publicly readable
# ═══════════════════════════════════════════════════════════════════
print("[1172] 1. Bucket policy: add PublicReadEquityCritique")
try:
    cur = s3.get_bucket_policy(Bucket=BUCKET)
    policy = json.loads(cur["Policy"])
    sids = [s.get("Sid", "") for s in policy.get("Statement", [])]
    if "PublicReadEquityCritique" not in sids:
        policy["Statement"].append({
            "Sid": "PublicReadEquityCritique",
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject"],
            "Resource": [f"arn:aws:s3:::{BUCKET}/equity-critique/*"],
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
# Step 2: Function URL
# ═══════════════════════════════════════════════════════════════════
print("\n[1172] 2. Create function URL for critique Lambda")
try:
    try:
        existing = lam.get_function_url_config(FunctionName=CRITIQUE_LAMBDA)
        url = existing["FunctionUrl"]
        out["steps"]["function_url"] = {"created": False, "url": url, "note": "already exists"}
        print(f"   ✓ already exists: {url}")
    except lam.exceptions.ResourceNotFoundException:
        resp = lam.create_function_url_config(
            FunctionName=CRITIQUE_LAMBDA,
            AuthType="NONE",
            Cors={
                "AllowOrigins": ["*"],
                "AllowMethods": ["GET", "POST"],
                "AllowHeaders": ["Content-Type"],
                "MaxAge": 86400,
            },
        )
        url = resp["FunctionUrl"]
        # Public invoke permission
        try:
            lam.add_permission(
                FunctionName=CRITIQUE_LAMBDA,
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
# Step 3: Patch URL into why.html
# ═══════════════════════════════════════════════════════════════════
critique_url = out["steps"].get("function_url", {}).get("url")
if critique_url:
    print(f"\n[1172] 3. Patch URL into why.html")
    try:
        with open("why.html") as f:
            html = f.read()
        # Look for the placeholder line
        old_line = 'let CRITIQUE_LAMBDA_URL = ""; // patched by ops once Lambda URL is created'
        new_line = f'let CRITIQUE_LAMBDA_URL = "{critique_url}"; // patched by ops 1172'
        if old_line in html:
            html = html.replace(old_line, new_line)
            with open("why.html", "w") as f:
                f.write(html)
            out["steps"]["patch_html"] = {"patched": True, "url": critique_url}
            print(f"   ✓ patched")
        else:
            # Already patched; just record
            out["steps"]["patch_html"] = {"patched": False, "note": "placeholder not found (already patched?)"}
            print(f"   ✓ already patched (or placeholder not found)")
    except Exception as e:
        out["steps"]["patch_html"] = {"error": str(e)[:300]}
        print(f"   ❌ {e}")
else:
    out["steps"]["patch_html"] = {"skipped": True, "reason": "no URL"}

# ═══════════════════════════════════════════════════════════════════
# Step 4: Smoke test — sync invoke critique for 2 tickers
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1172] 4. Smoke test critique on AAPL and NVDA")
smokes = {}
for ticker in ["AAPL", "NVDA"]:
    t0 = time.time()
    try:
        resp = lam.invoke(
            FunctionName=CRITIQUE_LAMBDA,
            InvocationType="RequestResponse",
            Payload=json.dumps({"ticker": ticker}).encode(),
        )
        elapsed = round(time.time() - t0, 1)
        payload = json.loads(resp["Payload"].read().decode())
        # body is a stringified JSON
        body = json.loads(payload.get("body", "{}")) if isinstance(payload.get("body"), str) else payload.get("body", {})
        c = body.get("critique", {})
        smokes[ticker] = {
            "elapsed_s":          elapsed,
            "status_code":        payload.get("statusCode"),
            "model_used":         (body.get("critic") or {}).get("model"),
            "provider":           (body.get("critic") or {}).get("provider"),
            "cost_usd":           (body.get("critic") or {}).get("cost_usd"),
            "analyst_rating":     (body.get("analyst_verdict") or {}).get("rating"),
            "alternative_rating": c.get("alternative_rating"),
            "disagreement_score": c.get("disagreement_score"),
            "key_disagreement":   c.get("key_disagreement_1liner"),
            "n_reinterps":        len(c.get("data_reinterpretations") or []),
            "n_risks":            len(c.get("underweighted_risks") or []),
        }
        print(f"   ✓ {ticker}: disagreement={c.get('disagreement_score')} "
              f"({(body.get('analyst_verdict') or {}).get('rating')} → {c.get('alternative_rating')}) "
              f"cost=${(body.get('critic') or {}).get('cost_usd')} {elapsed}s")
    except Exception as e:
        smokes[ticker] = {"error": str(e)[:300], "elapsed_s": round(time.time()-t0,1)}
        print(f"   ❌ {ticker}: {e}")

out["steps"]["smokes"] = smokes

# ═══════════════════════════════════════════════════════════════════
# Step 5: Verify S3 files exist
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1172] 5. Verify critique files in S3")
verify = {}
for ticker in ["AAPL", "NVDA"]:
    key = f"equity-critique/{ticker}.json"
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        verify[key] = {
            "exists": True,
            "size_kb": round(head["ContentLength"]/1024, 1),
            "last_modified": head["LastModified"].isoformat(),
        }
        print(f"   ✓ {key}: {verify[key]['size_kb']}KB")
    except Exception as e:
        verify[key] = {"error": str(e)[:200]}
        print(f"   ❌ {key}: {e}")
out["steps"]["s3_verify"] = verify

# ═══════════════════════════════════════════════════════════════════
# Step 6: Re-invoke snapshot Lambda (async + poll) to flatten critique
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1172] 6. Re-invoke snapshot to flatten critique table")
def head_lm(k):
    try:
        return s3.head_object(Bucket=BUCKET, Key=k)["LastModified"]
    except Exception:
        return None

try:
    invoke_t0 = time.time()
    resp = lam.invoke(
        FunctionName=SNAPSHOT_LAMBDA,
        InvocationType="Event",
        Payload=b"{}",
    )
    invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)
    print(f"   async invoke status={resp['StatusCode']}, polling for outputs...")
    # Poll for the critique flat file to appear (it's the new one)
    for i in range(40):
        time.sleep(3)
        lm = head_lm("analytics/research_critique_flat.json")
        if lm and lm > invoke_dt:
            elapsed = round(time.time() - invoke_t0, 1)
            print(f"   ✓ research_critique_flat.json appeared after {elapsed}s")
            out["steps"]["snapshot_rerun"] = {"elapsed_s": elapsed, "ok": True}
            break
    else:
        out["steps"]["snapshot_rerun"] = {"ok": False, "note": "timed out polling"}
        print(f"   ⚠ poll timeout")
except Exception as e:
    out["steps"]["snapshot_rerun"] = {"error": str(e)[:300]}
    print(f"   ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 7: Verify flat file
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1172] 7. Verify analytics/research_critique_flat.json")
try:
    obj = s3.get_object(Bucket=BUCKET, Key="analytics/research_critique_flat.json")
    doc = json.loads(obj["Body"].read())
    rows = doc.get("rows", [])
    out["steps"]["flat_verify"] = {
        "exists": True,
        "n_rows": len(rows),
        "schema_version": doc.get("schema_version"),
        "generated_at": doc.get("generated_at"),
        "sample_rows": [
            {k: v for k, v in r.items() if k in
              ["ticker", "analyst_rating", "alternative_rating", "disagreement_score",
               "rating_diverges", "key_disagreement", "critic_model"]}
            for r in rows[:5]
        ],
    }
    print(f"   ✓ {len(rows)} rows · schema v{doc.get('schema_version')}")
    for r in rows[:3]:
        print(f"     {r.get('ticker')}: analyst={r.get('analyst_rating')} critic={r.get('alternative_rating')} dis={r.get('disagreement_score')}")
except Exception as e:
    out["steps"]["flat_verify"] = {"error": str(e)[:300]}
    print(f"   ❌ {e}")

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1172] DONE")
