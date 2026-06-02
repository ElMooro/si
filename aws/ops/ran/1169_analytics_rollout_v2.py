"""1169 — Analytics rollout v2: async invoke + S3 polling pattern.

Replaces 1168 which hung on synchronous Lambda invoke (boto3 default config
hit some edge case). Async invoke is the standard pattern for Lambdas that
take >60s — fire and poll S3 for the result objects.
"""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1169_analytics_rollout_v2.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-analytics-snapshot"
RULE_NAME = "justhodl-analytics-snapshot-nightly"
SCHEDULE = "cron(0 9 * * ? *)"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"

cfg = Config(read_timeout=30, retries={"max_attempts": 1})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
events = boto3.client("events", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}

# ═══════════════════════════════════════════════════════════════════
# Step 1: Bucket policy
# ═══════════════════════════════════════════════════════════════════
print("[1169] Step 1: bucket policy")
try:
    cur = s3.get_bucket_policy(Bucket=BUCKET)
    policy = json.loads(cur["Policy"])
    sids = [s.get("Sid", "") for s in policy.get("Statement", [])]
    out["steps"]["bucket_policy"] = {"prior_sids": sids}

    if "PublicReadAnalytics" not in sids:
        policy["Statement"].append({
            "Sid": "PublicReadAnalytics",
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject"],
            "Resource": [f"arn:aws:s3:::{BUCKET}/analytics/*"],
        })
        s3.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps(policy))
        out["steps"]["bucket_policy"]["added"] = True
        print("  ✓ Added PublicReadAnalytics")
    else:
        out["steps"]["bucket_policy"]["added"] = False
        print("  ✓ Already present")
except Exception as e:
    out["steps"]["bucket_policy"] = {"error": str(e)[:300]}
    print(f"  ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 2: EventBridge schedule
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1169] Step 2: EventBridge schedule")
try:
    events.put_rule(
        Name=RULE_NAME, ScheduleExpression=SCHEDULE, State="ENABLED",
        Description="04:00 ET — rebuild analytics flat files (1h after prewarm)",
    )
    fn = lam.get_function(FunctionName=LAMBDA_NAME)
    fn_arn = fn["Configuration"]["FunctionArn"]
    events.put_targets(Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])

    sid = f"EBInvoke-{RULE_NAME}"
    try:
        lam.add_permission(
            FunctionName=LAMBDA_NAME, StatementId=sid,
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}",
        )
        out["steps"]["schedule"] = {"created": True, "permission_added": True}
    except lam.exceptions.ResourceConflictException:
        out["steps"]["schedule"] = {"created": True, "permission_added": False, "note": "already present"}
    print("  ✓ schedule + target + permission set")
except Exception as e:
    out["steps"]["schedule"] = {"error": str(e)[:300]}
    print(f"  ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 3: ASYNC invoke + poll S3 for the result objects
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1169] Step 3: async invoke snapshot Lambda + poll S3")
invoke_t0 = time.time()
try:
    # Capture pre-invoke last-modified so we can detect change
    def head_lm(key):
        try:
            return s3.head_object(Bucket=BUCKET, Key=key)["LastModified"]
        except Exception:
            return None

    pre_research = head_lm("analytics/equity_research_flat.json")
    pre_edgar = head_lm("analytics/edgar_insiders_flat.json")
    print(f"  pre-invoke: research_lm={pre_research}, edgar_lm={pre_edgar}")

    resp = lam.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="Event",  # async
        Payload=b"{}",
    )
    print(f"  async invoke returned status={resp['StatusCode']}")

    # Poll S3 for the files to appear / update
    print(f"  polling S3 for updates...")
    poll_start = time.time()
    new_research_lm = None
    new_edgar_lm = None
    for i in range(60):  # poll up to 60 × 3s = 3 minutes
        time.sleep(3)
        cur_research = head_lm("analytics/equity_research_flat.json")
        cur_edgar = head_lm("analytics/edgar_insiders_flat.json")
        # Both files updated and post-invoke timestamp
        invoke_dt = datetime.fromtimestamp(invoke_t0, timezone.utc)
        r_ok = cur_research and cur_research > invoke_dt
        e_ok = cur_edgar and cur_edgar > invoke_dt
        if r_ok and e_ok:
            new_research_lm = cur_research
            new_edgar_lm = cur_edgar
            print(f"  ✓ both files updated after {round(time.time()-poll_start,1)}s")
            break
        if i % 5 == 4:
            print(f"  still polling... ({i*3+3}s) r_ok={r_ok} e_ok={e_ok}")

    out["steps"]["invoke"] = {
        "invoke_status": resp["StatusCode"],
        "poll_elapsed_s": round(time.time() - poll_start, 1),
        "research_updated": bool(new_research_lm),
        "edgar_updated": bool(new_edgar_lm),
        "research_lm": str(new_research_lm) if new_research_lm else None,
        "edgar_lm": str(new_edgar_lm) if new_edgar_lm else None,
    }
except Exception as e:
    out["steps"]["invoke"] = {"error": str(e)[:300]}
    print(f"  ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 4: Verify the flat files have data
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1169] Step 4: verify file contents")
verify = {}
for key in [
    "analytics/equity_research_flat.json",
    "analytics/edgar_insiders_flat.json",
    "analytics/manifest.json",
]:
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        body = obj["Body"].read()
        doc = json.loads(body)
        n_rows = len(doc.get("rows", [])) if "rows" in doc else None
        verify[key] = {
            "exists": True,
            "size_kb": round(len(body) / 1024, 1),
            "n_rows": n_rows,
            "generated_at": doc.get("generated_at"),
            "schema_version": doc.get("schema_version"),
        }
        if "tables" in doc:  # manifest
            verify[key]["tables"] = doc["tables"]
            verify[key]["n_research_cols"] = len(doc.get("schema_research_columns", []))
            verify[key]["n_edgar_cols"] = len(doc.get("schema_edgar_columns", []))
        print(f"  ✓ {key}: {verify[key]['size_kb']}KB, {n_rows or '?'} rows")
    except Exception as e:
        verify[key] = {"error": str(e)[:200]}
        print(f"  ❌ {key}: {e}")
out["steps"]["s3_verify"] = verify

# ═══════════════════════════════════════════════════════════════════
# Step 5: CDN smoke
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1169] Step 5: CDN smoke")
import urllib.request, urllib.error, ssl
ctx = ssl.create_default_context()
cdn = "https://justhodl-data-proxy.raafouis.workers.dev"
cdn_checks = {}
for key in [
    "analytics/equity_research_flat.json",
    "analytics/edgar_insiders_flat.json",
    "analytics/manifest.json",
]:
    url = f"{cdn}/{key}?v={int(time.time())}"
    t0 = time.time()
    try:
        with urllib.request.urlopen(
            urllib.request.Request(url, headers={"User-Agent": "JustHodl-Ops/1.0"}),
            timeout=15, context=ctx,
        ) as r:
            body = r.read()
            elapsed = round(time.time() - t0, 2)
            doc = json.loads(body)
            cdn_checks[key] = {
                "http": r.status, "elapsed_s": elapsed,
                "size_kb": round(len(body) / 1024, 1),
                "n_rows": len(doc.get("rows", [])) if "rows" in doc else None,
            }
            print(f"  ✓ {key}: HTTP {r.status} in {elapsed}s ({cdn_checks[key]['size_kb']}KB)")
    except Exception as e:
        cdn_checks[key] = {"error": str(e)[:200]}
        print(f"  ❌ {key}: {e}")
out["steps"]["cdn_smoke"] = cdn_checks

# Sample some research rows for verification
try:
    research_obj = s3.get_object(Bucket=BUCKET, Key="analytics/equity_research_flat.json")
    research_doc = json.loads(research_obj["Body"].read())
    sample_rows = research_doc.get("rows", [])[:3]
    out["sample_research_rows"] = [
        {k: v for k, v in r.items() if k in
         ["ticker", "rating", "upside_pct", "pe_ttm", "roic_ttm_pct", "rev_5y_cagr_pct"]}
        for r in sample_rows
    ]
except Exception as e:
    out["sample_research_rows"] = [{"err": str(e)[:200]}]

out["finished"] = datetime.now(timezone.utc).isoformat()
all_ok = (
    not any("error" in str(v) for v in out["steps"].values()) and
    all(c.get("http") == 200 for c in cdn_checks.values() if isinstance(c, dict))
)
out["all_ok"] = all_ok

with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1169] DONE — all_ok={all_ok}")
