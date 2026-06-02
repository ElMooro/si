"""1168 — Analytics rollout orchestration.

Steps:
  1. Add 'analytics/*' to bucket policy (PublicReadAnalytics statement)
  2. Create EventBridge rule + Lambda permission for nightly snapshot
  3. Manually invoke snapshot Lambda to populate flat files (synchronous)
  4. Verify the flat files exist + are served via CDN
  5. Smoke test by hitting the CF proxy for both flat files
"""
import json
import time
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1168_analytics_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA_NAME = "justhodl-analytics-snapshot"
RULE_NAME = "justhodl-analytics-snapshot-nightly"
SCHEDULE = "cron(0 9 * * ? *)"
ACCOUNT_ID = "857687956942"
REGION = "us-east-1"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT_ID}:role/lambda-execution-role"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)

out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}

# ═══════════════════════════════════════════════════════════════════
# Step 1: Bucket policy — ensure analytics/* is publicly readable
# ═══════════════════════════════════════════════════════════════════
print("[1168] Step 1: bucket policy")
try:
    cur = s3.get_bucket_policy(Bucket=BUCKET)
    policy = json.loads(cur["Policy"])
except s3.exceptions.from_code("NoSuchBucketPolicy"):
    policy = {"Version": "2012-10-17", "Statement": []}
except Exception as e:
    print(f"  Couldn't read existing policy: {e}")
    policy = {"Version": "2012-10-17", "Statement": []}

# Check if there's already a PublicReadAnalytics statement; if so skip
sids = [s.get("Sid", "") for s in policy.get("Statement", [])]
out["steps"]["bucket_policy"] = {"prior_sids": sids}

if "PublicReadAnalytics" not in sids:
    new_stmt = {
        "Sid": "PublicReadAnalytics",
        "Effect": "Allow",
        "Principal": "*",
        "Action": ["s3:GetObject"],
        "Resource": [f"arn:aws:s3:::{BUCKET}/analytics/*"]
    }
    policy["Statement"].append(new_stmt)
    s3.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps(policy))
    out["steps"]["bucket_policy"]["added"] = True
    out["steps"]["bucket_policy"]["new_sid"] = "PublicReadAnalytics"
    print("  ✓ Added PublicReadAnalytics statement")
else:
    out["steps"]["bucket_policy"]["added"] = False
    print("  ✓ Already present")

# ═══════════════════════════════════════════════════════════════════
# Step 2: EventBridge schedule
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1168] Step 2: EventBridge schedule {RULE_NAME}")
try:
    # Create or update the rule
    events.put_rule(
        Name=RULE_NAME,
        ScheduleExpression=SCHEDULE,
        State="ENABLED",
        Description="04:00 ET — rebuild analytics flat files (1h after prewarm)",
    )
    out["steps"]["schedule_rule"] = {"created": True, "expression": SCHEDULE}
    print(f"  ✓ rule created/updated: {SCHEDULE}")

    # Get Lambda ARN
    fn = lam.get_function(FunctionName=LAMBDA_NAME)
    fn_arn = fn["Configuration"]["FunctionArn"]
    out["steps"]["schedule_rule"]["lambda_arn"] = fn_arn

    # Attach Lambda as target
    events.put_targets(
        Rule=RULE_NAME,
        Targets=[{"Id": "1", "Arn": fn_arn}],
    )
    print(f"  ✓ target attached")

    # Permission for EventBridge to invoke
    sid = f"EBInvoke-{RULE_NAME}"
    try:
        lam.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId=sid,
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}",
        )
        out["steps"]["schedule_rule"]["permission_added"] = True
        print(f"  ✓ permission added")
    except lam.exceptions.ResourceConflictException:
        out["steps"]["schedule_rule"]["permission_added"] = False
        print(f"  ✓ permission already present")
except Exception as e:
    out["steps"]["schedule_rule"] = {"error": str(e)[:300]}
    print(f"  ❌ {e}")

# ═══════════════════════════════════════════════════════════════════
# Step 3: Manually invoke snapshot Lambda (synchronous, get the result)
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1168] Step 3: invoke snapshot Lambda (sync, ~30-60s)")
t0 = time.time()
try:
    resp = lam.invoke(
        FunctionName=LAMBDA_NAME,
        InvocationType="RequestResponse",
        Payload=b"{}",
    )
    elapsed = round(time.time() - t0, 1)
    payload = resp["Payload"].read().decode()
    status = resp.get("StatusCode")
    body = json.loads(payload) if payload else {}
    if "body" in body and isinstance(body["body"], str):
        try:
            body["body"] = json.loads(body["body"])
        except Exception:
            pass
    out["steps"]["invoke"] = {
        "status_code": status,
        "elapsed_s": elapsed,
        "response": body,
    }
    print(f"  ✓ HTTP {status} in {elapsed}s")
    if "body" in body and isinstance(body["body"], dict):
        b = body["body"]
        print(f"    n_research_rows: {b.get('n_research_rows')}")
        print(f"    n_edgar_rows:    {b.get('n_edgar_rows')}")
except Exception as e:
    out["steps"]["invoke"] = {"error": str(e)[:400], "elapsed_s": round(time.time()-t0,1)}
    print(f"  ❌ {e}")

# Small pause for S3 consistency
time.sleep(3)

# ═══════════════════════════════════════════════════════════════════
# Step 4: Verify the flat files exist in S3
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1168] Step 4: verify S3 objects")
s3_checks = {}
for key in [
    "analytics/equity_research_flat.json",
    "analytics/edgar_insiders_flat.json",
    "analytics/manifest.json",
]:
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        s3_checks[key] = {
            "exists": True,
            "size_kb": round(head["ContentLength"] / 1024, 1),
            "last_modified": head["LastModified"].isoformat(),
            "content_type": head.get("ContentType"),
        }
        print(f"  ✓ {key}: {s3_checks[key]['size_kb']}KB")
    except Exception as e:
        s3_checks[key] = {"error": str(e)[:200]}
        print(f"  ❌ {key}: {e}")
out["steps"]["s3_verify"] = s3_checks

# ═══════════════════════════════════════════════════════════════════
# Step 5: CDN smoke test
# ═══════════════════════════════════════════════════════════════════
print(f"\n[1168] Step 5: CDN smoke test")
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
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Ops/1.0"})
        with urllib.request.urlopen(req, timeout=20, context=ctx) as r:
            body = r.read()
            elapsed = round(time.time() - t0, 2)
            try:
                doc = json.loads(body)
                n_rows = len(doc.get("rows", [])) if "rows" in doc else None
                cdn_checks[key] = {
                    "http": r.status, "elapsed_s": elapsed,
                    "size_kb": round(len(body)/1024, 1),
                    "n_rows": n_rows,
                    "generated_at": doc.get("generated_at"),
                    "schema_version": doc.get("schema_version"),
                }
                print(f"  ✓ {key}: {cdn_checks[key]['size_kb']}KB · {n_rows or '?'} rows · {elapsed}s")
            except Exception as e:
                cdn_checks[key] = {"http": r.status, "parse_err": str(e)[:200]}
                print(f"  ⚠ {key}: HTTP {r.status} but parse failed")
    except urllib.error.HTTPError as e:
        cdn_checks[key] = {"http": e.code, "elapsed_s": round(time.time()-t0,2)}
        print(f"  ❌ {key}: HTTP {e.code}")
    except Exception as e:
        cdn_checks[key] = {"error": str(e)[:200]}
        print(f"  ❌ {key}: {e}")
out["steps"]["cdn_smoke"] = cdn_checks

# ═══════════════════════════════════════════════════════════════════
# Summary
# ═══════════════════════════════════════════════════════════════════
out["finished"] = datetime.now(timezone.utc).isoformat()
all_steps_ok = all(
    isinstance(v, dict) and not v.get("error") and "error" not in str(v.get("response",""))
    for v in out["steps"].values()
)
cdn_all_ok = all(c.get("http") == 200 for c in cdn_checks.values())
out["all_ok"] = all_steps_ok and cdn_all_ok

with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1168] DONE — all_ok={out['all_ok']}")
