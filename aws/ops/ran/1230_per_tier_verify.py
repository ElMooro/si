"""1230 — Verify per-tier calibration fix + final check."""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1230_per_tier_verify.json"
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"

cfg = Config(read_timeout=200, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

# Wait for deploy
time.sleep(90)

# Invoke recalibrator (should work now)
print("[1230] 1. Invoke recalibrator (post-fix)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-cascade-recalibrator",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["invoke"] = {"status": resp.get("StatusCode"), "elapsed_s": elapsed,
                      "function_error": resp.get("FunctionError"), "body": payload[:1500]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:400]}")
    else:
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  ok={inner.get('ok')}")
            print(f"  candidates={inner.get('n_total_candidates')}")
            print(f"  global_weights={inner.get('n_weights_applied')}")
            print(f"  per_tier_active={inner.get('per_tier_calibration_active')}")
            print(f"  per_tier_weight_counts={inner.get('n_weights_by_tier')}")
            print(f"  confidence={inner.get('calibration_confidence')}")
        except: pass
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

# Read S3 outputs
print(f"\n[1230] 2. Verify S3 outputs written")
for key in ["data/theme-cascade-calibrated.json", "data/cascade-recalibration-audit.json"]:
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        print(f"  ✓ {key}: {head['ContentLength']} bytes, modified {head['LastModified'].isoformat()[:19]}")
    except Exception as e:
        print(f"  ✗ {key}: {e}")

# Final audit doc
print(f"\n[1230] 3. Final audit doc state")
try:
    audit = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cascade-recalibration-audit.json")["Body"].read())
    out["audit"] = {
        "schema": audit.get("schema_version") or audit.get("blend", {}).get("confidence"),
        "per_tier_active": audit.get("per_tier_calibration_active"),
        "n_global_weights": audit.get("n_weights_global"),
        "n_per_tier": audit.get("n_weights_by_tier"),
        "methodology_excerpt": (audit.get("methodology") or "")[:200],
        "rank_changes": audit.get("rank_changes"),
    }
    print(f"  per_tier_active: {audit.get('per_tier_calibration_active')}")
    print(f"  n_global_weights: {audit.get('n_weights_global')}")
    print(f"  methodology: {(audit.get('methodology') or '')[:120]}...")
    rc = audit.get('rank_changes') or {}
    for tier, info in rc.items():
        if isinstance(info, dict):
            print(f"  {tier}: weight_source={info.get('weight_source')}, retention={info.get('top_10_retention_pct')}%")
except Exception as e:
    out["audit"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1230] DONE")
