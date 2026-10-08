"""1231 — Deploy multi-horizon self-improvement + invoke + verify."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1231_multi_horizon_rollout.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-self-improvement"
SOURCE_DIR = "aws/lambdas/justhodl-self-improvement/source"
REGION = "us-east-1"

cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}


def build_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"):
                    continue
                fpath = os.path.join(root, f)
                rel = os.path.relpath(fpath, SOURCE_DIR)
                zf.write(fpath, arcname=rel)
    return buf.getvalue()


# Deploy
print(f"[1231] 1. Update {LAMBDA} with multi-horizon scoring")
try:
    zip_bytes = build_zip()
    lam.update_function_code(FunctionName=LAMBDA, ZipFile=zip_bytes)
    for _ in range(15):
        time.sleep(2)
        c = lam.get_function_configuration(FunctionName=LAMBDA)
        if c.get("LastUpdateStatus") == "Successful":
            break
    out["update"] = {"state": c.get("State"), "code_sha": c.get("CodeSha256")[:16]}
    print(f"  ✓ updated · code_sha={c.get('CodeSha256')[:16]}")
except Exception as e:
    out["update_err"] = str(e)[:300]
    print(f"  ❌ {e}")

# Invoke (bootstrap mode since no scored data yet — but verifies code runs)
print(f"\n[1231] 2. Invoke (verify multi-horizon code path)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
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
            print(f"  scored={inner.get('n_predictions_scored')} valid={inner.get('n_valid_outcomes')} "
                  f"features={inner.get('n_features_attributed')}")
        except: pass
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

# Verify horizon_attribution in S3 output
print(f"\n[1231] 3. Verify horizon_attribution field in calibration.json")
try:
    cal = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cascade-calibration.json")["Body"].read())
    ha = cal.get("horizon_attribution") or {}
    out["horizon_attribution"] = {
        "has_field": "horizon_attribution" in cal,
        "insufficient_data": ha.get("insufficient_data"),
        "n_valid_total": ha.get("n_valid_total"),
        "horizons_with_data": list((ha.get("by_horizon") or {}).keys()),
        "n_best_horizon_features": len(ha.get("best_horizon_per_feature") or {}),
        "best_horizon_per_feature": ha.get("best_horizon_per_feature"),
    }
    print(f"  ✓ horizon_attribution field present: {out['horizon_attribution']['has_field']}")
    print(f"  insufficient_data: {ha.get('insufficient_data', False)} (expected True on Day 1)")
    print(f"  horizons configured: {list((ha.get('by_horizon') or {}).keys())}")
    print(f"  n_valid_total: {ha.get('n_valid_total', 0)}")
except Exception as e:
    out["horizon_attribution"] = {"error": str(e)[:200]}

# Verify scored predictions have new return_14d, return_21d, return_30d fields
print(f"\n[1231] 4. Verify scored predictions have multi-horizon return fields")
try:
    # Get most recent scored predictions
    keys = s3.list_objects_v2(Bucket=BUCKET, Prefix="data/predictions-scored/", MaxKeys=10)
    contents = sorted([k["Key"] for k in keys.get("Contents", [])])
    if contents:
        latest = contents[-1]
        scored = json.loads(s3.get_object(Bucket=BUCKET, Key=latest)["Body"].read())
        sample = (scored.get("scored") or [])[:1]
        if sample:
            keys_in_sample = list(sample[0].keys())
            horizon_keys = [k for k in keys_in_sample if k.startswith("return_") and k.endswith("d_pct")]
            out["scored_schema"] = {
                "file": latest,
                "n_predictions": scored.get("n_predictions"),
                "sample_ticker": sample[0].get("ticker"),
                "sample_outcome": sample[0].get("outcome"),
                "horizon_return_fields": sorted(horizon_keys),
                "has_hit_by_horizon": "hit_by_horizon" in sample[0],
            }
            print(f"  ✓ file: {latest}")
            print(f"  horizon return fields: {sorted(horizon_keys)}")
            print(f"  has hit_by_horizon: {'hit_by_horizon' in sample[0]}")
    else:
        out["scored_schema"] = {"info": "no scored predictions yet"}
        print(f"  ℹ no scored predictions yet (expected on Day 1)")
except Exception as e:
    out["scored_schema"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1231] DONE")
