"""1229 — Redeploy self-improvement + recalibrator with per-tier calibration + verify."""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1229_per_tier_calibration_rollout.json"
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"

cfg = Config(read_timeout=300, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}


def build_zip(source_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _, files in os.walk(source_dir):
            for f in files:
                if f.startswith("__") or f.endswith(".pyc"):
                    continue
                fpath = os.path.join(root, f)
                rel = os.path.relpath(fpath, source_dir)
                zf.write(fpath, arcname=rel)
    return buf.getvalue()


# 1. Update both Lambdas
LAMBDAS = [
    ("justhodl-self-improvement", "aws/lambdas/justhodl-self-improvement/source"),
    ("justhodl-cascade-recalibrator", "aws/lambdas/justhodl-cascade-recalibrator/source"),
]
print("[1229] 1. Update Lambdas")
out["updates"] = {}
for name, src in LAMBDAS:
    try:
        zip_bytes = build_zip(src)
        lam.update_function_code(FunctionName=name, ZipFile=zip_bytes)
        for _ in range(15):
            time.sleep(2)
            c = lam.get_function_configuration(FunctionName=name)
            if c.get("LastUpdateStatus") == "Successful":
                break
        out["updates"][name] = {
            "state": c.get("State"),
            "code_sha": c.get("CodeSha256")[:16],
            "modified": c.get("LastModified")[:19],
        }
        print(f"  ✓ {name}: sha={c.get('CodeSha256')[:16]}")
    except Exception as e:
        out["updates"][name] = {"error": str(e)[:300]}
        print(f"  ❌ {name}: {e}")

# 2. Invoke self-improvement (will use bootstrap since no scored data yet)
print(f"\n[1229] 2. Invoke self-improvement (per-tier attribution)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-self-improvement",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["si_invoke"] = {"status": resp.get("StatusCode"), "elapsed_s": elapsed,
                          "function_error": resp.get("FunctionError"),
                          "body": payload[:1500]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:400]}")
    else:
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  scored={inner.get('n_predictions_scored')} valid={inner.get('n_valid_outcomes')} "
                  f"features={inner.get('n_features_attributed')} calibrated={inner.get('calibrated')}")
        except: pass
except Exception as e:
    out["si_invoke"] = {"error": str(e)[:300]}

# 3. Invoke recalibrator (will pick up new per-tier weights)
print(f"\n[1229] 3. Invoke cascade-recalibrator (uses per-tier weights)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-cascade-recalibrator",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["recal_invoke"] = {"status": resp.get("StatusCode"), "elapsed_s": elapsed,
                             "function_error": resp.get("FunctionError"),
                             "body": payload[:1500]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:400]}")
    else:
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  candidates={inner.get('n_total_candidates')} weights={inner.get('n_weights_applied')}")
        except: pass
except Exception as e:
    out["recal_invoke"] = {"error": str(e)[:300]}

# 4. Read calibration file to verify per-tier structure
print(f"\n[1229] 4. Verify per-tier calibration structure")
try:
    cal = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cascade-calibration.json")["Body"].read())
    out["calibration"] = {
        "last_updated": cal.get("last_updated"),
        "n_global_weights": len(cal.get("current_weights") or {}),
        "tiers_with_weights": list((cal.get("current_weights_by_tier") or {}).keys()),
        "tier_weight_counts": {tier: len(w) for tier, w in (cal.get("current_weights_by_tier") or {}).items()},
        "feature_attribution_by_tier_tiers": list((cal.get("feature_attribution_by_tier") or {}).get("by_tier", {}).keys()),
        "tier_distribution": (cal.get("feature_attribution_by_tier") or {}).get("tier_distribution"),
    }
    print(f"  ✓ Global weights: {out['calibration']['n_global_weights']}")
    print(f"  ✓ Tiers with separate weights: {out['calibration']['tiers_with_weights']}")
    if out['calibration']['tier_distribution']:
        print(f"  ✓ Tier distribution: {out['calibration']['tier_distribution']}")
except Exception as e:
    out["calibration"] = {"error": str(e)[:200]}

# 5. Read recalibration audit to verify per-tier annotations
print(f"\n[1229] 5. Verify recalibration audit (per-tier metadata)")
try:
    audit = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cascade-recalibration-audit.json")["Body"].read())
    out["audit"] = {
        "per_tier_calibration_active": audit.get("per_tier_calibration_active"),
        "n_weights_global": audit.get("n_weights_global"),
        "n_weights_by_tier": audit.get("n_weights_by_tier"),
        "rank_changes": audit.get("rank_changes"),
        "methodology": audit.get("methodology", "")[:300],
    }
    print(f"  ✓ Per-tier active: {audit.get('per_tier_calibration_active')}")
    print(f"  ✓ Methodology updated for per-tier")
    rc = audit.get("rank_changes") or {}
    for tier, info in rc.items():
        if isinstance(info, dict):
            print(f"    {tier}: weight_source={info.get('weight_source')}, n_weights={info.get('n_weights_applied')}")
except Exception as e:
    out["audit"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1229] DONE")
