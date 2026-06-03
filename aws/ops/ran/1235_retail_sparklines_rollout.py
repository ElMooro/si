"""1235 — Deploy retail sentiment integration + verify sparklines work.

Updates:
  - justhodl-prediction-snapshotter (reads retail-sentiment, creates RETAIL_* preds)
  - justhodl-self-improvement (tracks retail features in attribution + 2 new tiers)

Invokes snapshotter to verify RETAIL_VELOCITY tier appears.
"""
import json
import os
import time
import zipfile
import io
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1235_retail_sparklines_rollout.json"
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"

cfg = Config(read_timeout=600, retries={"max_attempts": 1})
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


# 1. Update Lambdas
LAMBDAS = [
    ("justhodl-prediction-snapshotter", "aws/lambdas/justhodl-prediction-snapshotter/source"),
    ("justhodl-self-improvement", "aws/lambdas/justhodl-self-improvement/source"),
]
print("[1235] 1. Update Lambdas")
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
        out["updates"][name] = {"sha": c.get("CodeSha256")[:16],
                                  "state": c.get("State")}
        print(f"  ✓ {name}: sha={c.get('CodeSha256')[:16]}")
    except Exception as e:
        out["updates"][name] = {"error": str(e)[:300]}

# 2. Invoke snapshotter to test retail integration
print("\n[1235] 2. Invoke prediction-snapshotter (test retail integration)")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName="justhodl-prediction-snapshotter",
                       InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["snap_invoke"] = {"status": resp.get("StatusCode"), "elapsed_s": elapsed,
                            "function_error": resp.get("FunctionError"),
                            "body": payload[:1500]}
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:400]}")
    else:
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  n_predictions: {inner.get('n_predictions')}")
            print(f"  alert_distribution: {inner.get('alert_distribution')}")
        except Exception as e:
            print(f"  parse: {e}")
except Exception as e:
    out["snap_invoke"] = {"error": str(e)[:300]}

# 3. Verify retail features in latest snapshot + check for RETAIL_VELOCITY tier
print("\n[1235] 3. Verify retail features in latest snapshot")
try:
    snap = json.loads(s3.get_object(Bucket=BUCKET, Key="data/predictions-snapshots/latest.json")["Body"].read())
    preds = snap.get("predictions") or []
    
    # Check for RETAIL_VELOCITY tickers
    retail_velocity_tickers = [p for p in preds 
                                 if "RETAIL_VELOCITY" in (p.get("alerts") or [])]
    retail_hot_tickers = [p for p in preds 
                            if "RETAIL_HOT" in (p.get("alerts") or [])]
    
    # Check for retail features anywhere
    preds_with_retail = [p for p in preds 
                          if (p.get("features") or {}).get("retail_velocity_pct") is not None]
    
    # Sample retail features
    sample_retail = []
    for p in preds_with_retail[:5]:
        sample_retail.append({
            "ticker": p.get("ticker"),
            "alerts": p.get("alerts"),
            "retail_velocity_pct": (p.get("features") or {}).get("retail_velocity_pct"),
            "retail_mentions": (p.get("features") or {}).get("retail_mentions"),
            "retail_rank_climb": (p.get("features") or {}).get("retail_rank_climb"),
        })
    
    out["retail_check"] = {
        "n_total_predictions": len(preds),
        "n_retail_velocity_tier": len(retail_velocity_tickers),
        "n_retail_hot_tier": len(retail_hot_tickers),
        "n_preds_with_retail_features": len(preds_with_retail),
        "retail_velocity_sample": [t.get("ticker") for t in retail_velocity_tickers[:8]],
        "retail_hot_sample": [t.get("ticker") for t in retail_hot_tickers[:8]],
        "sample_features": sample_retail,
    }
    print(f"  ✓ Total predictions: {len(preds)}")
    print(f"  ✓ RETAIL_VELOCITY tier: {len(retail_velocity_tickers)} new tickers")
    print(f"  ✓ RETAIL_HOT tier: {len(retail_hot_tickers)} tickers")
    print(f"  ✓ Predictions with retail features: {len(preds_with_retail)}")
    if retail_velocity_tickers:
        print(f"\n  Top RETAIL_VELOCITY tickers:")
        for t in retail_velocity_tickers[:5]:
            v = (t.get("features") or {}).get("retail_velocity_pct")
            m = (t.get("features") or {}).get("retail_mentions")
            print(f"    {t.get('ticker')}: velocity={v}%, mentions={m}")
except Exception as e:
    out["retail_check"] = {"error": str(e)[:300]}

# 4. Confirm calibration history exists for sparkline rendering
print("\n[1235] 4. Verify calibration history availability for sparklines")
try:
    cal = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cascade-calibration.json")["Body"].read())
    history = cal.get("history") or []
    out["calibration_history"] = {
        "n_entries": len(history),
        "first_date": history[0].get("date") if history else None,
        "latest_date": history[-1].get("date") if history else None,
        "weights_in_latest": len((history[-1].get("weights") or {})) if history else 0,
    }
    print(f"  ✓ History entries: {len(history)}")
    print(f"  ✓ Sparklines will render once history has ≥2 entries (current: {len(history)})")
    if history:
        print(f"  ✓ Latest entry has {out['calibration_history']['weights_in_latest']} weights tracked")
except Exception as e:
    out["calibration_history"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print("\n[1235] DONE")
