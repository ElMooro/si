"""1226 — Re-invoke recalibrator + verify S3 output directly."""
import json
import time
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1226_recalibrator_verify.json"
BUCKET = "justhodl-dashboard-live"
LAMBDA = "justhodl-cascade-recalibrator"
REGION = "us-east-1"

cfg = Config(read_timeout=120, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

# 1. Check Lambda function exists & is Active
print(f"[1226] 1. Lambda state check")
try:
    cfg_info = lam.get_function_configuration(FunctionName=LAMBDA)
    out["function_state"] = {
        "state": cfg_info.get("State"),
        "last_update_status": cfg_info.get("LastUpdateStatus"),
        "code_sha": cfg_info.get("CodeSha256"),
        "modified": cfg_info.get("LastModified"),
        "timeout": cfg_info.get("Timeout"),
        "memory": cfg_info.get("MemorySize"),
    }
    print(f"  state={cfg_info.get('State')}  last_update={cfg_info.get('LastUpdateStatus')}")
    print(f"  modified={cfg_info.get('LastModified')[:19]}")
except Exception as e:
    out["function_state"] = {"error": str(e)[:200]}
    print(f"  ❌ {e}")

# 2. Sync invoke
print(f"\n[1226] 2. Re-invoke Lambda")
try:
    t0 = time.time()
    resp = lam.invoke(FunctionName=LAMBDA, InvocationType="RequestResponse", Payload=b"{}")
    elapsed = round(time.time() - t0, 1)
    payload = resp.get("Payload").read().decode()
    out["invoke"] = {
        "elapsed_s": elapsed,
        "status": resp.get("StatusCode"),
        "function_error": resp.get("FunctionError"),
        "body": payload[:3000],
    }
    print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
    if resp.get("FunctionError"):
        print(f"  ⚠ {payload[:800]}")
    else:
        try:
            inner = json.loads(json.loads(payload).get("body", "{}"))
            print(f"  ok={inner.get('ok')} n_candidates={inner.get('n_total_candidates')} n_weights={inner.get('n_weights_applied')}")
            blend = inner.get("blend") or {}
            print(f"  confidence={blend.get('confidence')}  rationale={blend.get('rationale','')}")
        except Exception as e:
            print(f"  parse: {e}  body: {payload[:300]}")
except Exception as e:
    out["invoke"] = {"error": str(e)[:300]}

# 3. Verify S3 outputs
print(f"\n[1226] 3. Verify S3 outputs written")
for key in ["data/theme-cascade-calibrated.json", "data/cascade-recalibration-audit.json"]:
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        sz = head["ContentLength"]
        mod = head["LastModified"].isoformat()[:19]
        print(f"  ✓ {key}: {sz} bytes, modified {mod}")
        out[f"s3_{key.split('/')[-1]}"] = {"size": sz, "modified": mod}
    except Exception as e:
        print(f"  ✗ {key}: {e}")
        out[f"s3_{key.split('/')[-1]}"] = {"error": str(e)[:120]}

# 4. Read calibrated cascade content
print(f"\n[1226] 4. Read calibrated cascade content")
try:
    cal = json.loads(s3.get_object(Bucket=BUCKET, Key="data/theme-cascade-calibrated.json")["Body"].read())
    audit = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cascade-recalibration-audit.json")["Body"].read())
    
    out["content"] = {
        "blend": cal.get("blend"),
        "n_predictions": cal.get("calibration_n_predictions"),
        "alert_tier_count": len(cal.get("alert_tier") or []),
        "laggards_count": len(cal.get("laggards_hot_themes") or []),
        "audit_blend": audit.get("blend"),
        "audit_rank_changes": audit.get("rank_changes"),
        "top_weights": audit.get("top_weights"),
        "alert_tier_top5": [{
            "ticker": c.get("ticker"),
            "combined": c.get("combined_score"),
            "original": c.get("original_combined_score"),
            "calibrated": c.get("calibrated_combined_score"),
            "adj": c.get("calibration_adjustment"),
        } for c in (cal.get("alert_tier") or [])[:5]],
        "laggards_top5": [{
            "ticker": c.get("ticker"),
            "combined": c.get("combined_score"),
            "original": c.get("original_combined_score"),
            "calibrated": c.get("calibrated_combined_score"),
            "adj": c.get("calibration_adjustment"),
        } for c in (cal.get("laggards_hot_themes") or [])[:5]],
    }
    print(f"  ✓ alert_tier: {len(cal.get('alert_tier') or [])}, laggards: {len(cal.get('laggards_hot_themes') or [])}")
    print(f"  ✓ blend: {cal.get('blend',{}).get('original','?'):.0%}/{cal.get('blend',{}).get('calibrated','?'):.0%}")
except Exception as e:
    out["content"] = {"error": str(e)[:300]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print(f"\n[1226] DONE")
