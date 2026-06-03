"""1232 — Quick verify multi-horizon Lambda + S3 schema."""
import json
from datetime import datetime, timezone
import boto3
from botocore.config import Config

REPORT = "aws/ops/reports/1232_horizon_verify.json"
BUCKET = "justhodl-dashboard-live"
REGION = "us-east-1"
cfg = Config(read_timeout=120, retries={"max_attempts": 1})
lam = boto3.client("lambda", region_name=REGION, config=cfg)
s3 = boto3.client("s3", region_name=REGION, config=cfg)

out = {"started": datetime.now(timezone.utc).isoformat()}

# 1. Confirm Lambda has new code
print("[1232] 1. Lambda state check")
info = lam.get_function_configuration(FunctionName="justhodl-self-improvement")
out["lambda"] = {
    "state": info.get("State"),
    "last_update": info.get("LastUpdateStatus"),
    "code_sha": info.get("CodeSha256")[:16],
    "modified": info.get("LastModified")[:19],
}
print(f"  state={info.get('State')} update={info.get('LastUpdateStatus')} sha={info.get('CodeSha256')[:16]}")
print(f"  modified={info.get('LastModified')[:19]}")

# 2. Invoke + read response
print("\n[1232] 2. Quick invoke")
import time
t0 = time.time()
resp = lam.invoke(FunctionName="justhodl-self-improvement",
                   InvocationType="RequestResponse", Payload=b"{}")
elapsed = round(time.time() - t0, 1)
payload = resp.get("Payload").read().decode()
out["invoke"] = {"status": resp.get("StatusCode"),
                  "elapsed_s": elapsed,
                  "function_error": resp.get("FunctionError"),
                  "body": payload[:1200]}
print(f"  status={resp.get('StatusCode')} elapsed={elapsed}s")
if resp.get("FunctionError"):
    print(f"  ⚠ {payload[:400]}")

# 3. Verify horizon_attribution field in cascade-calibration.json
print("\n[1232] 3. Verify horizon_attribution in calibration.json")
try:
    cal = json.loads(s3.get_object(Bucket=BUCKET, Key="data/cascade-calibration.json")["Body"].read())
    out["cal_fields"] = {
        "has_horizon_attribution": "horizon_attribution" in cal,
        "horizon_attribution_keys": list((cal.get("horizon_attribution") or {}).keys()),
        "by_horizon_keys": list(((cal.get("horizon_attribution") or {}).get("by_horizon") or {}).keys()),
        "has_best_horizon_per_feature": "best_horizon_per_feature" in (cal.get("horizon_attribution") or {}),
    }
    print(f"  ✓ has horizon_attribution: {out['cal_fields']['has_horizon_attribution']}")
    print(f"  ✓ horizon keys: {out['cal_fields']['by_horizon_keys']}")
except Exception as e:
    out["cal_fields"] = {"error": str(e)[:200]}

out["finished"] = datetime.now(timezone.utc).isoformat()
with open(REPORT, "w") as f:
    json.dump(out, f, indent=2, default=str)
print("\n[1232] DONE")
