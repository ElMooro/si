"""Ops verify: invoke the two new engines on AAPL, confirm live output + S3 write.
Runs in-account via run-ops.yml (has AWS creds). Writes report to aws/ops/reports/."""
import json, boto3, time, os
from datetime import datetime, timezone

lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3")
BUCKET = "justhodl-dashboard-live"
report = {"generated": datetime.now(timezone.utc).isoformat(), "engines": {}}

for fn, s3key in [("justhodl-investor-lenses", "data/investor-lenses/AAPL.json"),
                  ("justhodl-technical-overlays", "data/technical-overlays/AAPL.json")]:
    entry = {}
    try:
        # confirm function exists + config
        cfg = lam.get_function_configuration(FunctionName=fn)
        entry["exists"] = True
        entry["runtime"] = cfg.get("Runtime")
        entry["env_keys"] = sorted((cfg.get("Environment", {}).get("Variables", {}) or {}).keys())
        entry["last_modified"] = cfg.get("LastModified")
        # live invoke on AAPL
        r = lam.invoke(FunctionName=fn, Payload=json.dumps({"ticker": "AAPL"}).encode())
        payload = json.loads(r["Payload"].read().decode())
        entry["invoke_status"] = r.get("StatusCode")
        entry["invoke_body"] = payload.get("body", payload) if isinstance(payload, dict) else payload
        # confirm S3 output written
        time.sleep(2)
        head = s3.head_object(Bucket=BUCKET, Key=s3key)
        entry["s3_written"] = True
        entry["s3_bytes"] = head["ContentLength"]
        entry["s3_key"] = s3key
    except Exception as e:
        entry["error"] = f"{type(e).__name__}: {e}"
    report["engines"][fn] = entry

os.makedirs("aws/ops/reports", exist_ok=True)
with open("aws/ops/reports/verify_new_engines.json", "w") as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
