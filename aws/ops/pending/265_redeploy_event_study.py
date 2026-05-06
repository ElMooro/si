#!/usr/bin/env python3
"""Step 265 — Force-redeploy event-study Lambda from local source.

The probe in 264 showed spx: 2513 obs in CloudWatch logs but the
response said 'no SPX data' — meaning deployed code != repo source.
Push the current source via boto3, then re-invoke and check S3 output.
"""
import boto3, io, os, time, zipfile, json
from datetime import datetime, timezone

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-event-study"
SOURCE_DIR = f"aws/lambdas/{LAMBDA_NAME}/source"
REPORT_PATH = "aws/ops/reports/265_event_study_redeploy.json"
BUCKET = "justhodl-dashboard-live"
KEY = "data/event-study.json"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)

# 1. Build zip and update
buf = io.BytesIO()
with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
    for root, _d, files in os.walk(SOURCE_DIR):
        for fn in files:
            fp = os.path.join(root, fn)
            zf.write(fp, os.path.relpath(fp, SOURCE_DIR))
zip_bytes = buf.getvalue()
print(f"[265] zip {len(zip_bytes):,}b")

lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zip_bytes)
lam.get_waiter("function_updated").wait(FunctionName=LAMBDA_NAME)
print("[265] code updated")

# 2. Invoke
print("[265] invoking…")
inv = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", Payload=b"{}")
payload = json.loads(inv["Payload"].read())
print(f"  status={inv.get('StatusCode')}  err={inv.get('FunctionError')}")
print(f"  payload: {payload}")
time.sleep(3)

# 3. Re-read S3
body = json.loads(s3.get_object(Bucket=BUCKET, Key=KEY)["Body"].read())
studies = body.get("studies") or {}

out = {
    "redeployed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    "invoke_status": inv.get("StatusCode"),
    "invoke_err": inv.get("FunctionError"),
    "invoke_payload": payload,
    "n_studies": len(studies),
    "active_themes": body.get("active_themes"),
    "expected_21d_return_from_active_pct": body.get("expected_21d_return_from_active_pct"),
    "studies_n_events": {k: v.get("n_events", 0) for k, v in studies.items()},
    "studies_currently_active": {k: v.get("currently_active") for k, v in studies.items()},
    "first_study_summary": next(iter(studies.values()), None),
}
os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
open(REPORT_PATH, "w").write(json.dumps(out, indent=2, default=str))
print(json.dumps(out, indent=2, default=str)[:4000])
