import json, os, boto3
from datetime import datetime, timezone

s3 = boto3.client("s3", region_name="us-east-1")
today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
key = f"system-events/audit/{today}.jsonl"

try:
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
    body = obj["Body"].read().decode("utf-8")
    lines = [l for l in body.split("\n") if l.strip()]
    out = {"n_entries": len(lines), "entries": []}
    for line in lines[-10:]:
        entry = json.loads(line)
        out["entries"].append(entry)
    with open("aws/ops/reports/1022b_audit_inspection.json", "w") as f:
        json.dump(out, f, indent=2, default=str)
except Exception as e:
    with open("aws/ops/reports/1022b_audit_inspection.json", "w") as f:
        json.dump({"err": str(e)}, f, indent=2)
