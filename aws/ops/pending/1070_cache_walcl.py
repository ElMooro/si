#!/usr/bin/env python3
"""1070 — see actual WALCL list entries."""
import json, os, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1070_cache_walcl.json"
s3 = boto3.client("s3", region_name="us-east-1")

obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/fred-cache.json")
d = json.loads(obj["Body"].read())

out = {
    "started": datetime.now(timezone.utc).isoformat(),
    "WALCL_type": type(d.get("WALCL")).__name__,
    "WALCL_first_3": d.get("WALCL", [])[:3],
    "WALCL_last_3":  d.get("WALCL", [])[-3:],
    "WALCL_count":   len(d.get("WALCL", [])),
    "WTREGEN_last_3": d.get("WTREGEN", [])[-3:],
    "WTREGEN_count":  len(d.get("WTREGEN", [])),
    "RRPONTSYD_last_3": d.get("RRPONTSYD", [])[-3:],
    "RRPONTSYD_count":  len(d.get("RRPONTSYD", [])),
}

pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
print(f"[1070] DONE")
