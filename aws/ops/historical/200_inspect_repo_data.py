#!/usr/bin/env python3
"""Step 200 — inspect repo-data.json shape (S3 read)."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")

with report("inspect_repo_data") as r:
    r.heading("Inspect repo-data.json")
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="repo-data.json")
    data = json.loads(obj["Body"].read())
    r.log(f"Top-level keys ({len(data)}):")
    for k, v in data.items():
        if isinstance(v, dict):
            r.log(f"  {k}: dict({len(v)} keys)")
            for k2, v2 in list(v.items())[:6]:
                if isinstance(v2, (dict, list)):
                    r.log(f"    {k2}: {type(v2).__name__}({len(v2)})")
                else:
                    r.log(f"    {k2}: {repr(v2)[:100]}")
        elif isinstance(v, list):
            r.log(f"  {k}: list[{len(v)}]")
            if v and isinstance(v[0], dict):
                r.log(f"    [0] keys: {list(v[0].keys())[:8]}")
                for k2, v2 in list(v[0].items())[:8]:
                    r.log(f"      {k2}: {repr(v2)[:80]}")
        else:
            r.log(f"  {k}: {repr(v)[:120]}")
    r.log("Done")
