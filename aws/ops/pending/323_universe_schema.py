#!/usr/bin/env python3
"""Step 323 — Find universe.json actual schema + sniff."""
import json
import os
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
REPORT = "aws/ops/reports/323_universe_schema.json"

s3 = boto3.client("s3", region_name=REGION)


def main():
    obj = s3.get_object(Bucket=BUCKET, Key="data/universe.json")
    d = json.loads(obj["Body"].read())
    out = {
        "as_of": datetime.now(timezone.utc).isoformat(),
        "size_kb": round(obj["ContentLength"]/1024, 1),
    }
    if isinstance(d, dict):
        out["type"] = "dict"
        out["top_keys"] = list(d.keys())[:25]
        out["per_key"] = {}
        for k in list(d.keys())[:25]:
            v = d[k]
            if isinstance(v, list):
                out["per_key"][k] = {
                    "type": "list",
                    "len": len(v),
                    "first_3": v[:3],
                }
            elif isinstance(v, dict):
                out["per_key"][k] = {
                    "type": "dict",
                    "n_keys": len(v),
                    "sample_keys": list(v.keys())[:6],
                    "sample_value": str(list(v.values())[0])[:120] if v else None,
                }
            else:
                out["per_key"][k] = {"type": type(v).__name__, "value": str(v)[:120]}
    elif isinstance(d, list):
        out["type"] = "list"
        out["len"] = len(d)
        out["first_3"] = d[:3]

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])


if __name__ == "__main__":
    main()
