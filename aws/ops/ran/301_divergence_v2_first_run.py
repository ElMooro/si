#!/usr/bin/env python3
"""Step 301 — Fetch the divergence-v2.json output to see what's flagging.

The first run flagged 5 extreme + 5 flagged. This script pulls the
actual JSON to show which 10 cross-asset pairs are dislocated right now.
"""
import json
import os
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEY = "data/divergence-v2.json"
REPORT = "aws/ops/reports/301_divergence_v2_first_run.json"

s3 = boto3.client("s3", region_name=REGION)


def main():
    obj = s3.get_object(Bucket=BUCKET, Key=KEY)
    data = json.loads(obj["Body"].read())

    # Distill the interesting bits
    out = {
        "as_of": data.get("as_of"),
        "composite_index": data.get("composite_divergence_index"),
        "n_relationships": data.get("n_relationships"),
        "n_with_data": data.get("n_with_data"),
        "by_status": data.get("by_status"),
        "fetch_errors": data.get("fetch_errors"),
        "extreme_alerts": [
            {
                "id": a["id"], "name": a["name"], "category": a.get("category"),
                "z_a": a.get("z_a"), "z_b": a.get("z_b"),
                "divergence_z": a.get("divergence_z"),
                "description": a.get("description", "")[:140],
            }
            for a in (data.get("extreme_alerts") or [])
        ],
        "flagged": [
            {
                "id": a["id"], "name": a["name"], "category": a.get("category"),
                "divergence_z": a.get("divergence_z"),
            }
            for a in (data.get("flagged") or [])
        ],
        "by_category_counts": {
            cat: len(items) for cat, items in (data.get("by_category") or {}).items()
        },
    }

    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:5000])


if __name__ == "__main__":
    main()
