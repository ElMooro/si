#!/usr/bin/env python3
"""Step 306 — Inspect schemas of theme-tiers.json, supply-inflection.json,
pead-signals.json so the 3 orphan-signal HTML pages display correctly.
"""
import json
import os
from datetime import datetime, timezone

import boto3

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
KEYS = [
    "data/theme-tiers.json",
    "data/supply-inflection.json",
    "data/pead-signals.json",
]
REPORT = "aws/ops/reports/306_orphan_schemas.json"

s3 = boto3.client("s3", region_name=REGION)


def describe(obj, max_depth=3, depth=0):
    """Recursively summarize a JSON structure for design purposes."""
    if depth >= max_depth:
        return f"<{type(obj).__name__}>"
    if isinstance(obj, dict):
        return {
            k: describe(v, max_depth, depth + 1)
            for k, v in list(obj.items())[:10]
        }
    if isinstance(obj, list):
        if not obj:
            return "[]"
        if isinstance(obj[0], (dict, list)):
            return [describe(obj[0], max_depth, depth + 1), f"... ({len(obj)} items total)"]
        return f"<list of {len(obj)} {type(obj[0]).__name__}>"
    return str(obj)[:80] + ("…" if len(str(obj)) > 80 else "")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "files": {}}
    for key in KEYS:
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            data = json.loads(obj["Body"].read())
            entry = {
                "size_kb": round(obj["ContentLength"] / 1024, 1),
                "last_modified": obj["LastModified"].isoformat(),
                "schema": describe(data, max_depth=4),
            }
            # If list-of-dicts at top: capture first item full
            if isinstance(data, list) and data and isinstance(data[0], dict):
                entry["first_item_full"] = data[0]
            elif isinstance(data, dict):
                # Capture top-level keys + sample of any list inside
                for k, v in data.items():
                    if isinstance(v, list) and v and isinstance(v[0], dict):
                        entry[f"sample_of_{k}"] = v[0]
                        break
            out["files"][key] = entry
        except Exception as e:
            out["files"][key] = {"err": str(e)[:200]}
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:6000])


if __name__ == "__main__":
    main()
