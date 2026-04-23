#!/usr/bin/env python3
"""
Inspect live shapes of flow-data.json and crypto-intel.json so v2.1
knows exactly which keys/paths to pull.
"""

import json
from ops_report import report
import boto3

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def summarize(data, prefix="", max_depth=3, depth=0, max_keys_shown=12):
    lines = []
    if depth >= max_depth:
        lines.append(f"{prefix} … (depth limit)")
        return lines
    if isinstance(data, dict):
        keys = list(data.keys())
        for k in keys[:max_keys_shown]:
            v = data[k]
            if isinstance(v, dict):
                lines.append(f"{prefix}{k}/ ({len(v)} keys)")
                lines.extend(summarize(v, prefix + "  ", max_depth, depth + 1, max_keys_shown))
            elif isinstance(v, list):
                sample = ""
                if v:
                    if isinstance(v[0], dict):
                        sample = f" [list of dict, first keys: {list(v[0].keys())[:5]}]"
                    else:
                        sample = f" [{type(v[0]).__name__}, e.g., {str(v[0])[:60]}]"
                lines.append(f"{prefix}{k}: list({len(v)}){sample}")
            elif isinstance(v, (int, float)):
                lines.append(f"{prefix}{k}: {v}")
            elif isinstance(v, str):
                lines.append(f"{prefix}{k}: \"{v[:60]}\"")
            elif v is None:
                lines.append(f"{prefix}{k}: None")
            else:
                lines.append(f"{prefix}{k}: {type(v).__name__}")
        if len(keys) > max_keys_shown:
            lines.append(f"{prefix}… and {len(keys) - max_keys_shown} more keys")
    return lines


with report("inspect_tier2_shapes") as r:
    r.heading("Live shapes of flow-data.json + crypto-intel.json")

    for key in ("flow-data.json", "crypto-intel.json"):
        r.section(key)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=key)
            lm = obj["LastModified"].isoformat()
            size = obj["ContentLength"]
            data = json.loads(obj["Body"].read().decode())
            r.log(f"  LastModified: {lm}  Size: {size} bytes")
            r.kv(file=key, last_modified=lm, size=size)
            for line in summarize(data):
                r.log("  " + line)
        except Exception as e:
            r.fail(f"  fetch failed: {e}")

    r.log("Done")
