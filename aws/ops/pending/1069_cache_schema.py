#!/usr/bin/env python3
"""1069 — inspect actual fred-cache.json schema for WALCL/WTREGEN/RRPONTSYD."""
import json, os, pathlib
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/1069_cache_schema.json"
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    
    obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/fred-cache.json")
    body = obj["Body"].read()
    d = json.loads(body)
    
    out["top_level_keys_count"] = len(d.keys()) if isinstance(d, dict) else None
    
    # Dump the structure of the 3 key series
    for sid in ["WALCL", "WTREGEN", "RRPONTSYD"]:
        if sid in d:
            entry = d[sid]
            out[sid] = {
                "type":       type(entry).__name__,
                "top_keys":   list(entry.keys()) if isinstance(entry, dict) else None,
                "raw_sample": json.dumps(entry, default=str)[:1000] if isinstance(entry, dict) else None,
            }
        else:
            out[sid] = {"err": "not in cache"}
    
    # Also peek at one random series to see the cache structure
    for k, v in list(d.items())[:3]:
        if isinstance(v, dict):
            out["sample_other_" + k] = json.dumps(v, default=str)[:800]
            break
    
    out["finished"] = datetime.now(timezone.utc).isoformat()
    pathlib.Path(os.path.dirname(REPORT)).mkdir(parents=True, exist_ok=True)
    pathlib.Path(REPORT).write_text(json.dumps(out, indent=2, default=str))
    print(f"[1069] DONE → {REPORT}")


if __name__ == "__main__":
    main()
