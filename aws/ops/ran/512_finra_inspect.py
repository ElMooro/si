#!/usr/bin/env python3
"""512 — Inspect FINRA sidecar structure (field names, top-level keys, top values)."""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/512_finra_inspect.json"
s3 = boto3.client("s3", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/finra-short.json")
        body = obj["Body"].read()
        p = json.loads(body)
        out["size_kb"] = round(len(body) / 1024, 1)
        out["modified"] = obj["LastModified"].isoformat()[:19]
        out["top_level_keys"] = list(p.keys())
        # Sample each top-level key (truncated)
        out["samples"] = {}
        for k, v in p.items():
            if isinstance(v, list):
                out["samples"][k] = {"type": "list", "len": len(v),
                                       "first_2": v[:2] if v else []}
            elif isinstance(v, dict):
                out["samples"][k] = {"type": "dict", "keys": list(v.keys())[:15],
                                       "n_keys": len(v),
                                       "first_value": (list(v.values())[0] if v else None)}
            else:
                out["samples"][k] = {"type": type(v).__name__, "value": v}
    except Exception as e:
        out["err"] = str(e)[:300]

    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
