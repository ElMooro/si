"""Inspect options-flow data to understand schema available for skew page."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("inspect_options_data") as r:
        r.heading("Options data inventory")
        keys = [
            "data/options-flow.json",
            "data/options-flow-v2.json",
            "options-flow.json",
            "data/skew.json",
            "data/vol-surface.json",
        ]
        for k in keys:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=k)
                r.ok(f"  ✓ {k:40s} {obj['ContentLength']:>9,}b  modified={obj['LastModified'].isoformat()}")
            except Exception:
                r.log(f"  ✗ {k}  not found")

        # Also list everything under options/
        r.heading("All keys under options* prefix")
        try:
            paginator = s3.get_paginator("list_objects_v2")
            for prefix in ["data/options", "options"]:
                for page in paginator.paginate(Bucket="justhodl-dashboard-live", Prefix=prefix):
                    for it in page.get("Contents", []):
                        r.log(f"  {it['Key']:60s} {it['Size']:>9,}b")
        except Exception as e:
            r.log(f"  ✗ {e}")

        # Sample existing options-flow.json schema if present
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/options-flow.json")
            d = json.loads(obj["Body"].read())
            r.heading("data/options-flow.json shape")
            r.log(f"  top-level keys: {list(d.keys())[:15]}")
            for k in list(d.keys())[:5]:
                v = d[k]
                if isinstance(v, list):
                    r.log(f"  {k}: list len={len(v)}")
                    if v:
                        r.log(f"    sample: {json.dumps(v[0], default=str)[:300]}")
                elif isinstance(v, dict):
                    r.log(f"  {k}: dict keys={list(v.keys())[:8]}")
                else:
                    r.log(f"  {k}: {str(v)[:120]}")
        except Exception as e:
            r.log(f"  ✗ options-flow.json: {e}")


if __name__ == "__main__":
    main()
