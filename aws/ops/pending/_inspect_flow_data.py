"""Inspect flow-data.json to know what it provides."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("inspect_flow_data") as r:
        r.heading("flow-data.json shape")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="flow-data.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  top-level keys: {list(d.keys())[:20]}")
            for k, v in d.items():
                if isinstance(v, list):
                    r.log(f"  {k}: list len={len(v)}")
                    if v and isinstance(v[0], dict):
                        r.log(f"    first item keys: {list(v[0].keys())[:10]}")
                        r.log(f"    sample: {json.dumps(v[0], default=str)[:200]}")
                elif isinstance(v, dict):
                    r.log(f"  {k}: dict keys={list(v.keys())[:10]}")
                else:
                    r.log(f"  {k}: {str(v)[:100]}")
        except Exception as e:
            r.log(f"  ✗ {e}")

        r.heading("vix-curve.json")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/vix-curve.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  full: {json.dumps(d, default=str, indent=2)[:800]}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
