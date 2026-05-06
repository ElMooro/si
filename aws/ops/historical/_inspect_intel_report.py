"""Inspect intelligence-report.json content + last_modified."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("inspect_intel_report") as r:
        for key in ["intelligence-report.json", "intelligence/latest.json", "predictions.json"]:
            try:
                obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=key)
                last = obj["LastModified"].isoformat()
                size = obj["ContentLength"]
                d = json.loads(obj["Body"].read())
                r.heading(f"{key}  {size:,}b  {last}")
                if isinstance(d, dict):
                    r.log(f"  keys: {list(d.keys())[:20]}")
                    for k, v in list(d.items())[:15]:
                        if isinstance(v, str):
                            r.log(f"  {k}: {v[:200]}")
                        elif isinstance(v, (int, float)):
                            r.log(f"  {k}: {v}")
                        elif isinstance(v, list):
                            r.log(f"  {k}: list len={len(v)}, sample[0]={str(v[0])[:120] if v else 'empty'}")
                        elif isinstance(v, dict):
                            r.log(f"  {k}: dict keys={list(v.keys())[:8]}")
                        else:
                            r.log(f"  {k}: {type(v).__name__}")
            except Exception as e:
                r.log(f"  ✗ {key}: {e}")


if __name__ == "__main__":
    main()
