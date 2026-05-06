"""Find correct eurodollar-stress S3 path."""
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")


def main():
    with report("find_eurodollar_path") as r:
        for prefix in ["data/", "eurodollar/", ""]:
            try:
                resp = s3.list_objects_v2(
                    Bucket="justhodl-dashboard-live",
                    Prefix=prefix,
                    MaxKeys=50,
                )
                r.log(f"  prefix='{prefix}':")
                for o in resp.get("Contents", []):
                    if "eurodollar" in o["Key"].lower():
                        r.log(f"    {o['Key']:60s} {o['Size']:>9,}b")
            except Exception as e:
                r.log(f"  ✗ {e}")
        r.log("")
        # Also check sample shapes
        for k in ["data/eurodollar-stress.json", "eurodollar/current.json", "data/eurodollar-current.json", "eurodollar-stress.json"]:
            try:
                obj = s3.head_object(Bucket="justhodl-dashboard-live", Key=k)
                r.log(f"  ✓ {k} EXISTS  {obj['ContentLength']:>9,}b")
            except Exception as e:
                pass

        # Look at historical-analogs and event-study shape
        for k in ["data/historical-analogs.json", "data/event-study.json", "data/auction-crisis.json", "data/macro-surprise.json", "data/yield-curve.json"]:
            try:
                import json
                obj = s3.get_object(Bucket="justhodl-dashboard-live", Key=k)
                d = json.loads(obj["Body"].read())
                r.log(f"  {k} keys: {list(d.keys())[:10]}")
            except Exception as e:
                r.log(f"  ✗ {k}: {e}")


if __name__ == "__main__":
    main()
