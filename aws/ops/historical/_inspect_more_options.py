"""Find where options-flow data actually lives + check Polygon options endpoint key."""
import json
import boto3
from ops_report import report

s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")


def main():
    with report("inspect_more_options") as r:
        r.heading("Search all S3 keys with 'flow' or 'skew' or 'vol' in name")
        paginator = s3.get_paginator("list_objects_v2")
        all_keys = []
        for page in paginator.paginate(Bucket="justhodl-dashboard-live"):
            for it in page.get("Contents", []):
                all_keys.append((it["Key"], it["Size"], it["LastModified"]))
        # filter
        for k, sz, lm in all_keys:
            if any(t in k.lower() for t in ["options", "flow", "skew", "vol", "vix", "iv"]):
                r.log(f"  {k:60s} {sz:>9,}b  {lm.isoformat()}")

        # Find justhodl-options-flow Lambda
        r.heading("Find justhodl-options* Lambdas")
        next_marker = None
        while True:
            kw = {"MaxItems": 50}
            if next_marker:
                kw["Marker"] = next_marker
            resp = lam.list_functions(**kw)
            for f in resp["Functions"]:
                name = f["FunctionName"]
                if "options" in name.lower() or "vix" in name.lower() or "skew" in name.lower():
                    r.log(f"  {name:50s} runtime={f['Runtime']:12s} memory={f['MemorySize']}MB")
            next_marker = resp.get("NextMarker")
            if not next_marker:
                break

        # Show options-gamma.json content
        r.heading("data/options-gamma.json")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/options-gamma.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  keys: {list(d.keys())}")
            r.log(f"  full: {json.dumps(d, default=str)[:600]}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
