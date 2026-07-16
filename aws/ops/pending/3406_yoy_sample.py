"""ops 3406 — dump the full monthly long-history so I can render the 3 YoY-transform options
locally for comparison."""
import json, boto3
from ops_report import report
s3=boto3.client("s3",region_name="us-east-1")
with report("3406_yoy_sample") as r:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/global-sovereign-longhistory.json")["Body"].read())
    h=d["history"]
    # dump every point compactly
    r.log(f"n={len(h)}")
    r.log(json.dumps([[p["date"][:7],p["stress"]] for p in h]))
