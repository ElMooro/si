"""ops 3404 — dump a sample of the long-history for the render test."""
import json, boto3
from ops_report import report
s3=boto3.client("s3",region_name="us-east-1")
with report("3404_dump_lh") as r:
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/global-sovereign-longhistory.json")["Body"].read())
    h=d["history"]
    # every 12th point to keep it small
    sample=h[::12]
    r.log(f"n={len(h)} sampled={len(sample)}")
    r.log(json.dumps([{"date":p["date"],"stress":p["stress"]} for p in sample]))
