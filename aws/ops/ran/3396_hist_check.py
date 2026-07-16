"""ops 3396 — check the accumulating history + locate the actual risk/black-swan engines
for the upcoming fusion (so targets are precise, not guessed)."""
import json, boto3
from ops_report import report
s3=boto3.client("s3",region_name="us-east-1")
with report("3396_hist_and_targets") as r:
    r.section("Barometer history so far")
    try:
        h=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/global-sovereign-history.json")["Body"].read())
        r.log(f"  history points: {len(h)}")
        for x in h[-5:]: r.log(f"    {x}")
    except Exception as e:
        r.log(f"  history: {e}")
    d=json.loads(s3.get_object(Bucket="justhodl-dashboard-live",Key="data/global-sovereign.json")["Body"].read())
    r.log(f"  current: stress={d.get('eurodollar_hub_stress_0_100')} pctile={d.get('eurodollar_hub_percentile')} hist_n={d.get('eurodollar_hub_history_n')}")
