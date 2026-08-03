"""ops_4322 -- the stored growth_vs_mcap block for TSM, verbatim."""
import json, sys
import boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
with report("4322_growth_block") as r:
    r.heading("ops 4322 -- growth block, verbatim")
    d = json.loads(s3.get_object(
        Bucket="justhodl-dashboard-live",
        Key="equity-research/TSM.json")["Body"].read())
    g = d.get("growth_vs_mcap") or {}
    r.log("generated_at=%s" % d.get("generated_at"))
    r.ok("growth_vs_mcap: %s" % json.dumps(g))
    if g.get("pe_ttm") is None:
        r.fail("no pe_ttm in growth block")
        sys.exit(1)
