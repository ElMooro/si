"""ops_4345 -- adapter recon: real top-level + row keys for the five
dead sources (+squeeze-fuel existence)."""
import json, sys
import boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
B = "justhodl-dashboard-live"
with report("4345_adapter_recon") as r:
    r.heading("ops 4345 -- what the artifacts actually say")
    for key in ("data/ai-rerating-radar.json",
                "data/magic-formula.json",
                "data/opportunities.json",
                "data/insider-clusters.json",
                "data/congress-direct.json",
                "data/squeeze-fuel.json",
                "data/short-interest.json"):
        try:
            d = json.loads(s3.get_object(Bucket=B, Key=key)
                           ["Body"].read())
        except Exception as e:
            r.warn("%s: %s" % (key, str(e)[:60]))
            continue
        tops = list(d)[:9]
        rows = None
        for rk in tops:
            if isinstance(d.get(rk), list) and d[rk] \
                    and isinstance(d[rk][0], dict):
                rows = (rk, d[rk][0])
                break
        r.log("%s tops=%s" % (key.split("/")[-1], tops))
        if rows:
            r.log("   rows@'%s' r0keys=%s"
                  % (rows[0], list(rows[1])[:16]))
            samp = {k: str(rows[1][k])[:28]
                    for k in list(rows[1])[:8]}
            r.log("   sample=%s" % json.dumps(samp))
    r.ok("recon complete")
    if False:
        sys.exit(1)
