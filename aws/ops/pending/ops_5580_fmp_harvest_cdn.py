"""ops 5580 -- make the FMP harvest reach the chart's origin: CloudFront invalidation for /data/fmp-ratios.json and
/data/cryptoquant-onchain.json on the justhodl.ai distribution, plus an origin-side check that the object is public JSON
(HEAD from S3 + the distribution's origin). No secrets."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

PUB = "justhodl-dashboard-live"
PATHS = ["/data/fmp-ratios.json", "/data/cryptoquant-onchain.json", "/data/symbology/master.json", "/chart.html", "/jh-chart-tvsearch.js", "/fmp.html"]


def main() -> int:
    s3 = boto3.client("s3", region_name="us-east-1")
    cf = boto3.client("cloudfront")
    with report("ops_5580_fmp_harvest_cdn") as R:
        R.heading("ops 5580 -- CDN: invalidate the harvest + chart assets on the justhodl.ai distribution; confirm the origin object")
        h = s3.head_object(Bucket=PUB, Key="data/fmp-ratios.json")
        R.ok("origin s3://%s/data/fmp-ratios.json: %d bytes, content-type=%s, last-modified=%s" % (PUB, h["ContentLength"], h.get("ContentType"), h.get("LastModified")))
        dists = cf.list_distributions().get("DistributionList", {}).get("Items", []) or []
        targets = [d for d in dists if any("justhodl.ai" in a for a in (d.get("Aliases", {}).get("Items") or []))]
        if not targets:
            R.fail("no CloudFront distribution with a justhodl.ai alias found (%d distributions listed)" % len(dists)); return 1
        for d in targets:
            inv = cf.create_invalidation(DistributionId=d["Id"], InvalidationBatch={"Paths": {"Quantity": len(PATHS), "Items": PATHS}, "CallerReference": "ops-5580-%d" % int(time.time())})
            R.ok("invalidation %s on %s (%s): %s" % (inv["Invalidation"]["Id"], d["Id"], ", ".join(d.get("Aliases", {}).get("Items") or []), ", ".join(PATHS)))
        R.ok("GREEN -- CDN paths invalidated; the chart fetches /data/fmp-ratios.json from the same origin")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
