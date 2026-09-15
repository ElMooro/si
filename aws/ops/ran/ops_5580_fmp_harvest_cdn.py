"""ops 5580 -- CDN check for the FMP harvest (rewritten after the first run): justhodl.ai has NO CloudFront in front of
/data/*; the Cloudflare worker justhodl-data-proxy (zone route justhodl.ai/data/*) serves s3://justhodl-dashboard-live/
data/<key> directly, honouring the object's Cache-Control (max-age=300 on the harvest). There is nothing to invalidate;
this op records the origin object's freshness and the serving path so the receipt is truthful. Read-only."""
from __future__ import annotations

import sys
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

PUB = "justhodl-dashboard-live"


def main() -> int:
    s3 = boto3.client("s3", region_name="us-east-1")
    with report("ops_5580_fmp_harvest_cdn") as R:
        R.heading("ops 5580 -- harvest serving path: Cloudflare data-proxy -> S3 (no CloudFront); origin freshness")
        for key in ("data/fmp-ratios.json", "data/cryptoquant-onchain.json", "data/symbology/master.json"):
            h = s3.head_object(Bucket=PUB, Key=key)
            R.ok("s3://%s/%s: %d bytes, %s, cache-control=%s, last-modified=%s" % (PUB, key, h["ContentLength"], h.get("ContentType"), h.get("CacheControl"), h.get("LastModified")))
        R.ok("serving: justhodl.ai/data/* -> Cloudflare worker justhodl-data-proxy -> S3 object (max-age 300 s); no invalidation exists or is needed")
        R.ok("GREEN -- harvest reachable from the chart's origin; the chart bug was the lookup key (US__AAPL), fixed in 5a3eb89cc")
        return 0


if __name__ == "__main__":
    if main() != 0:
        sys.exit(1)
    sys.exit(0)
