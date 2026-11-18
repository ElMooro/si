"""ops 6828 - READ-ONLY audit before taking the tearsheet (and its donut) to the other desks (re-runnable).

Khalid 2026-11-17: "apply these concepts from the picture I attached and that donut everywhere in my system where
you see it's necessary." Audit before building: which desks publish (a) a ranked universe with a period-over-period
change and (b) a whole that adds up - the two things a tearsheet and a donut need. Reads the candidate feeds from
the live bucket, prints their shapes and freshness, and embeds a trimmed copy of each (lists cut to 50 items) so
the pages can be built and tested offline against the real numbers. Nothing is deployed or written but this report.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import re
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=60)
FEEDS = ["data/ark-holdings.json", "ark-holdings.json", "data/smart-money-13f.json", "data/firm-book.json", "data/paper-book.json", "data/proven-portfolio.json", "data/proven-portfolio-history.json",
         "data/strategy-portfolio.json", "data/whales.json", "data/institutional-footprint.json", "data/stablecoin-flow.json", "data/auction-desk.json", "data/insider-trades.json", "data/insider-clusters.json",
         "data/sp500.json", "data/rotation-dashboard.json", "data/spx-beaters.json", "data/strength.json", "data/leaders.json", "data/apac-flows.json", "data/short-book.json", "data/katlin.json", "data/bottom.json",
         "data/fortress.json", "data/etf-census.json", "data/accumulation-radar.json", "data/master-ranker.json", "data/crypto-opportunities.json", "data/tic-flows.json", "data/etf-true-flows.json", "flow-data.json",
         "data/industry-rotation.json", "data/smart-money-clusters.json", "data/cftc.json", "data/cftc-positioning.json", "data/cot.json", "data/13f-tickers.json", "data/capital-inflows.json",
         "data/cross-asset-flow-state.json", "data/etf-flows.json", "data/insider-trades-full.json", "data/concentration-liquidity.json"]
KEY_RE = re.compile(r"cftc|cot[-_.]|ark|positioning|holdings|portfolio|book", re.I)
KEEP = 50


def shape(v, depth=0):
    if isinstance(v, dict):
        if depth >= 3:
            return "{%d keys}" % len(v)
        return "{" + ", ".join("%s: %s" % (k, shape(x, depth + 1)) for k, x in list(v.items())[:16]) + (", ...+%d" % (len(v) - 16) if len(v) > 16 else "") + "}"
    if isinstance(v, list):
        return "[%d x %s]" % (len(v), shape(v[0], depth + 1) if v else "")
    s = repr(v)
    return s if len(s) < 30 else s[:27] + "..."


def trim(v):
    if isinstance(v, dict):
        if len(v) > 400:
            keys = list(v.keys())[:KEEP]
            return {k: trim(v[k]) for k in keys}
        return {k: trim(x) for k, x in v.items()}
    if isinstance(v, list):
        return [trim(x) for x in (v if len(v) <= KEEP else v[:KEEP - 10] + v[-10:])]
    return v


def main():
    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    pack = {}
    with report("ops_6828_tearsheet_everywhere_feed_audit") as r:
        r.heading("ops 6828 - tearsheet everywhere, feed audit %s (read-only)" % now.isoformat(timespec="minutes"))
        r.section("1. candidate feeds")
        found = 0
        for key in FEEDS:
            try:
                obj = s3.get_object(Bucket=LIVE, Key=key)
            except Exception as exc:  # noqa: BLE001
                r.log("%-40s %s" % (key, type(exc).__name__))
                continue
            body = obj["Body"].read()
            try:
                doc = json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body)
            except Exception as exc:  # noqa: BLE001
                r.log("%-40s unreadable %s" % (key, type(exc).__name__))
                continue
            found += 1
            age = (now - obj["LastModified"]).total_seconds() / 86400
            r.log("**%s** %d B, %.1f d old: %s" % (key, len(body), age, shape(doc)[:1700]))
            pack[key] = trim(doc)
        r.section("2. other keys that look like holdings / positioning")
        for prefix in ("data/", ""):
            token = None
            keys = []
            while True:
                kw = {"Bucket": LIVE, "Prefix": prefix, "Delimiter": "/", "MaxKeys": 1000}
                if token:
                    kw["ContinuationToken"] = token
                resp = s3.list_objects_v2(**kw)
                keys.extend(resp.get("Contents") or [])
                token = resp.get("NextContinuationToken")
                if not token:
                    break
            hits = [k for k in keys if KEY_RE.search(k["Key"]) and k["Key"] not in FEEDS]
            r.log("%s top-level objects %d, matching %d" % (prefix or "(root)", len(keys), len(hits)))
            for k in sorted(hits, key=lambda z: z["Key"])[:70]:
                r.log("    %-60s %9d B %6.1f d old" % (k["Key"], k["Size"], (now - k["LastModified"]).total_seconds() / 86400))
        blob = base64.b64encode(gzip.compress(json.dumps(pack, separators=(",", ":"), default=str).encode("utf-8"), 9)).decode("ascii")
        r.log("pack: %d feeds, %d chars" % (len(pack), len(blob)))
        r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN")
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("FEED-PACK-GZ-B64-END -->")
        r.kv(feeds_requested=str(len(FEEDS)), feeds_found=str(found), pack_chars=str(len(blob)))
        r.ok("AUDIT: %d of %d candidate feeds read" % (found, len(FEEDS)))


if __name__ == "__main__":
    main()
    sys.exit(0)
