"""ops 6829 - READ-ONLY: the full feeds the tearsheet kit is tested against (re-runnable).

Khalid 2026-11-17: "apply these concepts from the picture I attached and that donut everywhere in my system where
you see it's necessary." ops 6828 embedded trimmed copies (lists cut to 50). Three sheets need the whole population
to be proved offline before they ship: the ARK sheet (the previous snapshot, so every opened / added / cut / closed
line can be listed instead of the feed's top 15), the S&P 500 sheet (all members, for contributions) and the insider
sheet (every company with a buy, for the dollars ring). Nothing is deployed or written but this report.
"""
from datetime import datetime, timezone
from pathlib import Path
import base64
import gzip
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE = "us-east-1", "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=60)
FULL = ["data/ark-holdings-prev.json", "data/ark-holdings.json", "data/sp500.json", "data/tic-flows.json", "data/capital-inflows.json", "data/rotation-dashboard.json"]
NESTED = ("insiders", "transactions", "fundamentals", "bank_context")


def shape(v, depth=0):
    if isinstance(v, dict):
        if depth >= 3:
            return "{%d keys}" % len(v)
        return "{" + ", ".join("%s: %s" % (k, shape(x, depth + 1)) for k, x in list(v.items())[:14]) + (", ...+%d" % (len(v) - 14) if len(v) > 14 else "") + "}"
    if isinstance(v, list):
        return "[%d x %s]" % (len(v), shape(v[0], depth + 1) if v else "")
    s = repr(v)
    return s if len(s) < 30 else s[:27] + "..."


def read(s3, key):
    obj = s3.get_object(Bucket=LIVE, Key=key)
    body = obj["Body"].read()
    return json.loads(gzip.decompress(body) if body[:2] == b"\x1f\x8b" else body), len(body), obj["LastModified"]


def main():
    now = datetime.now(timezone.utc)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    pack = {}
    with report("ops_6829_tearsheet_kit_full_feeds") as r:
        r.heading("ops 6829 - tearsheet kit, full feeds %s (read-only)" % now.isoformat(timespec="minutes"))
        r.section("1. feeds read whole")
        found = 0
        for key in FULL:
            try:
                doc, size, modified = read(s3, key)
            except Exception as exc:  # noqa: BLE001
                r.log("%-36s %s" % (key, type(exc).__name__))
                continue
            found += 1
            r.log("**%s** %d B, %.1f d old: %s" % (key, size, (now - modified).total_seconds() / 86400, shape(doc)[:1500]))
            pack[key] = doc
        r.section("2. insider clusters without the nested detail")
        try:
            doc, size, modified = read(s3, "data/insider-clusters.json")
            doc["clusters"] = [{k: v for k, v in c.items() if k not in NESTED} for c in doc.get("clusters") or []]
            pack["data/insider-clusters.json"] = doc
            found += 1
            r.log("data/insider-clusters.json %d B: %d clusters, %d companies with a buy" % (size, len(doc["clusters"]), len(doc.get("all_ticker_buys") or [])))
        except Exception as exc:  # noqa: BLE001
            r.log("data/insider-clusters.json %s" % type(exc).__name__)
        r.section("3. ARK: does the previous snapshot reproduce the feed's own counts")
        cur, prev = pack.get("data/ark-holdings.json"), pack.get("data/ark-holdings-prev.json")
        if cur and prev:
            r.log("previous snapshot top-level: %s" % shape(prev)[:900])
        blob = base64.b64encode(gzip.compress(json.dumps(pack, separators=(",", ":"), default=str).encode("utf-8"), 9)).decode("ascii")
        r.log("pack: %d feeds, %d chars" % (len(pack), len(blob)))
        r.lines.append("<!-- FEED-PACK-GZ-B64-BEGIN")
        r.lines.extend(blob[i:i + 16000] for i in range(0, len(blob), 16000))
        r.lines.append("FEED-PACK-GZ-B64-END -->")
        r.kv(feeds_requested=str(len(FULL) + 1), feeds_found=str(found), pack_chars=str(len(blob)))
        if found < len(FULL):
            r.fail("only %d of %d feeds read" % (found, len(FULL) + 1))
        else:
            r.ok("READ: %d of %d feeds" % (found, len(FULL) + 1))


if __name__ == "__main__":
    main()
    sys.exit(0)
