"""ops 6725 - justhodl-13f-book second light (v1.0.1): one invoke, read back the engines/observability block.

v1.0.1 (deployed 19:53 UTC) fixed the smart-money-cluster output key (data/smart-money-clusters.json) and made the
tiers cadence-aware (a weekly engine 8 days old is fresh, a DISABLED trigger is 'unscheduled'); the live book was
still v1.0.0 from first light. Invokes only the book (a reader). Read-only otherwise.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, LIVE, FN, OUT = "us-east-1", "justhodl-dashboard-live", "justhodl-13f-book", "data/13f-book.json"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)


def main():
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    with report("ops_6725_13f_book_second_light") as r:
        r.heading("ops 6725 - justhodl-13f-book second light (v1.0.1) %s" % datetime.now(timezone.utc).isoformat(timespec="minutes"))
        before = s3.head_object(Bucket=LIVE, Key=OUT)["LastModified"]
        r.log("before: %s" % before.isoformat(timespec="seconds"))
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        t_end = time.time() + 5 * 60
        mod = before
        while time.time() < t_end:
            time.sleep(10)
            mod = s3.head_object(Bucket=LIVE, Key=OUT)["LastModified"]
            if mod != before:
                break
        if mod == before:
            r.fail("no new book within 5 min")
            raise SystemExit(1)
        book = json.loads(s3.get_object(Bucket=LIVE, Key=OUT)["Body"].read())
        r.log("book version %s generated_at %s run %ss errors %s" % (book.get("version"), book.get("generated_at"), book.get("run_seconds"), (book.get("book") or {}).get("errors")))
        src = book.get("source") or {}
        r.log("source_originals %s | holdings_research %s" % (src.get("source_originals"), json.dumps(src.get("holdings_research"))[:300]))
        for e in book.get("engines") or []:
            r.log("engine %s: tier %s newest %s h expected %s | %s" % (e["engine"], e.get("tier"), e.get("newest_age_h"), e.get("expected_h"),
                                                                     "; ".join("%s %s" % (o["key"].split("/")[-1], o.get("tier")) for o in e.get("objects") or [])))
        for o in book.get("state_objects") or []:
            r.log("state %s: %s (%s h)" % (o["key"], o.get("tier"), o.get("age_h")))
        bad = [e["engine"] for e in book.get("engines") or [] if e.get("tier") == "missing" and e["engine"] != FN]
        if book.get("version") != "1.0.1" or bad:
            r.fail("version %s / missing outputs %s" % (book.get("version"), bad))
            raise SystemExit(1)
        r.ok("v1.0.1 live; every 13F-family output found")


if __name__ == "__main__":
    main()
    sys.exit(0)
