"""ops 6643 - READ-ONLY: real engine files written after the archive switched to Intelligent-Tiering (ops 6637, Khalid
2026-10-24 "A") reach the archive as INTELLIGENT_TIERING, not only the canary.

For a list of files the fleet rewrites many times a day, HEAD the live copy (last write time, replication status) and,
once it was written after the switch and COMPLETED, HEAD the archive copy: same size, written no earlier than the live
copy, storage class INTELLIGENT_TIERING. Polls up to 15 minutes until 3 real files prove it. Nothing is written.
"""
from datetime import datetime, timezone
from pathlib import Path
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, ARCHIVE = "justhodl-dashboard-live", "justhodl-warehouse-archive-%s" % ACCOUNT
SWITCHED = datetime(2026, 10, 24, 14, 48, 47, tzinfo=timezone.utc)      # ops 6637 put the rule
KEYS = ["data/risk-regime.json", "data/tail-risk.json", "data/implied-prob.json", "data/options-analytics.json",
        "data/market-map.json", "data/sector-groups.json", "data/themes.json", "data/opex-calendar.json", "edge-data.json",
        "data/fx-quote-research.json", "data/polygon-fx-regime.json", "crypto-intel.json", "data/crypto-intel-history.json",
        "_health/dashboard.json", "_health/fleet.json", "data/repo-history.json"]
NEED = 3


def main():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    from ops_report import report

    s3 = boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 8, "mode": "standard"}))
    with report("ops_6643_archive_it_real_writes") as r:
        r.heading("ops 6643 - real engine files after the switch: archived as Intelligent-Tiering? (read-only)")
        proven, seen = {}, {}
        deadline = time.time() + 900
        while time.time() < deadline and len(proven) < NEED:
            for k in KEYS:
                if k in proven:
                    continue
                try:
                    h = s3.head_object(Bucket=LIVE, Key=k)
                except ClientError:
                    continue
                lm, st = h["LastModified"], h.get("ReplicationStatus")
                seen[k] = (lm.isoformat()[:19], st)
                if lm <= SWITCHED or st != "COMPLETED":
                    continue
                try:
                    a = s3.head_object(Bucket=ARCHIVE, Key=k)
                except ClientError as ex:
                    r.warn("%s: live written %s COMPLETED but archive HEAD %s" % (k, lm.isoformat()[:19], ex.response["Error"]["Code"]))
                    continue
                sc = a.get("StorageClass") or "STANDARD"
                if a["ContentLength"] == h["ContentLength"] and a["LastModified"] >= lm:
                    proven[k] = (lm.isoformat()[:19], a["LastModified"].isoformat()[:19], sc, h["ContentLength"])
                    r.log("%s: live %s (%d bytes) -> archive %s, storage class %s" % (k, lm.isoformat()[:19], h["ContentLength"],
                                                                                   a["LastModified"].isoformat()[:19], sc))
            if len(proven) < NEED:
                time.sleep(60)
        wrong = {k: v for k, v in proven.items() if v[2] != "INTELLIGENT_TIERING"}
        r.kv(proven=len(proven), wrong_class=len(wrong), switched_at=SWITCHED.isoformat()[:19])
        if not proven:
            r.log("last seen: %s" % seen)
            r.fail("no real file was rewritten and archived inside 15 minutes - re-run later")
            sys.exit(1)
        if wrong:
            r.fail("archived in the wrong class: %s" % wrong)
            sys.exit(1)
        r.ok("%d real engine files written after the switch are in the archive as INTELLIGENT_TIERING" % len(proven))
    sys.exit(0)


if __name__ == "__main__":
    main()
