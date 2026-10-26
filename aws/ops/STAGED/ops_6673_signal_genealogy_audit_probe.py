"""ops 6673 - read-only: WHY does justhodl-signal-genealogy raise "unreconciled archive cannot produce chronology"?

The engine's public-archive audit (genealogy_public_archive.audit) marks the archive complete only when no object failed
validation, no capture references a record that is missing or differs, and no reference conflicts exist; the engine then
raises before publishing and the reasons never leave the function.  This probe runs the SAME discover() + audit() on the
runner against the public archive (data/research-forecasts/{captures,records}/, read-only) and prints the counts and the
failure reasons with sample keys, so the fix is aimed at the real defect (a corrupt object, a schema drift, a missing
record) instead of guessed.  Nothing is written.
"""
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "shared"))  # instrument_identity (the lane packages aws/shared into the function)
sys.path.insert(0, str(ROOT / "aws" / "lambdas" / "justhodl-signal-genealogy" / "source"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"


def main():
    import genealogy_public_archive as archive
    import genealogy_native_runtime as runtime
    client = boto3.client("s3", region_name=REGION, config=Config(max_pool_connections=8, connect_timeout=10, read_timeout=30, retries={"mode": "standard", "total_max_attempts": 2}))
    with report("ops_6673_signal_genealogy_audit_probe") as r:
        r.heading("ops 6673 - signal-genealogy: the archive audit on the runner (read-only)")
        cutoff = datetime.now(timezone.utc).isoformat()
        t0 = time.time()
        inventories, rows = runtime.discover(client, cutoff)
        r.log("discover: %d original objects, %d bytes, prefixes %s in %.1fs" % (len(rows), sum(x["bytes"] for x in rows), {p: i["objects_at_cutoff"] for p, i in inventories.items()}, time.time() - t0))
        t1 = time.time()
        a = archive.audit(client, inventories, workers=8)
        r.log("audit in %.1fs: listed %s, validated captures %s, records %s, original bytes %s, complete %s" % (time.time() - t1, a["listed_objects"], a["validated_captures"], a["validated_records"], a["validated_original_bytes"], a["record_body_and_storage_checks_complete"]))
        r.section("failures")
        fails = a.get("failures") or []
        r.log("failures: %d; reasons %s" % (len(fails), dict(Counter(f["reason"] for f in fails))))
        for f in fails[:25]:
            r.log("   %s -> %s" % (f["key"], f["reason"]))
        r.section("reference errors / conflicts / unreferenced")
        re_ = a.get("reference_errors") or []
        r.log("reference_errors: %d; reasons %s" % (len(re_), dict(Counter(x["reason"] for x in re_))))
        for x in re_[:15]:
            r.log("   %s" % json.dumps(x))
        rc = a.get("reference_conflicts") or []
        r.log("reference_conflicts: %d" % len(rc))
        for x in rc[:10]:
            r.log("   %s" % json.dumps(x))
        r.log("unreferenced records: %d; capture scans complete: %s; registrations by day (last 7): %s" % (len(a.get("unreferenced_record_ids") or []), a.get("capture_scans_complete"), dict(list((a.get("registration_counts_by_utc_day") or {}).items())[-7:])))
        r.section("verdict")
        if a["record_body_and_storage_checks_complete"]:
            r.log("the archive audits clean on the runner - the Lambda's failure is elsewhere (budget guard / listing); read its last log lines")
        else:
            r.warn("the archive does NOT audit clean: %d failures, %d reference errors, %d conflicts (above) - the engine refuses to publish by contract until these are repaired" % (len(fails), len(re_), len(rc)))
        r.ok("ops 6673 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
