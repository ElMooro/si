"""ops_5445 -- dead-lanes: imf-full state >48h. One Event invoke, no HTTP."""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

import boto3

HERE = Path(__file__).resolve()
sys.path.insert(0, str(HERE.parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
FN = "justhodl-imf-full"
PREFIX = "data/warm/imf-full/"


def main():
    with report("ops_5445_imf_full_kick") as R:
        R.heading("ops 5445 imf-full kick")
        s3 = boto3.client("s3", region_name="us-east-1")
        lam = boto3.client("lambda", region_name="us-east-1")
        listed = s3.list_objects_v2(Bucket=B, Prefix=PREFIX, MaxKeys=20)
        keys = listed.get("Contents") or []
        if not keys:
            R.warn("no objects under %s" % PREFIX)
        else:
            newest = max(keys, key=lambda o: o["LastModified"])
            R.ok("before newest=%s lm=%s" % (newest["Key"], newest["LastModified"]))
        cfg = lam.get_function(FunctionName=FN)
        R.ok("lambda last_modified=%s" % cfg["Configuration"].get("LastModified"))
        inv = lam.invoke(
            FunctionName=FN,
            InvocationType="Event",
            Payload=json.dumps({"kicked_by": "ops_5445"}).encode("utf-8"),
        )
        R.ok("invoke status=%s" % inv.get("StatusCode"))
        time.sleep(8)
        listed2 = s3.list_objects_v2(Bucket=B, Prefix=PREFIX, MaxKeys=20)
        keys2 = listed2.get("Contents") or []
        if keys2:
            newest2 = max(keys2, key=lambda o: o["LastModified"])
            R.ok("after 8s newest=%s lm=%s" % (newest2["Key"], newest2["LastModified"]))
        R.ok("Event kick only — full drain is 850s; sentinel will drop STALE when state writes")


if __name__ == "__main__":
    main()
