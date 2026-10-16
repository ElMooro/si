"""ops 6526 - delete the event loop's dead audit-log versions NOW instead of waiting for lifecycle.

ops 6520 found justhodl-event-coordinator rewriting system-events/audit/<day>.jsonl (20-33 MB) on every routed
event in a versioned bucket: ~4M noncurrent copies, BucketSizeBytes(Standard) 0.9 TB -> 106 TB by Oct 14 and
still growing until the quarantine (Oct 16 19:44 UTC).  At $0.023/GB-month that is ~$4-6 per HOUR of storage.
The ops6520 lifecycle rule (noncurrent 1 day) only stops billing once S3 evaluates it (up to 24-48 h), so this
op deletes them directly.  DELETE requests are free; listing ~4M versions is ~4k LIST calls (~$0.02).

Safety (the data is pure garbage, but deletions are permanent):
  * only keys system-events/audit/<YYYY-MM-DD>.jsonl dated 2026-10-10 or later
  * only versions with IsLatest == False; the current version of every day-log is never touched
  * a noncurrent version larger than its key's current version (would break the append-only premise: every
    noncurrent copy is a byte-prefix of the current file) is kept and reported
  * delete markers are left to lifecycle
Bounded to 70 minutes; whatever remains is finished by the lifecycle rule.
"""

from datetime import datetime, timezone, timedelta
from pathlib import Path
import concurrent.futures as cf
import json
import re
import sys
import threading
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
PREFIX = "system-events/audit/"
MIN_DAY = "2026-10-10"
KEY_RE = re.compile(r"^system-events/audit/(\d{4}-\d{2}-\d{2})\.jsonl$")
DEADLINE = time.time() + 70 * 60
OUT_JSON = ROOT / "aws" / "ops" / "reports" / "6526.json"
s3 = boto3.client("s3", region_name=REGION,
                  config=Config(retries={"max_attempts": 12, "mode": "standard"}, max_pool_connections=64))
cw = boto3.client("cloudwatch", region_name=REGION)
lock = threading.Lock()


def day_keys():
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        for o in page.get("Contents", []):
            m = KEY_RE.match(o["Key"])
            if m and m.group(1) >= MIN_DAY:
                keys.append((o["Key"], o["Size"]))
    return keys


def purge_key(key, current_size, deleter):
    st = {"key": key, "current_bytes": current_size, "listed": 0, "deleted": 0, "deleted_bytes": 0,
          "kept_larger": 0, "errors": 0, "pages": 0, "complete": False}
    km, vm = key, None
    first = True
    futures = []
    carry = None  # the page's last version is the next VersionIdMarker: delete it only after the next page is listed
    while time.time() < DEADLINE:
        kw = {"Bucket": BUCKET, "Prefix": key, "MaxKeys": 1000}
        if not first:
            kw["KeyMarker"] = km
            if vm:
                kw["VersionIdMarker"] = vm
        first = False
        page = s3.list_object_versions(**kw)
        st["pages"] += 1
        batch = [carry] if carry else []
        carry = None
        for v in page.get("Versions", []):
            if v["Key"] != key:
                continue
            st["listed"] += 1
            if v.get("IsLatest"):
                continue
            if v["Size"] > current_size:
                st["kept_larger"] += 1
                continue
            batch.append({"Key": key, "VersionId": v["VersionId"]})
            st["deleted_bytes"] += v["Size"]
        if page.get("IsTruncated") and batch and batch[-1]["VersionId"] == page.get("NextVersionIdMarker"):
            carry = batch.pop()
        for i in range(0, len(batch), 1000):
            futures.append(deleter.submit(delete_batch, batch[i:i + 1000]))
        if not page.get("IsTruncated"):
            st["complete"] = True
            break
        km, vm = page.get("NextKeyMarker"), page.get("NextVersionIdMarker")
    if carry:
        futures.append(deleter.submit(delete_batch, [carry]))
    for f in futures:
        n_ok, n_err = f.result()
        st["deleted"] += n_ok
        st["errors"] += n_err
    return st


def delete_batch(objs):
    try:
        r = s3.delete_objects(Bucket=BUCKET, Delete={"Objects": objs, "Quiet": True})
        errs = r.get("Errors", [])
        return len(objs) - len(errs), len(errs)
    except Exception as e:  # noqa: BLE001
        print("delete batch failed: %s" % e)
        return 0, len(objs)


def bucket_size_series():
    now = datetime.now(timezone.utc)
    r = cw.get_metric_statistics(Namespace="AWS/S3", MetricName="BucketSizeBytes",
                                 Dimensions=[{"Name": "BucketName", "Value": BUCKET},
                                             {"Name": "StorageType", "Value": "StandardStorage"}],
                                 StartTime=now - timedelta(days=5), EndTime=now, Period=86400, Statistics=["Average"])
    return [(p["Timestamp"].strftime("%m-%d"), round(p["Average"] / 1e12, 2)) for p in
            sorted(r.get("Datapoints", []), key=lambda p: p["Timestamp"])]


def main():
    out = {"op": 6526, "ran_at": datetime.now(timezone.utc).isoformat()}
    with report("ops_6526_purge_audit_versions") as r:
        r.heading("ops 6526 - delete the loop's dead audit-log versions")
        out["bucket_tb_before"] = bucket_size_series()
        r.log("BucketSizeBytes(Standard) TB by day: %s" % out["bucket_tb_before"])
        keys = day_keys()
        r.log("day-logs in scope: %s" % [(k.split("/")[-1], round(sz / 1e6, 1)) for k, sz in keys])
        with cf.ThreadPoolExecutor(max_workers=24) as deleter, cf.ThreadPoolExecutor(max_workers=len(keys) or 1) as listers:
            results = list(listers.map(lambda ks: purge_key(ks[0], ks[1], deleter), keys))
        out["keys"] = results
        tot_del = sum(x["deleted"] for x in results)
        tot_bytes = sum(x["deleted_bytes"] for x in results)
        tot_err = sum(x["errors"] for x in results)
        for x in results:
            r.log("%s: listed %d, deleted %d (%.2f TB), kept_larger %d, errors %d, complete %s" % (
                x["key"].split("/")[-1], x["listed"], x["deleted"], x["deleted_bytes"] / 1e12, x["kept_larger"],
                x["errors"], x["complete"]))
        out["totals"] = {"deleted_versions": tot_del, "deleted_tb": round(tot_bytes / 1e12, 3), "errors": tot_err,
                         "all_complete": all(x["complete"] for x in results)}
        # every current version must still be there, byte-for-byte the size it had
        intact = []
        for k, sz in keys:
            h = s3.head_object(Bucket=BUCKET, Key=k)
            intact.append(h["ContentLength"] == sz)
        out["current_versions_intact"] = all(intact)
        (r.ok if all(intact) else r.fail)("current day-logs intact: %d/%d" % (sum(intact), len(intact)))
        (r.ok if tot_err == 0 else r.warn)("deleted %d noncurrent versions = %.2f TB (errors %d, complete %s)" % (
            tot_del, tot_bytes / 1e12, tot_err, out["totals"]["all_complete"]))
        r.log("storage stopped billing for those bytes now (~$%.2f/hour at $0.023/GB-month)" % (tot_bytes / 1e9 * 0.023 / 730))
        OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
        r.ok("wrote %s" % OUT_JSON.relative_to(ROOT))
        if not all(intact):
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
