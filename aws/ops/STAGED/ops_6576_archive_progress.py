"""ops 6576 - READ-ONLY: progress of the archive backfill (S3 Batch Replication job), object count in the archive vs
the live bucket's current objects (census: 1,281,312), the canary, and the old-copy cleanup under way on the live bucket
(a bounded recount of the two storm keys' versions).  Re-run until the job is Complete and the counts agree."""
from pathlib import Path
import concurrent.futures as cf
import json
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
ARCHIVE = "justhodl-warehouse-archive-%s" % ACCOUNT
DESC = "ops 6575 warehouse archive backfill (%s -> %s)" % (LIVE, ARCHIVE)
LIVE_CURRENT = 1281312
STORM_KEYS = ["data/miss-calibrator-proposals.json"]
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=40)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def job_status(s3c):
    for status in (["Active", "Preparing", "Ready", "Suspended", "New", "Completing"], ["Complete", "Failed", "Cancelled"]):
        for j in s3c.list_jobs(AccountId=ACCOUNT, JobStatuses=status).get("Jobs", []):
            if (j.get("Description") or "") == DESC:
                d = s3c.describe_job(AccountId=ACCOUNT, JobId=j["JobId"])["Job"]
                p = d.get("ProgressSummary") or {}
                return {"job": j["JobId"], "status": d.get("Status"), "total": p.get("TotalNumberOfTasks"), "succeeded": p.get("NumberOfTasksSucceeded"),
                        "failed": p.get("NumberOfTasksFailed"), "failure": [(f.get("FailureCode"), (f.get("FailureReason") or "")[:160]) for f in (d.get("FailureReasons") or [])]}
    return {"job": None}


def count_archive(s3, budget_s=900):
    """Count current objects in the archive by prefix in parallel (LIST, ~$0.005/1k calls)."""
    res = s3.list_objects_v2(Bucket=ARCHIVE, Delimiter="/")
    prefixes = [cp["Prefix"] for cp in res.get("CommonPrefixes", [])]
    root = len(res.get("Contents", []))
    if "data/" in prefixes:
        prefixes.remove("data/")
        r2 = s3.list_objects_v2(Bucket=ARCHIVE, Prefix="data/", Delimiter="/")
        prefixes += [cp["Prefix"] for cp in r2.get("CommonPrefixes", [])]
        root += len(r2.get("Contents", []))
    deadline = time.time() + budget_s

    def one(p):
        n, b, token = 0, 0, None
        cli = boto3.client("s3", region_name=REGION, config=CFG)
        while True:
            kw = {"Bucket": ARCHIVE, "Prefix": p, "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            rr = cli.list_objects_v2(**kw)
            n += rr.get("KeyCount", 0)
            b += sum(o.get("Size", 0) for o in rr.get("Contents", []))
            token = rr.get("NextContinuationToken")
            if not token or time.time() > deadline:
                return p, n, b, bool(token)
    total, bytes_, partial = root, 0, []
    with cf.ThreadPoolExecutor(max_workers=24) as ex:
        for p, n, b, trunc in ex.map(one, prefixes):
            total += n
            bytes_ += b
            if trunc:
                partial.append(p)
    return total, bytes_, partial


def storm_versions(s3):
    out = {}
    for key in STORM_KEYS:
        n, token, calls = 0, {}, 0
        while calls < 200:
            res = s3.list_object_versions(Bucket=LIVE, Prefix=key, MaxKeys=1000, **token)
            calls += 1
            n += sum(1 for v in res.get("Versions", []) if v["Key"] == key and not v.get("IsLatest"))
            if not res.get("IsTruncated"):
                break
            nk, nv = res.get("NextKeyMarker"), res.get("NextVersionIdMarker")
            if not nv or (token.get("KeyMarker") == nk and token.get("VersionIdMarker") == nv):
                break          # pagination not advancing (the trap seen in ops 6569/6570): stop, report what was counted
            token = {"KeyMarker": nk, "VersionIdMarker": nv}
        out[key] = {"old_copies": n, "calls": calls}
    # the audit key under system-events/ - find it (the one key with versions) without listing all 573k copies
    res = s3.list_object_versions(Bucket=LIVE, Prefix="system-events/", MaxKeys=1000)
    keys = {}
    for v in res.get("Versions", []):
        keys[v["Key"]] = keys.get(v["Key"], 0) + (0 if v.get("IsLatest") else 1)
    hot = max(keys.items(), key=lambda kv: kv[1]) if keys else (None, 0)
    out["system-events/ (first page)"] = {"hottest_key_old_copies_on_page": hot[1], "truncated": bool(res.get("IsTruncated"))}
    return out


def main():
    s3, s3c = c("s3"), c("s3control")
    with report("ops_6576_archive_progress") as r:
        r.heading("ops 6576 - archive backfill progress, counts, storm-copy cleanup (read-only)")
        js = job_status(s3c)
        r.log("backfill job: %s" % json.dumps(js))
        total, bytes_, partial = count_archive(s3)
        r.log("archive: %d current objects, %.1f GB%s; live bucket current objects (census): %d -> %.1f%% backfilled" % (
            total, bytes_ / 1024 ** 3, " (partial: %s)" % partial if partial else "", LIVE_CURRENT, 100.0 * total / LIVE_CURRENT))
        try:
            h = s3.head_object(Bucket=ARCHIVE, Key="_health/replication-canary.json")
            r.log("canary in the archive: %s, %s" % (h.get("ReplicationStatus") or "REPLICA", h.get("StorageClass")))
        except Exception as ex:  # noqa: BLE001
            r.warn("canary missing from the archive: %s" % type(ex).__name__)
        sv = storm_versions(s3)
        r.log("old copies on the storm keys (count rule keeps 10): %s" % json.dumps(sv))
        r.kv(job_status=js.get("status"), job_succeeded=js.get("succeeded"), job_failed=js.get("failed"), archive_objects=total,
             archive_gb=round(bytes_ / 1024 ** 3, 1), backfilled_pct=round(100.0 * total / LIVE_CURRENT, 1))
        r.ok("read")


if __name__ == "__main__":
    main()
