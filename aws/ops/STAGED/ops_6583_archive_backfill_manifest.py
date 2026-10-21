"""ops 6583 - archive backfill, take two: S3 Batch Replication from a CURRENT-VERSIONS-ONLY manifest.

ops 6575's job used an S3-generated manifest, which replicates every version (it would have carried the October
loop's 200k+ and 573k old copies into Standard-IA); ops 6577 cancelled it before it moved anything.  This script
lists the live bucket itself and writes a manifest of exactly one row per current object:

  * every prefix is walked with ListObjectVersions (no delimiter: the markers advance), keeping IsLatest rows only;
  * the 751 top-level data/*.json files and the root files are taken from ListObjectsV2 + HeadObject (the delimiter
    walk of a key with hundreds of thousands of versions is the pagination trap seen in ops 6569/6570);
  * rows are Bucket,Key(URL-encoded),VersionId - objects written before versioning was on carry the version id
    "null" and are listed as such (the job's failed-task report says whether S3 accepts them);
  * the CSV goes to s3://justhodl-ops-private-.../batch-replication/manifests/<stamp>.csv and the job is created with
    its ETag (Operation S3ReplicateObject, failed tasks reported, no confirmation step).

Ongoing replication (ops 6574) keeps covering new writes; this job only copies what existed before it.  Cost about
$14 once.  Idempotent: an existing job with this description is reported, not duplicated.
"""
from pathlib import Path
import concurrent.futures as cf
import io
import json
import sys
import time
import urllib.parse

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
ARCHIVE = "justhodl-warehouse-archive-%s" % ACCOUNT
ROLE_ARN = "arn:aws:iam::%s:role/justhodl-s3-replication-role" % ACCOUNT
DESC = "ops 6583 warehouse archive backfill, current versions only (%s -> %s)" % (LIVE, ARCHIVE)
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=40)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def prefixes(s3):
    """Second-level prefixes under data/ plus the other top-level ones; '' and 'data/' are handled by HEAD."""
    out = []
    res = s3.list_objects_v2(Bucket=LIVE, Delimiter="/")
    for cp in res.get("CommonPrefixes", []):
        p = cp["Prefix"]
        if p == "data/":
            r2 = s3.list_objects_v2(Bucket=LIVE, Prefix="data/", Delimiter="/")
            out += [x["Prefix"] for x in r2.get("CommonPrefixes", [])]
        else:
            out.append(p)
    return sorted(set(out))


def walk_current(prefix):
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    rows, token, calls = [], {}, 0
    while True:
        res = s3.list_object_versions(Bucket=LIVE, Prefix=prefix, MaxKeys=1000, **token)
        calls += 1
        rows += [(v["Key"], v.get("VersionId") or "null") for v in res.get("Versions", []) if v.get("IsLatest")]
        if not res.get("IsTruncated"):
            break
        nk, nv = res.get("NextKeyMarker"), res.get("NextVersionIdMarker")
        if not nk or (token.get("KeyMarker") == nk and token.get("VersionIdMarker") == nv):
            raise RuntimeError("pagination stalled under %s after %d calls" % (prefix, calls))
        token = {"KeyMarker": nk, "VersionIdMarker": nv} if nv else {"KeyMarker": nk}
    return prefix, rows, calls


def flat_files(s3, prefix):
    """Top-level files of a prefix (delimiter listing) with their current version ids via HEAD."""
    keys, token = [], None
    while True:
        kw = {"Bucket": LIVE, "Prefix": prefix, "Delimiter": "/", "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        res = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in res.get("Contents", [])]
        token = res.get("NextContinuationToken")
        if not token:
            break

    def head(k):
        cli = boto3.client("s3", region_name=REGION, config=CFG)
        return k, cli.head_object(Bucket=LIVE, Key=k).get("VersionId") or "null"
    with cf.ThreadPoolExecutor(max_workers=16) as ex:
        return list(ex.map(head, keys))


def main():
    s3, s3c = c("s3"), c("s3control")
    with report("ops_6583_archive_backfill_manifest") as r:
        r.heading("ops 6583 - archive backfill from a current-versions-only manifest")
        for status in (["Active", "Preparing", "Ready", "Suspended", "New", "Completing"], ["Complete"]):
            for j in s3c.list_jobs(AccountId=ACCOUNT, JobStatuses=status).get("Jobs", []):
                if (j.get("Description") or "") == DESC:
                    d = s3c.describe_job(AccountId=ACCOUNT, JobId=j["JobId"])["Job"]
                    p = d.get("ProgressSummary") or {}
                    r.kv(job_id=j["JobId"], status=d.get("Status"), total=p.get("TotalNumberOfTasks"), succeeded=p.get("NumberOfTasksSucceeded"), failed=p.get("NumberOfTasksFailed"))
                    r.ok("backfill job already exists: %s (%s)" % (j["JobId"], d.get("Status")))
                    return
        t0 = time.time()
        rows = []
        for pre in ("", "data/"):
            got = flat_files(s3, pre)
            rows += got
            r.log("%s: %d top-level files (HEAD)" % (pre or "(root)", len(got)))
        plist = prefixes(s3)
        calls, stalled = 0, []
        with cf.ThreadPoolExecutor(max_workers=16) as ex:
            futs = {ex.submit(walk_current, p): p for p in plist}
            for f in cf.as_completed(futs):
                try:
                    p, got, n = f.result()
                    rows += got
                    calls += n
                except Exception as ex2:  # noqa: BLE001
                    stalled.append((futs[f], str(ex2)[:120]))
        if stalled:
            r.fail("walk incomplete: %s" % stalled[:5])
            raise SystemExit(1)
        rows = sorted(set(rows))
        null_v = sum(1 for k, v in rows if v == "null")
        r.log("current objects listed: %d (%d with a null version id), %d LIST calls, %d s" % (len(rows), null_v, calls, int(time.time() - t0)))
        buf = io.StringIO()
        for k, v in rows:
            if k == "_health/replication-canary.json":
                continue
            buf.write("%s,%s,%s\n" % (LIVE, urllib.parse.quote(k, safe="/"), v))
        body = buf.getvalue().encode("utf-8")
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        key = "batch-replication/manifests/%s.csv" % stamp
        put = s3.put_object(Bucket=PRIVATE, Key=key, Body=body, ContentType="text/csv", ServerSideEncryption="AES256")
        etag = put["ETag"].strip('"')
        r.log("manifest: s3://%s/%s (%d rows, %.1f MB, etag %s)" % (PRIVATE, key, len(rows), len(body) / 1e6, etag[:12]))
        job_id = s3c.create_job(AccountId=ACCOUNT, ConfirmationRequired=False, Priority=10, RoleArn=ROLE_ARN, Description=DESC,
                                Operation={"S3ReplicateObject": {}},
                                Report={"Bucket": "arn:aws:s3:::%s" % PRIVATE, "Format": "Report_CSV_20180820", "Enabled": True,
                                        "Prefix": "batch-replication/reports", "ReportScope": "FailedTasksOnly"},
                                Manifest={"Spec": {"Format": "S3BatchOperations_CSV_20180820", "Fields": ["Bucket", "Key", "VersionId"]},
                                          "Location": {"ObjectArn": "arn:aws:s3:::%s/%s" % (PRIVATE, key), "ETag": etag}})["JobId"]
        time.sleep(45)
        d = s3c.describe_job(AccountId=ACCOUNT, JobId=job_id)["Job"]
        p = d.get("ProgressSummary") or {}
        r.kv(job_id=job_id, status=d.get("Status"), rows=len(rows), null_versions=null_v, total=p.get("TotalNumberOfTasks"),
             succeeded=p.get("NumberOfTasksSucceeded"), failed=p.get("NumberOfTasksFailed"))
        if d.get("Status") in ("Failed", "Cancelled"):
            r.fail("job %s: %s" % (d.get("Status"), [(f.get("FailureCode"), (f.get("FailureReason") or "")[:200]) for f in d.get("FailureReasons") or []]))
            raise SystemExit(1)
        r.ok("backfill job %s created from %d current objects (%s); ops 6576 reads progress" % (job_id, len(rows), d.get("Status")))


if __name__ == "__main__":
    main()
