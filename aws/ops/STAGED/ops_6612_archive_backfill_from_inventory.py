"""ops 6612 - archive backfill #2: the current objects the first backfill never saw, from the live bucket's own S3
Inventory.  DISPATCHED ONLY ON KHALID'S GO (KHALID_ACTIONS.md 0f, 2026-10-22).

Why: ops 6583 built its manifest from a listing whose data/ sub-prefix discovery was one unpaginated
ListObjectsV2(Delimiter="/") page - every data/ sub-prefix after about "data/pre..." (data/providers/ 1.14 M objects
/ 294 GB, data/warm/ 779 k / 207 GB, data/series-cache/, data/statement-research/, data/price-observations/,
data/symdir/, data/research-*, data/raw/, data/vintage-research/, data/xbrl-raw/ ...) was never listed, so job
523b902a copied 1.28 M of the bucket's 3.38 M current objects.  ops 6611 (2026-10-22 16:00 UTC) measured it: 38% of a
3,073-key sample of current objects is in the archive, data/providers/ 1,708 of 1,137,344, data/warm/ 7,201 of 778,619.

What this does:
  1. reads the newest inventory manifest (s3://justhodl-ops-private-.../inventory/<bucket>/versions-daily/<stamp>/)
     and keeps one row per CURRENT object (IsLatest=true, not a delete marker; the canary excluded);
  2. lists the archive bucket (current keys only, paginated, in parallel by first character) and drops every key that
     is already there - replication of an existing key would store a second copy under the archive's deny-delete
     policy;
  3. writes the manifest Bucket,Key(URL-encoded),VersionId ("null" for pre-versioning objects, as ops 6583 did) to
     s3://justhodl-ops-private-.../batch-replication/manifests/<stamp>-inventory.csv and creates ONE S3 Batch
     Replication job (Operation S3ReplicateObject, failed tasks reported, no confirmation step, same role as ops 6583);
  4. idempotent: an existing job with this description is reported, not duplicated.
Cost (list prices): ~2.05 M objects x $0.01 / 1,000 Standard-IA PUTs ~ $21 + Batch Operations $0.25 + $1 / M ~ $2.3,
once; the archive grows from ~439 GB to ~1,023 GB of Standard-IA (~$13 / month instead of ~$6).  ops 6576 (with this
job's description added to its DESCS) reads progress.
"""
from pathlib import Path
import concurrent.futures as cf
import csv
import gzip
import io
import json
import string
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
DESC = "ops 6612 warehouse archive backfill #2 from the S3 Inventory, current versions only (%s -> %s)" % (LIVE, ARCHIVE)
INV_PREFIX = "inventory/"
CANARY = "_health/replication-canary.json"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=300, max_pool_connections=40)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def parse_bool(v):
    return str(v).strip().lower() == "true"


def newest_manifest(s3):
    out, token = [], {}
    while True:
        res = s3.list_objects_v2(Bucket=PRIVATE, Prefix=INV_PREFIX, **token)
        out += [o["Key"] for o in res.get("Contents", []) if o["Key"].endswith("/manifest.json") and "/hive/" not in o["Key"]]
        if not res.get("IsTruncated"):
            break
        token = {"ContinuationToken": res["NextContinuationToken"]}
    return sorted(out)[-1] if out else None


def archive_keys_under(prefix):
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    keys, token, calls = set(), {}, 0
    while True:
        res = s3.list_objects_v2(Bucket=ARCHIVE, Prefix=prefix, MaxKeys=1000, **token)
        calls += 1
        keys.update(o["Key"] for o in res.get("Contents", []))
        if not res.get("IsTruncated"):
            return prefix, keys, calls
        token = {"ContinuationToken": res["NextContinuationToken"]}


def main():
    s3, s3c = c("s3"), c("s3control")
    with report("ops_6612_archive_backfill_from_inventory") as r:
        r.heading("ops 6612 - archive backfill #2 from the S3 Inventory (current versions only, keys not yet in the archive)")
        for status in (["Active", "Preparing", "Ready", "Suspended", "New", "Completing"], ["Complete"]):
            for j in s3c.list_jobs(AccountId=ACCOUNT, JobStatuses=status).get("Jobs", []):
                if (j.get("Description") or "") == DESC:
                    d = s3c.describe_job(AccountId=ACCOUNT, JobId=j["JobId"])["Job"]
                    p = d.get("ProgressSummary") or {}
                    r.kv(job_id=j["JobId"], status=d.get("Status"), total=p.get("TotalNumberOfTasks"), succeeded=p.get("NumberOfTasksSucceeded"), failed=p.get("NumberOfTasksFailed"))
                    r.ok("backfill #2 job already exists: %s (%s)" % (j["JobId"], d.get("Status")))
                    return
        t0 = time.time()
        r.section("1. current objects from the inventory")
        mkey = newest_manifest(s3)
        if not mkey:
            r.fail("no inventory manifest under s3://%s/%s" % (PRIVATE, INV_PREFIX))
            raise SystemExit(1)
        man = json.loads(s3.get_object(Bucket=PRIVATE, Key=mkey)["Body"].read())
        schema = [x.strip() for x in man["fileSchema"].split(",")]
        idx = {n: i for i, n in enumerate(schema)}
        for need in ("Key", "VersionId", "IsLatest", "IsDeleteMarker"):
            if need not in idx:
                r.fail("inventory schema lacks %s: %s" % (need, schema))
                raise SystemExit(1)
        i_key, i_ver, i_latest, i_dm, i_size = idx["Key"], idx["VersionId"], idx["IsLatest"], idx["IsDeleteMarker"], idx.get("Size")
        stamp_inv = mkey.split("/")[-2]
        current = {}
        sizes = {}
        for f in man["files"]:
            body = s3.get_object(Bucket=PRIVATE, Key=f["key"])["Body"]
            with gzip.GzipFile(fileobj=body) as gz:
                for row in csv.reader(io.TextIOWrapper(gz, encoding="utf-8", newline="")):
                    if parse_bool(row[i_dm]) or not parse_bool(row[i_latest]):
                        continue
                    key = urllib.parse.unquote(row[i_key])
                    if key == CANARY:
                        continue
                    current[key] = row[i_ver].strip() or "null"
                    if i_size is not None:
                        sizes[key] = int(row[i_size] or 0) if row[i_size] else 0
        r.log("inventory %s: %d current objects, %.1f GB" % (stamp_inv, len(current), sum(sizes.values()) / 1024 ** 3))
        r.section("2. what the archive already holds")
        # top-level split: list the root once with a delimiter to find prefixes, then walk each in parallel
        res = s3.list_objects_v2(Bucket=ARCHIVE, Delimiter="/")
        tops = [cp["Prefix"] for cp in res.get("CommonPrefixes", [])]
        token = {}
        while res.get("IsTruncated"):
            token = {"ContinuationToken": res["NextContinuationToken"]}
            res = s3.list_objects_v2(Bucket=ARCHIVE, Delimiter="/", **token)
            tops += [cp["Prefix"] for cp in res.get("CommonPrefixes", [])]
        walk = []
        for t in sorted(set(tops)):
            if t == "data/":
                walk += ["data/" + h for h in string.ascii_lowercase + string.digits + "_-.ABCDEFGHIJKLMNOPQRSTUVWXYZ"]
            else:
                walk.append(t)
        in_archive, calls = set(), 0
        with cf.ThreadPoolExecutor(max_workers=16) as ex:
            for p, keys, n in ex.map(archive_keys_under, walk):
                in_archive |= keys
                calls += n
        # root-level files of the archive (no prefix)
        root = s3.list_objects_v2(Bucket=ARCHIVE, Delimiter="/")
        in_archive.update(o["Key"] for o in root.get("Contents", []))
        r.log("archive: %d current keys (%d LIST calls, %d s)" % (len(in_archive), calls, int(time.time() - t0)))
        todo = sorted(k for k in current if k not in in_archive)
        gb = sum(sizes.get(k, 0) for k in todo) / 1024 ** 3
        by_prefix = {}
        for k in todo:
            parts = k.split("/")
            p = parts[0] + "/" + parts[1] + "/" if k.startswith("data/") and len(parts) >= 3 else parts[0] + ("/" if len(parts) > 1 else "")
            by_prefix[p] = by_prefix.get(p, 0) + 1
        r.log("to copy: %d objects, %.1f GB; by prefix (top 20): %s" % (len(todo), gb, json.dumps(sorted(by_prefix.items(), key=lambda kv: -kv[1])[:20])))
        if not todo:
            r.ok("nothing to copy - the archive already holds every current object of the inventory")
            return
        r.section("3. manifest + job")
        buf = io.StringIO()
        null_v = 0
        for k in todo:
            v = current[k]
            null_v += v == "null"
            buf.write("%s,%s,%s\n" % (LIVE, urllib.parse.quote(k, safe="/"), v))
        body = buf.getvalue().encode("utf-8")
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        key = "batch-replication/manifests/%s-inventory.csv" % stamp
        put = s3.put_object(Bucket=PRIVATE, Key=key, Body=body, ContentType="text/csv", ServerSideEncryption="AES256")
        etag = put["ETag"].strip('"')
        r.log("manifest: s3://%s/%s (%d rows, %d null version ids, %.1f MB, etag %s)" % (PRIVATE, key, len(todo), null_v, len(body) / 1e6, etag[:12]))
        job_id = s3c.create_job(AccountId=ACCOUNT, ConfirmationRequired=False, Priority=10, RoleArn=ROLE_ARN, Description=DESC,
                                Operation={"S3ReplicateObject": {}},
                                Report={"Bucket": "arn:aws:s3:::%s" % PRIVATE, "Format": "Report_CSV_20180820", "Enabled": True,
                                        "Prefix": "batch-replication/reports", "ReportScope": "FailedTasksOnly"},
                                Manifest={"Spec": {"Format": "S3BatchOperations_CSV_20180820", "Fields": ["Bucket", "Key", "VersionId"]},
                                          "Location": {"ObjectArn": "arn:aws:s3:::%s/%s" % (PRIVATE, key), "ETag": etag}})["JobId"]
        time.sleep(45)
        d = s3c.describe_job(AccountId=ACCOUNT, JobId=job_id)["Job"]
        p = d.get("ProgressSummary") or {}
        r.kv(job_id=job_id, status=d.get("Status"), rows=len(todo), gb=round(gb, 1), null_versions=null_v, total=p.get("TotalNumberOfTasks"),
             succeeded=p.get("NumberOfTasksSucceeded"), failed=p.get("NumberOfTasksFailed"), inventory=stamp_inv)
        if d.get("Status") in ("Failed", "Cancelled"):
            r.fail("job %s: %s" % (d.get("Status"), [(f.get("FailureCode"), (f.get("FailureReason") or "")[:200]) for f in d.get("FailureReasons") or []]))
            raise SystemExit(1)
        r.ok("backfill #2 job %s created from %d current objects not yet in the archive (%s); ops 6576 reads progress" % (job_id, len(todo), d.get("Status")))


if __name__ == "__main__":
    main()
