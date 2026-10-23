"""ops 6637 - archive copies as S3 Intelligent-Tiering instead of Standard-IA (ONLY on Khalid's yes; prepared 2026-10-23).

Same protection: the one whole-bucket rule warehouse-archive-all keeps copying every write of the live bucket to the
sealed archive; delete markers are still not copied; nothing already in the archive changes (those copies stay
Standard-IA). Only the storage class of NEW copies changes. Why it is cheaper for this bucket (ops 6632, from the
inventory): most writes are small files (often a few KB) and many are replaced within days, and Standard-IA bills every
copy as at least 128 KB for at least 30 days, at $0.01 per 1,000 copies; Intelligent-Tiering bills actual size, has no
minimum duration, copies cost $0.005 per 1,000, and files >= 128 KB move by themselves to $0.0125/GB after 30 days and
$0.004/GB after 90 ($0.0025 per 1,000 such files a month to monitor them). Estimated cost of the archive's new copies:
$29.00 -> $16.44 for the first month, $46.79 -> $27.36 averaged over 12 months. No retrieval fee on restore (IA:
$0.01/GB).
  1. read the configuration: exactly one rule, warehouse-archive-all, Enabled, prefix '', delete markers off - or stop;
  2. back it up to the private bucket (backups/replication/);
  3. put the same rule with Destination.StorageClass INTELLIGENT_TIERING; read it back;
  4. canary: write _health/replication-canary-it-<stamp>.json, wait for COMPLETED, HEAD the archive copy - its storage
     class must be INTELLIGENT_TIERING; anything else puts the backup back exactly.
Undo = the same op with STANDARD_IA (or put back the backup).
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
ARCHIVE = "justhodl-warehouse-archive-%s" % ACCOUNT
BASE_ID = "warehouse-archive-all"
TARGET = "INTELLIGENT_TIERING"


def prefix_of(rule):
    f = rule.get("Filter")
    if f is None:
        return rule.get("Prefix")
    return f.get("Prefix", "" if f == {} else None)


def check(cfg):
    rules = cfg.get("Rules", [])
    if len(rules) != 1 or rules[0].get("ID") != BASE_ID:
        return "expected exactly the one rule %s, found %s" % (BASE_ID, [x.get("ID") for x in rules])
    b = rules[0]
    if b.get("Status") != "Enabled" or prefix_of(b) != "" or (b.get("DeleteMarkerReplication") or {}).get("Status") != "Disabled":
        return "rule differs: %s" % json.dumps(b, default=str)
    if b["Destination"].get("Bucket") != "arn:aws:s3:::%s" % ARCHIVE:
        return "destination differs: %s" % b["Destination"]
    return None


def selftest():
    base = {"ID": BASE_ID, "Priority": 1, "Status": "Enabled", "Filter": {"Prefix": ""}, "DeleteMarkerReplication": {"Status": "Disabled"},
            "Destination": {"Bucket": "arn:aws:s3:::%s" % ARCHIVE, "StorageClass": "STANDARD_IA"}}
    assert check({"Rules": [base]}) is None
    assert check({"Rules": [base, dict(base, ID="x")]}) is not None
    assert check({"Rules": [dict(base, Status="Disabled")]}) is not None
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    from ops_report import report

    selftest()
    s3 = boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60))
    run = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    with report("ops_6637_archive_intelligent_tiering") as r:
        r.heading("ops 6637 - archive copies as Intelligent-Tiering (same whole-bucket rule, same seal)")
        current = s3.get_bucket_replication(Bucket=LIVE)["ReplicationConfiguration"]
        err = check(current)
        if err:
            r.fail("refusing: %s" % err)
            sys.exit(1)
        rule = current["Rules"][0]
        was = rule["Destination"].get("StorageClass")
        r.log("now: %s -> %s, storage class %s" % (BASE_ID, rule["Destination"]["Bucket"], was))
        if was == TARGET:
            r.ok("already %s - nothing to do" % TARGET)
            sys.exit(0)
        bkey = "backups/replication/%s-%s.json" % (LIVE, run)
        s3.put_object(Bucket=PRIVATE, Key=bkey, Body=json.dumps(current, default=str, indent=1).encode(), ContentType="application/json",
                      ServerSideEncryption="AES256")
        r.ok("backup: s3://%s/%s" % (PRIVATE, bkey))
        new_rule = json.loads(json.dumps(rule))
        new_rule["Destination"]["StorageClass"] = TARGET
        changed, done = False, False
        try:
            s3.put_bucket_replication(Bucket=LIVE, ReplicationConfiguration={"Role": current["Role"], "Rules": [new_rule]})
            changed = True
            back = s3.get_bucket_replication(Bucket=LIVE)["ReplicationConfiguration"]["Rules"]
            if len(back) != 1 or back[0]["Destination"].get("StorageClass") != TARGET or check({"Rules": back}):
                raise RuntimeError("read-back differs: %s" % back)
            r.ok("rule %s now copies as %s" % (BASE_ID, TARGET))
            time.sleep(30)
            key = "_health/replication-canary-it-%s.json" % run
            s3.put_object(Bucket=LIVE, Key=key, Body=json.dumps({"purpose": "storage-class canary (ops 6637)", "run": run}).encode(),
                          ContentType="application/json", CacheControl="no-store")
            deadline, st = time.time() + 840, None
            while time.time() < deadline:
                st = s3.head_object(Bucket=LIVE, Key=key).get("ReplicationStatus")
                if st in ("COMPLETED", "FAILED"):
                    break
                time.sleep(15)
            sc = None
            if st == "COMPLETED":
                try:
                    sc = s3.head_object(Bucket=ARCHIVE, Key=key).get("StorageClass")
                except ClientError as ex:
                    sc = "ERR:%s" % ex.response["Error"]["Code"]
            r.log("canary %s: source %s, archive storage class %s" % (key, st, sc))
            if st != "COMPLETED" or sc != TARGET:
                raise RuntimeError("canary not archived as %s (source %s, class %s)" % (TARGET, st, sc))
            done = True
            r.kv(rule=BASE_ID, storage_class_before=was, storage_class_now=TARGET, backup=bkey, canary=key)
            r.ok("new archive copies are %s; protection unchanged (every write still copied, nothing deletable but by root)" % TARGET)
        except Exception as ex:  # noqa: BLE001
            r.warn("%s: %s" % (type(ex).__name__, str(ex)[:300]))
        finally:
            if changed and not done:
                try:
                    s3.put_bucket_replication(Bucket=LIVE, ReplicationConfiguration={"Role": current["Role"], "Rules": current["Rules"]})
                    back = s3.get_bucket_replication(Bucket=LIVE)["ReplicationConfiguration"]["Rules"]
                    (r.ok if back[0]["Destination"].get("StorageClass") == was else r.warn)("configuration restored (%s)" % was)
                except Exception as ex2:  # noqa: BLE001
                    r.fail("RESTORE FAILED (%s) - put back s3://%s/%s by hand" % (type(ex2).__name__, PRIVATE, bkey))
        if not done:
            r.fail("storage class unchanged")
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
