"""ops 6643 - READ-ONLY: real engine files written after the archive switched to Intelligent-Tiering (ops 6637, Khalid
2026-10-24 "A") reach the archive as INTELLIGENT_TIERING, not only the canary.

First run (fixed list of 15-minute files) saw none rewritten inside 15 minutes - it is Saturday and the intraday engines
run Monday-Friday. So the files are taken from what the fleet actually wrote: the live bucket's S3 server access logs
since the switch (REST.PUT.OBJECT, 2xx, any writer, canaries excluded). For each written file: HEAD the live copy
(written after the switch, replication COMPLETED) and the archive copy (same size, written no earlier, storage class).
Pass = at least 3 real files proven, none archived in another class. Polls the logs up to 20 minutes. Nothing is written.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import re
import sys
import time

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, ARCHIVE = "justhodl-dashboard-live", "justhodl-warehouse-archive-%s" % ACCOUNT
SWITCHED = datetime(2026, 10, 24, 14, 48, 47, tzinfo=timezone.utc)      # ops 6637 put the rule
NEED, CHECK_MAX = 3, 40
LOG_RE = re.compile(r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) '
                    r'"([^"]*)" "([^"]*)"')


def written_after(text, after):
    """(key, time, writer) for each successful PUT in a log text, written after `after`."""
    out = []
    for line in text.splitlines():
        m = LOG_RE.match(line)
        if not m or m.group(7) != "REST.PUT.OBJECT" or not m.group(10).startswith("2"):
            continue
        try:
            t = datetime.strptime(m.group(3), "%d/%b/%Y:%H:%M:%S %z")
        except ValueError:
            continue
        key = m.group(8)
        if t > after and key != "-" and not key.startswith("_health/"):
            req = m.group(5)
            out.append((key, t, req.split("/")[-1][:40]))
    return out


def selftest():
    line = ('79a5 justhodl-dashboard-live [24/Oct/2026:15:01:02 +0000] 1.2.3.4 arn:aws:sts::857687956942:assumed-role/r/justhodl-risk-regime '
            'ABC REST.PUT.OBJECT data/risk-regime.json "PUT /data/risk-regime.json HTTP/1.1" 200 - - 404076 50 10 "-" "Boto3/1.34"')
    got = written_after(line, SWITCHED)
    assert got and got[0][0] == "data/risk-regime.json" and got[0][2] == "justhodl-risk-regime", got
    assert written_after(line.replace("15:01:02", "14:40:00"), SWITCHED) == []
    assert written_after(line.replace("data/risk-regime.json", "_health/x.json"), SWITCHED) == []
    print("selftest ok")


def main():
    import boto3
    from botocore.config import Config
    from botocore.exceptions import ClientError
    from urllib.parse import unquote
    from ops_report import report

    selftest()
    s3 = boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 8, "mode": "standard"}, max_pool_connections=32))
    with report("ops_6643_archive_it_real_writes") as r:
        r.heading("ops 6643 - real engine files after the switch: archived as Intelligent-Tiering? (read-only)")
        lg = s3.get_bucket_logging(Bucket=LIVE).get("LoggingEnabled") or {}
        tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
        if not tb:
            r.fail("live bucket has no access logging")
            sys.exit(1)
        proven, tried, writers = {}, set(), {}
        deadline = time.time() + 1200
        read_logs = set()
        while time.time() < deadline and len(proven) < NEED:
            now = datetime.now(timezone.utc)
            candidates = []
            for day in sorted({SWITCHED.date(), now.date()}):
                base = "%s%s/%s/%s/%s/" % (tp, ACCOUNT, REGION, LIVE, day.strftime("%Y/%m/%d"))
                tok = None
                while True:
                    kw = {"Bucket": tb, "Prefix": base, "MaxKeys": 1000}
                    if tok:
                        kw["ContinuationToken"] = tok
                    res = s3.list_objects_v2(**kw)
                    for o in res.get("Contents", []):
                        if o["LastModified"] >= SWITCHED and o["Key"] not in read_logs:
                            candidates.append(o["Key"])
                    if not res.get("IsTruncated"):
                        break
                    tok = res.get("NextContinuationToken")
            writes = []
            for lk in candidates[:400]:
                read_logs.add(lk)
                try:
                    text = s3.get_object(Bucket=tb, Key=lk)["Body"].read().decode("utf-8", "replace")
                except ClientError:
                    continue
                writes += written_after(text, SWITCHED + timedelta(seconds=5))
            for key, t, who in sorted(writes, key=lambda x: x[1]):
                key = unquote(key)
                if key in tried or len(tried) >= CHECK_MAX:
                    continue
                tried.add(key)
                writers[key] = who
                try:
                    h = s3.head_object(Bucket=LIVE, Key=key)
                except ClientError:
                    continue
                if h.get("ReplicationStatus") != "COMPLETED":
                    tried.discard(key)                 # look again on the next pass
                    continue
                try:
                    a = s3.head_object(Bucket=ARCHIVE, Key=key)
                except ClientError as ex:
                    r.warn("%s: live COMPLETED but archive HEAD %s" % (key, ex.response["Error"]["Code"]))
                    continue
                if a["ContentLength"] != h["ContentLength"] or a["LastModified"] < h["LastModified"]:
                    tried.discard(key)                 # rewritten again meanwhile; try later
                    continue
                sc = a.get("StorageClass") or "STANDARD"
                proven[key] = sc
                r.log("%s (written %s by %s, %d bytes): archive copy %s, storage class %s" % (
                    key, h["LastModified"].isoformat()[:19], who, h["ContentLength"], a["LastModified"].isoformat()[:19], sc))
            r.log("pass: %d log objects read so far, %d writes after the switch seen, %d files proven" % (len(read_logs), len(writes), len(proven)))
            if len(proven) < NEED:
                time.sleep(90)
        wrong = {k: v for k, v in proven.items() if v != "INTELLIGENT_TIERING"}
        r.kv(proven=len(proven), wrong_class=len(wrong), files_checked=len(tried), switched_at=SWITCHED.isoformat()[:19])
        if wrong:
            r.fail("archived in another class: %s" % wrong)
            sys.exit(1)
        if len(proven) < NEED:
            r.fail("only %d real file(s) proven inside 20 minutes (logs arrive late) - re-run later" % len(proven))
            sys.exit(1)
        r.ok("%d real engine files written after the switch are in the archive as INTELLIGENT_TIERING" % len(proven))
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
