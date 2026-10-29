"""ops 6702 - READ-ONLY follow-up to ops 6701: have the lifecycle rules removed the old copies of worldbank-full's progress
file?  Live bucket rule ops6541-bound-noncurrent-versions keeps the 10 newest old copies per file; the sealed archive's
archive-keep-3-old-copies keeps 3 (both NoncurrentDays 1; AWS removes eligible copies asynchronously).  On 2026-10-27
23:42 UTC both buckets still held 56,709 versions / 153.5 GB of data/warm/worldbank-full/_state/state.json (the runner
may not delete versions in either bucket: explicit deny).  Counts versions, bytes and age per bucket for state.json and
inflight.json, plus the drain's phase.  No key involved.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

BUCKETS = {"live": "justhodl-dashboard-live", "archive": "justhodl-warehouse-archive-857687956942"}
KEYS = ["data/warm/worldbank-full/_state/state.json", "data/warm/worldbank-full/_state/inflight.json"]
KEEP = {"live": 10, "archive": 3}


def versions(s3, bucket, key):
    out, tok = [], {}
    while True:
        resp = s3.list_object_versions(Bucket=bucket, Prefix=key, MaxKeys=1000, **tok)
        out += [v for v in resp.get("Versions", []) if v["Key"] == key]
        if not resp.get("IsTruncated"):
            break
        tok = {"KeyMarker": resp["NextKeyMarker"], "VersionIdMarker": resp["NextVersionIdMarker"]}
    return sorted(out, key=lambda v: v["LastModified"], reverse=True)


def main():
    s3 = boto3.client("s3", region_name="us-east-1", config=Config(retries={"max_attempts": 8, "mode": "standard"}))
    now = datetime.now(timezone.utc)
    with report("ops_6702_worldbank_copies_check") as r:
        r.heading("ops 6702 - old copies of worldbank-full's progress file: what the lifecycle rules have removed (read-only)")
        raw = s3.get_object(Bucket=BUCKETS["live"], Key=KEYS[0])["Body"].read()
        st = json.loads(gzip.decompress(raw) if raw[:2] == b"\x1f\x8b" else raw)
        r.log("current state: version %s, phase %s, queue %d, banked %s, as_of %s" % (st.get("version"), st.get("phase"), len(st.get("queue") or []),
                                                                                 st.get("n_banked"), st.get("as_of")))
        done = True
        for name, bucket in BUCKETS.items():
            for key in KEYS:
                vs = versions(s3, bucket, key)
                old = [v for v in vs if not v.get("IsLatest")]
                excess = old[KEEP[name]:]
                eligible = [v for v in excess if (now - v["LastModified"]).total_seconds() > 2 * 86400]
                done = done and not eligible
                r.log("%-7s %s: %d versions, %.2f GB; old copies %d (rule keeps %d); beyond the rule %d (%.2f GB), of which older than 2 days %d" % (
                    name, key.rsplit("/", 1)[-1], len(vs), sum(v["Size"] for v in vs) / 1e9, len(old), KEEP[name], len(excess),
                    sum(v["Size"] for v in excess) / 1e9, len(eligible)))
        (r.ok if done else r.warn)("old copies beyond the rules: %s" % ("removed" if done else "still present - lifecycle has not finished"))
        cw = boto3.client("cloudwatch", region_name="us-east-1")
        for a in cw.describe_alarms(AlarmNames=["justhodl-guard-s3-bytes-growth-1d"]).get("MetricAlarms", []):
            r.log("alarm %s: %s since %s - %s" % (a["AlarmName"], a["StateValue"], a.get("StateUpdatedTimestamp"), (a.get("StateReason") or "")[:200]))
        from datetime import timedelta
        for name, bucket in BUCKETS.items():
            q = [{"Id": "b%d" % i, "MetricStat": {"Metric": {"Namespace": "AWS/S3", "MetricName": "BucketSizeBytes", "Dimensions": [
                {"Name": "BucketName", "Value": bucket}, {"Name": "StorageType", "Value": c}]}, "Period": 86400, "Stat": "Average"}}
                 for i, c in enumerate(["StandardStorage", "StandardIAStorage", "IntelligentTieringFAStorage", "IntelligentTieringIAStorage",
                                        "GlacierInstantRetrievalStorage"])]
            days = {}
            for m in cw.get_metric_data(MetricDataQueries=q, StartTime=now - timedelta(days=7), EndTime=now)["MetricDataResults"]:
                for t, v in zip(m["Timestamps"], m["Values"]):
                    days[t.date().isoformat()] = days.get(t.date().isoformat(), 0) + v
            prev = None
            for d in sorted(days):
                r.log("%-7s %s: %.1f GB%s" % (name, d, days[d] / 1e9, "" if prev is None else " (%+.1f GB)" % ((days[d] - prev) / 1e9)))
                prev = days[d]
        r.ok("ops 6702 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
