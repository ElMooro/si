"""ops 6698 - READ-ONLY: the alarm justhodl-guard-s3-bytes-growth-1d (ops 5250) fired 2026-10-27 20:03 UTC - the live
bucket grew 82.5 GB in one daily datapoint (threshold 80 GB).  Khalid forwarded it.  Which writer put the bytes there,
and is it a one-time fill or the new daily rate?

  A. the free daily S3 storage metrics, 21 days: bytes per storage class, objects, day-over-day deltas;
  B. the S3 server access logs (sampled, scaled to a day) for Oct 24-27: bytes WRITTEN (REST.PUT.OBJECT / COPY /
     multipart, object size) by writer and by folder, plus bytes REMOVED by lifecycle (S3.EXPIRE.OBJECT) and deletes -
     on a versioned bucket every PUT adds its full size (the overwritten copy becomes an old version and stays until the
     lifecycle count rule expires it), so written bytes ~ gross growth;
  C. the lifecycle rules on the bucket (what caps old copies);
  D. for the biggest writers: how many versions their files carry and what one more day of rewrites costs.
Nothing is written but this report.  No key involved.
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import concurrent.futures as cf
import random
import re
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops" / "STAGED"))
from ops_report import report  # noqa: E402
from ops_6678_write_rate_jump import LOG_RE, family, writer  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE = "justhodl-dashboard-live"
DAYS = ["2026/10/24", "2026/10/25", "2026/10/26", "2026/10/27"]
WRITE_OPS = ("REST.PUT.OBJECT", "REST.COPY.OBJECT", "REST.PUT.PART", "REST.COPY.PART")   # parts carry the bytes; the completing POST is not counted again
REMOVE_OPS = ("S3.EXPIRE.OBJECT", "REST.DELETE.OBJECT", "BATCH.DELETE.OBJECT", "S3.DELETE.OBJECT")
SAMPLE = 9000
GB = 1024 ** 3


def num(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return 0


def main():
    from urllib.parse import unquote
    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, max_pool_connections=64)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
    with report("ops_6698_s3_growth_alarm") as r:
        r.heading("ops 6698 - live bucket +82.5 GB in a day (alarm justhodl-guard-s3-bytes-growth-1d): who, and is it recurring? (read-only)")

        r.section("A. daily S3 storage metrics, 21 days")
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=22)
        classes = ["StandardStorage", "IntelligentTieringFAStorage", "IntelligentTieringIAStorage", "IntelligentTieringAIAStorage",
                   "StandardIAStorage", "GlacierInstantRetrievalStorage", "ReducedRedundancyStorage"]
        q = [{"Id": "b%d" % i, "MetricStat": {"Metric": {"Namespace": "AWS/S3", "MetricName": "BucketSizeBytes",
              "Dimensions": [{"Name": "BucketName", "Value": LIVE}, {"Name": "StorageType", "Value": c}]}, "Period": 86400, "Stat": "Average"}}
             for i, c in enumerate(classes)]
        q.append({"Id": "objs", "MetricStat": {"Metric": {"Namespace": "AWS/S3", "MetricName": "NumberOfObjects",
                  "Dimensions": [{"Name": "BucketName", "Value": LIVE}, {"Name": "StorageType", "Value": "AllStorageTypes"}]}, "Period": 86400, "Stat": "Average"}})
        res = cw.get_metric_data(MetricDataQueries=q, StartTime=start, EndTime=end)["MetricDataResults"]
        days = defaultdict(dict)
        for m in res:
            for t, v in zip(m["Timestamps"], m["Values"]):
                d = t.date().isoformat()
                if m["Id"] == "objs":
                    days[d]["objects"] = v
                else:
                    days[d][classes[int(m["Id"][1:])]] = v
        prev = None
        for d in sorted(days):
            row = days[d]
            tot = sum(v for k, v in row.items() if k != "objects")
            line = "%s  total %8.1f GB  objects %10s" % (d, tot / GB, "{:,}".format(int(row.get("objects", 0))))
            if prev is not None:
                line += "  | delta %+7.1f GB  %+10s objects" % ((tot - prev[0]) / GB, "{:,}".format(int(row.get("objects", 0) - prev[1])))
            line += "  | " + ", ".join("%s %.1f" % (k.replace("Storage", ""), v / GB) for k, v in sorted(row.items()) if k != "objects" and v > 0)
            r.log(line)
            prev = (tot, row.get("objects", 0))

        r.section("B. bytes written / removed per day (access logs, sampled and scaled)")
        lg = s3.get_bucket_logging(Bucket=LIVE).get("LoggingEnabled") or {}
        tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
        per = {}
        for day in DAYS:
            base = "%s%s/%s/%s/%s/" % (tp, ACCOUNT, REGION, LIVE, day)
            keys, tok = [], None
            while True:
                kw = {"Bucket": tb, "Prefix": base, "MaxKeys": 1000}
                if tok:
                    kw["ContinuationToken"] = tok
                resp = s3.list_objects_v2(**kw)
                keys += [(o["Key"], o["LastModified"]) for o in resp.get("Contents", [])]
                if not resp.get("IsTruncated") or len(keys) >= 1500000:
                    break
                tok = resp.get("NextContinuationToken")
            if not keys:
                r.warn("%s: no access logs" % day)
                continue
            random.seed(6698)
            sample = random.sample([k for k, _ in keys], min(SAMPLE, len(keys)))
            newest, oldest = max(lm for _, lm in keys), min(lm for _, lm in keys)
            span_h = max(1.0, (newest - oldest).total_seconds() / 3600.0)
            scale = len(keys) / float(len(sample))
            to_day = 24.0 / span_h if span_h < 23 else 1.0

            def read(k):
                try:
                    return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
                except Exception:  # noqa: BLE001
                    return ""
            wb_who, wb_fam, wb_pair, wn_who, rm = Counter(), Counter(), Counter(), Counter(), Counter()
            with cf.ThreadPoolExecutor(max_workers=64) as ex:
                for text in ex.map(read, sample):
                    for line in text.splitlines():
                        m = LOG_RE.match(line)
                        if not m or not m.group(10).startswith("2"):
                            continue
                        op = m.group(7)
                        size = num(m.group(13))
                        if op in WRITE_OPS:
                            fm, w = family(unquote(m.group(8))), writer(m.group(5))
                            wb_who[w] += size
                            wb_fam[fm] += size
                            wb_pair[(w, fm)] += size
                            wn_who[w] += 1
                        elif op in REMOVE_OPS:
                            rm[op] += size
            f = scale
            per[day] = {"span_h": span_h, "to_day": to_day, "who": {k: v * f for k, v in wb_who.items()}, "fam": {k: v * f for k, v in wb_fam.items()},
                        "pair": {k: v * f for k, v in wb_pair.items()}, "n": {k: v * f for k, v in wn_who.items()}, "rm": {k: v * f for k, v in rm.items()},
                        "written": sum(wb_who.values()) * f}
            r.log("%s: %d log objects over %.1f h (sampled %d, x%.1f): written %.1f GB (%.1f GB/day pace), writes %d; removed %s" % (
                day, len(keys), span_h, len(sample), f, per[day]["written"] / GB, per[day]["written"] / GB * to_day,
                sum(per[day]["n"].values()), {k: "%.1f GB" % (v / GB) for k, v in per[day]["rm"].items()}))
        last = [d for d in DAYS if d in per]
        r.section("bytes written by writer (GB, scaled per log-day; the last day is partial)")
        allw = set().union(*[set(per[d]["who"]) for d in last])
        for w in sorted(allw, key=lambda x: -max(per[d]["who"].get(x, 0) for d in last))[:22]:
            r.log("%-44s %s" % (w, " | ".join("%s %6.1f GB (%7d writes)" % (d[5:], per[d]["who"].get(w, 0) / GB, per[d]["n"].get(w, 0)) for d in last)))
        r.section("bytes written by writer x folder, Oct 26 and Oct 27 (top 20)")
        for d in last[-2:]:
            for (w, fm), v in sorted(per[d]["pair"].items(), key=lambda kv: -kv[1])[:20]:
                r.log("%s  %7.2f GB  %-36s %s" % (d[5:], v / GB, w, fm))

        r.section("C. lifecycle rules")
        try:
            for rule in s3.get_bucket_lifecycle_configuration(Bucket=LIVE).get("Rules", []):
                r.log("%s [%s] filter %s | noncurrent %s | expiration %s | abort-mpu %s" % (
                    rule.get("ID"), rule.get("Status"), rule.get("Filter") or rule.get("Prefix"), rule.get("NoncurrentVersionExpiration"),
                    rule.get("Expiration"), rule.get("AbortIncompleteMultipartUpload")))
        except Exception as e:  # noqa: BLE001
            r.warn("lifecycle: %s" % type(e).__name__)

        r.section("D. versions on the biggest writers' files (sampled keys)")
        top = sorted({fm for d in last[-2:] for (w, fm), v in per[d]["pair"].items() if v > 0.5 * GB}, key=lambda fm: -sum(per[d]["fam"].get(fm, 0) for d in last[-2:]))[:8]
        for fm in top:
            prefix = fm.replace("(files)", "").replace("(root files)", "")
            try:
                resp = s3.list_object_versions(Bucket=LIVE, Prefix=prefix, MaxKeys=1000)
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (fm, type(e).__name__))
                continue
            vers = resp.get("Versions", [])
            by_key = defaultdict(list)
            for v in vers:
                by_key[v["Key"]].append(v)
            if not by_key:
                continue
            nv = sorted(len(v) for v in by_key.values())
            cur_bytes = sum(v["Size"] for v in vers if v.get("IsLatest"))
            old_bytes = sum(v["Size"] for v in vers if not v.get("IsLatest"))
            recent = sum(1 for v in vers if v["LastModified"] >= datetime(2026, 10, 26, tzinfo=timezone.utc))
            r.log("%s: %d keys sampled, versions/key median %d max %d, current %.2f GB, old copies %.2f GB, versions written since Oct 26: %d" % (
                fm, len(by_key), nv[len(nv) // 2], nv[-1], cur_bytes / GB, old_bytes / GB, recent))
        r.ok("ops 6698 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
