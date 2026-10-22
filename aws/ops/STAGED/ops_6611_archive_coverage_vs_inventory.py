"""ops 6611 - READ-ONLY: is the archive a complete copy?  The live bucket's own S3 Inventory (2026-10-21T01:00Z, ops 6610)
counts 3,381,373 current objects / 1,022.9 GB, while the backfill manifest (ops 6583) was built from a runner listing of
1,282,031 objects and the census (ops 6570) flagged `data/` as truncated.  Here, from the same inventory:
  a. current objects and bytes by top-level prefix and by second-level prefix under data/ (top 30 each);
  b. a systematic sample of current keys (every Nth row, ~3,000 keys) HEADed in the archive bucket -> the share that is
     missing, by prefix;
  c. for the ten biggest data/ sub-prefixes, a bounded object count in the archive vs the inventory count;
  d. the CloudWatch storage metrics of both buckets for the last 4 days (NumberOfObjects, BucketSizeBytes by class) so
     the CloudWatch / census / inventory figures are reconciled on the record.
Nothing is written; no object is copied or deleted.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import Counter
import concurrent.futures as cf
import csv
import gzip
import io
import json
import sys
import urllib.parse

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
ARCHIVE = "justhodl-warehouse-archive-%s" % ACCOUNT
INV_PREFIX = "inventory/"
SAMPLE_EVERY = 1100          # ~3,000 of 3.38 M current keys
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=300, max_pool_connections=40)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def newest_manifest(s3):
    out, token = [], {}
    while True:
        res = s3.list_objects_v2(Bucket=PRIVATE, Prefix=INV_PREFIX, **token)
        out += [o["Key"] for o in res.get("Contents", []) if o["Key"].endswith("/manifest.json") and "/hive/" not in o["Key"]]
        if not res.get("IsTruncated"):
            break
        token = {"ContinuationToken": res["NextContinuationToken"]}
    return sorted(out)[-1] if out else None


def parse_bool(v):
    return str(v).strip().lower() == "true"


def second_level(key):
    parts = key.split("/")
    if len(parts) >= 3:
        return parts[0] + "/" + parts[1] + "/"
    return parts[0] + ("/" if len(parts) == 2 else "")


def metrics(cw, bucket, days=4):
    now = datetime.now(timezone.utc)
    out = {}
    for metric, dims in (("NumberOfObjects", [{"Name": "StorageType", "Value": "AllStorageTypes"}]),
                         ("BucketSizeBytes/StandardStorage", [{"Name": "StorageType", "Value": "StandardStorage"}]),
                         ("BucketSizeBytes/StandardIAStorage", [{"Name": "StorageType", "Value": "StandardIAStorage"}])):
        res = cw.get_metric_statistics(Namespace="AWS/S3", MetricName=metric.split("/")[0], Dimensions=[{"Name": "BucketName", "Value": bucket}] + dims,
                                       StartTime=now - timedelta(days=days + 1), EndTime=now, Period=86400, Statistics=["Average"])
        out[metric] = sorted((p["Timestamp"].strftime("%m-%d"), round(p["Average"] / (1024 ** 3), 1) if "Bytes" in metric else int(p["Average"])) for p in res.get("Datapoints", []))
    return out


def main():
    s3, cw = c("s3"), c("cloudwatch")
    with report("ops_6611_archive_coverage_vs_inventory") as r:
        r.heading("ops 6611 - archive coverage against the live bucket's S3 Inventory (read-only)")
        r.section("d. CloudWatch storage metrics, last days")
        for b in (LIVE, ARCHIVE):
            r.log("%s: %s" % (b, json.dumps(metrics(cw, b))))
        mkey = newest_manifest(s3)
        if not mkey:
            r.fail("no inventory manifest")
            raise SystemExit(1)
        man = json.loads(s3.get_object(Bucket=PRIVATE, Key=mkey)["Body"].read())
        schema = [x.strip() for x in man["fileSchema"].split(",")]
        idx = {n: i for i, n in enumerate(schema)}
        i_key, i_latest, i_dm, i_size = idx["Key"], idx["IsLatest"], idx["IsDeleteMarker"], idx.get("Size")
        stamp = mkey.split("/")[-2]
        r.section("a. current objects by prefix (inventory %s)" % stamp)
        top_n, top_b, sec_n, sec_b = Counter(), Counter(), Counter(), Counter()
        sample, cur = [], 0
        for f in man["files"]:
            body = s3.get_object(Bucket=PRIVATE, Key=f["key"])["Body"]
            with gzip.GzipFile(fileobj=body) as gz:
                for row in csv.reader(io.TextIOWrapper(gz, encoding="utf-8", newline="")):
                    if parse_bool(row[i_dm]) or not parse_bool(row[i_latest]):
                        continue
                    cur += 1
                    key = urllib.parse.unquote(row[i_key])
                    size = int(row[i_size] or 0) if i_size is not None and row[i_size] else 0
                    top = key.split("/", 1)[0] + ("/" if "/" in key else "")
                    top_n[top] += 1
                    top_b[top] += size
                    if top == "data/":
                        s2 = second_level(key)
                        sec_n[s2] += 1
                        sec_b[s2] += size
                    if cur % SAMPLE_EVERY == 0:
                        sample.append(key)
        r.log("current objects %d; top-level prefixes by objects: %s" % (cur, json.dumps([(p, n, round(top_b[p] / 1024 ** 3, 1)) for p, n in top_n.most_common(20)])))
        r.log("data/ second level by objects (top 30): %s" % json.dumps([(p, n, round(sec_b[p] / 1024 ** 3, 1)) for p, n in sec_n.most_common(30)]))
        r.log("data/ second level by GB (top 15): %s" % json.dumps([(p, round(b / 1024 ** 3, 1), sec_n[p]) for p, b in sec_b.most_common(15)]))
        r.section("b. sample of %d current keys HEADed in the archive" % len(sample))

        def head(key):
            try:
                h = s3.head_object(Bucket=ARCHIVE, Key=key)
                return key, True, h.get("StorageClass") or "STANDARD", h.get("ReplicationStatus")
            except Exception as e:  # noqa: BLE001
                code = str(getattr(e, "response", {}).get("Error", {}).get("Code", type(e).__name__))
                return key, False, code, None
        found, missing = Counter(), Counter()
        classes = Counter()
        miss_examples = []
        with cf.ThreadPoolExecutor(max_workers=24) as pool:
            for key, ok, cls, rep in pool.map(head, sample):
                p = second_level(key) if key.startswith("data/") else key.split("/", 1)[0] + ("/" if "/" in key else "")
                if ok:
                    found[p] += 1
                    classes[cls] += 1
                else:
                    missing[p] += 1
                    if len(miss_examples) < 25:
                        miss_examples.append((key, cls))
        tot_f, tot_m = sum(found.values()), sum(missing.values())
        r.log("in the archive: %d of %d sampled (%.1f%%); storage classes %s" % (tot_f, len(sample), 100.0 * tot_f / max(1, len(sample)), json.dumps(classes)))
        by_p = sorted({**{p: (found[p], missing[p]) for p in found}, **{p: (found[p], missing[p]) for p in missing}}.items(), key=lambda kv: -(kv[1][0] + kv[1][1]))
        r.log("by prefix (found, missing): %s" % json.dumps(by_p[:40]))
        r.log("missing examples: %s" % json.dumps(miss_examples))
        r.section("c. bounded archive counts for the ten biggest data/ sub-prefixes")
        for p, n in sec_n.most_common(10):
            cnt, token, pages = 0, {}, 0
            while pages < 60:
                res = s3.list_objects_v2(Bucket=ARCHIVE, Prefix=p, **token)
                pages += 1
                cnt += res.get("KeyCount", 0)
                if not res.get("IsTruncated"):
                    break
                token = {"ContinuationToken": res["NextContinuationToken"]}
            r.log("%-45s inventory %8d | archive %8d%s" % (p, n, cnt, " (listing stopped at 60 pages)" if pages >= 60 and res.get("IsTruncated") else ""))
        r.kv(inventory_stamp=stamp, current_objects=cur, sampled=len(sample), in_archive=tot_f, missing_in_archive=tot_m, missing_pct=round(100.0 * tot_m / max(1, len(sample)), 1))
        if tot_m:
            r.warn("the archive is missing about %.1f%% of the live bucket's current objects (sample) - the ops 6583 manifest came from a truncated listing; a second backfill from the inventory itself is the proposal" % (100.0 * tot_m / max(1, len(sample))))
        else:
            r.ok("every sampled current object is in the archive")
        r.ok("read-only; nothing written, copied or deleted")


if __name__ == "__main__":
    main()
