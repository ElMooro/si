"""ops 6610 - READ-ONLY: the daily S3 Inventory of the live bucket (all versions, ops 6569) read end to end - how many
old copies each file still carries, two days after the count rule `ops6541-bound-noncurrent-versions` (keep the 10
newest old copies of every file, expire the rest once a day old; ops 6571, 2026-10-20) went live.

Self-scheduled follow-up for 2026-10-22 15:47 UTC (KHALID_ACTIONS.md 0f): the proof that the 573k storm copies of the
system-events audit key and the >= 200k copies of data/miss-calibrator-proposals.json are down to 10 - or, if S3's
lifecycle has not caught up, the exact inventory of what a one-time manual purge of OLD COPIES ONLY would remove
(proposed to Khalid; nothing is deleted here, nothing is written).

Reads: s3://justhodl-ops-private-857687956942/inventory/<bucket>/<config id>/<stamp>/manifest.json (newest stamp) and
its CSV data files, streamed; counts per key: current object, old copies (IsLatest=false, not a delete marker), delete
markers, bytes; old-copy age (older than 1 day = expirable under the rule); top keys and top prefixes by old copies;
how many keys still carry more than 10 old copies and how many copies that is in total.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import Counter
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
INV_PREFIX = "inventory/"
STORM_KEYS = ["data/miss-calibrator-proposals.json"]
KEEP = 10
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=300, max_pool_connections=20)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def find_manifests(s3):
    """Every manifest.json under the inventory prefix (the layout is inventory/<bucket>/<id>/<stamp>/manifest.json)."""
    out = []
    token = {}
    while True:
        res = s3.list_objects_v2(Bucket=PRIVATE, Prefix=INV_PREFIX, **token)
        for o in res.get("Contents", []):
            if o["Key"].endswith("/manifest.json") and "/hive/" not in o["Key"]:
                out.append((o["Key"], o["LastModified"]))
        if not res.get("IsTruncated"):
            break
        token = {"ContinuationToken": res["NextContinuationToken"]}
    return sorted(out, key=lambda kv: kv[0])


def parse_bool(v):
    return str(v).strip().lower() == "true"


def main():
    s3 = c("s3")
    with report("ops_6610_inventory_old_copies") as r:
        r.heading("ops 6610 - S3 Inventory of the live bucket: old copies per file after the count rule (read-only)")
        mans = find_manifests(s3)
        r.log("inventory manifests found: %d -> %s" % (len(mans), json.dumps([(k, str(t)[:19]) for k, t in mans[-5:]])))
        if not mans:
            r.warn("no inventory delivered yet under s3://%s/%s - S3 promises the first report within 48 h of configuration (ops 6569, 2026-10-20 ~18:30 UTC); re-run later" % (PRIVATE, INV_PREFIX))
            r.kv(inventory="not delivered yet")
            r.ok("read-only; nothing to count yet")
            return
        mkey, mtime = mans[-1]
        man = json.loads(s3.get_object(Bucket=PRIVATE, Key=mkey)["Body"].read())
        schema = [x.strip() for x in (man.get("fileSchema") or "").split(",")]
        files = man.get("files") or []
        stamp = mkey.split("/")[-2]
        r.log("newest manifest %s (delivered %s): source %s, version %s, format %s, %d data files, schema %s" % (
            stamp, str(mtime)[:19], man.get("sourceBucket"), man.get("version"), man.get("fileFormat"), len(files), schema))
        if man.get("fileFormat") != "CSV":
            r.fail("inventory format %s is not CSV - this reader expects CSV" % man.get("fileFormat"))
            raise SystemExit(1)
        idx = {name: i for i, name in enumerate(schema)}
        need = ("Key", "IsLatest", "IsDeleteMarker")
        if any(n not in idx for n in need):
            r.fail("schema lacks %s" % [n for n in need if n not in idx])
            raise SystemExit(1)
        i_key, i_latest, i_dm = idx["Key"], idx["IsLatest"], idx["IsDeleteMarker"]
        i_size, i_mod = idx.get("Size"), idx.get("LastModifiedDate")
        cutoff_1d = datetime.now(timezone.utc) - timedelta(days=1)
        rows = cur = cur_bytes = old = old_bytes = dms = 0
        old_expirable = 0                       # old copies more than a day old (the rule's age condition)
        nc_per_key = Counter()                  # old copies per key
        dm_per_key = Counter()
        old_bytes_prefix, old_count_prefix, cur_count_prefix, cur_bytes_prefix = Counter(), Counter(), Counter(), Counter()
        newest_old, oldest_old = None, None
        for n, f in enumerate(files):
            body = s3.get_object(Bucket=PRIVATE, Key=f["key"])["Body"]
            with gzip.GzipFile(fileobj=body) as gz:
                reader = csv.reader(io.TextIOWrapper(gz, encoding="utf-8", newline=""))
                for row in reader:
                    rows += 1
                    key = urllib.parse.unquote(row[i_key])
                    prefix = key.split("/", 1)[0] + ("/" if "/" in key else "")
                    size = int(row[i_size] or 0) if i_size is not None and row[i_size] else 0
                    if parse_bool(row[i_dm]):
                        dms += 1
                        dm_per_key[key] += 1
                        continue
                    if parse_bool(row[i_latest]):
                        cur += 1
                        cur_bytes += size
                        cur_count_prefix[prefix] += 1
                        cur_bytes_prefix[prefix] += size
                        continue
                    old += 1
                    old_bytes += size
                    nc_per_key[key] += 1
                    old_count_prefix[prefix] += 1
                    old_bytes_prefix[prefix] += size
                    if i_mod is not None and row[i_mod]:
                        try:
                            ts = datetime.strptime(row[i_mod][:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
                            if ts < cutoff_1d:
                                old_expirable += 1
                            newest_old = ts if newest_old is None or ts > newest_old else newest_old
                            oldest_old = ts if oldest_old is None or ts < oldest_old else oldest_old
                        except ValueError:
                            pass
            r.log("file %d/%d read: rows so far %d (current %d, old copies %d, delete markers %d)" % (n + 1, len(files), rows, cur, old, dms))
        over = {k: v for k, v in nc_per_key.items() if v > KEEP}
        excess = sum(v - KEEP for v in over.values())
        r.section("totals (inventory %s)" % stamp)
        r.log("rows %d: current objects %d (%.1f GB), old copies %d (%.1f GB) on %d keys, delete markers %d on %d keys" % (
            rows, cur, cur_bytes / 1024 ** 3, old, old_bytes / 1024 ** 3, len(nc_per_key), dms, len(dm_per_key)))
        r.log("old copies older than 1 day (the rule's age condition): %d; oldest old copy %s, newest %s" % (old_expirable, str(oldest_old)[:19] if oldest_old else None, str(newest_old)[:19] if newest_old else None))
        r.log("keys with more than %d old copies: %d; copies above the cap in total: %d" % (KEEP, len(over), excess))
        r.log("histogram of old copies per key: %s" % json.dumps(sorted(Counter(("1", "2-10", "11-100", "101-1k", "1k-10k", "10k-100k", ">100k")[
            0 if v == 1 else 1 if v <= 10 else 2 if v <= 100 else 3 if v <= 1000 else 4 if v <= 10000 else 5 if v <= 100000 else 6] for v in nc_per_key.values()).items())))
        r.section("the storm keys")
        for k in STORM_KEYS:
            r.log("%s: old copies %d, delete markers %d" % (k, nc_per_key.get(k, 0), dm_per_key.get(k, 0)))
        se = [(k, v) for k, v in nc_per_key.items() if k.startswith("system-events/")]
        se.sort(key=lambda kv: -kv[1])
        r.log("system-events/: %d keys with old copies, %d old copies in total; hottest %s" % (len(se), sum(v for _, v in se), json.dumps(se[:5])))
        r.section("top 25 keys by old copies")
        for k, v in nc_per_key.most_common(25):
            r.log("%7d  %s" % (v, k))
        r.section("top 15 prefixes by old copies")
        for p, v in old_count_prefix.most_common(15):
            r.log("%8d old copies, %7.2f GB | current %7d objects, %7.2f GB | %s" % (v, old_bytes_prefix[p] / 1024 ** 3, cur_count_prefix[p], cur_bytes_prefix[p] / 1024 ** 3, p))
        r.kv(inventory_stamp=stamp, rows=rows, current=cur, current_gb=round(cur_bytes / 1024 ** 3, 2), old_copies=old, old_gb=round(old_bytes / 1024 ** 3, 2),
             old_expirable_1d=old_expirable, delete_markers=dms, keys_over_cap=len(over), copies_over_cap=excess,
             miss_calibrator_old=nc_per_key.get(STORM_KEYS[0], 0), system_events_old=sum(v for _, v in se))
        if excess:
            r.warn("the count rule has not (yet) expired %d old copies above the 10-per-file cap on %d keys - a one-time purge of OLD COPIES ONLY is the proposal for Khalid; nothing deleted here" % (excess, len(over)))
        else:
            r.ok("every key is at or under %d old copies in the %s inventory - the count rule has done its work" % (KEEP, stamp))
        r.ok("read-only; nothing written")


if __name__ == "__main__":
    main()
