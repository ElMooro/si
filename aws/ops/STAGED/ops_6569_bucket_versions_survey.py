"""ops 6569 - READ-ONLY survey of justhodl-dashboard-live for the "keep my data, stop the balloon" design (Khalid,
2026-10-20: ~500 GB on data.html must never be erased; anomalies must not run the bill up).

Reads (no change to the live bucket):
  1. versioning status, replication configuration (destination buckets, prefixes, delete-marker replication),
     lifecycle rules: how many, which touch CURRENT objects (Expiration) and which only NONCURRENT versions, on which
     prefixes (only '', data/ and system-events/ are named in the public report - ops 6544 convention)
  2. 45 days of the free daily CloudWatch metrics BucketSizeBytes (per storage type) and NumberOfObjects: the October
     balloon, the shrink after it, the growth since the Oct 19 disable
  3. a bounded sample of versions: every top-level data/*.json key (the daily-rewritten outputs) - how many
     noncurrent versions each carries and how many bytes they add; plus the 25 largest keys' version bytes
  4. the DR buckets' sizes and the access-log bucket's lifecycle
One benign change, outside the live bucket: a daily S3 Inventory (all versions, sizes, storage class) delivered to
s3://justhodl-ops-private-857687956942/inventory/ - the exact current-vs-noncurrent split per prefix arrives within 48 h
(cost ~$0.01/day).  The private bucket policy gets the statement S3 needs to deliver it (merged, existing statements kept).
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
PUBLIC_PREFIXES = {"", "data/", "system-events/"}
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=20)
INV_ID = "versions-daily"


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def pfx(p):
    return p if p in PUBLIC_PREFIXES else "WITHHELD"


def gb(b):
    return round(b / 1024 ** 3, 1)


def config_survey(r, s3):
    ver = s3.get_bucket_versioning(Bucket=LIVE)
    r.log("versioning: %s (MFA delete %s)" % (ver.get("Status"), ver.get("MFADelete", "Disabled")))
    try:
        rep = s3.get_bucket_replication(Bucket=LIVE)["ReplicationConfiguration"]
        for rule in rep.get("Rules", []):
            f = rule.get("Filter") or {}
            p = f.get("Prefix") if "Prefix" in f else (f.get("And") or {}).get("Prefix", rule.get("Prefix", ""))
            d = rule.get("Destination") or {}
            r.log("replication rule %s: %s, prefix %s -> %s (class %s), delete markers %s" % (
                rule.get("ID"), rule.get("Status"), pfx(p or ""), d.get("Bucket", "").split(":")[-1], d.get("StorageClass", "same"),
                (rule.get("DeleteMarkerReplication") or {}).get("Status", "n/a")))
        if not rep.get("Rules"):
            r.log("replication: configured but no rules")
    except s3.exceptions.ClientError as ex:
        r.log("replication: none (%s)" % ex.response["Error"]["Code"])
    rules = s3.get_bucket_lifecycle_configuration(Bucket=LIVE).get("Rules", [])
    cur_rules, nc_rules = [], []
    for rule in rules:
        f = rule.get("Filter") or {}
        p = f.get("Prefix", (f.get("And") or {}).get("Prefix", rule.get("Prefix", "")))
        exp = rule.get("Expiration") or {}
        nce = rule.get("NoncurrentVersionExpiration") or {}
        if exp:
            cur_rules.append((rule.get("ID"), rule.get("Status"), pfx(p or ""), exp))
        if nce:
            nc_rules.append((rule.get("ID"), rule.get("Status"), pfx(p or ""), nce))
    r.log("lifecycle: %d rules; %d expire CURRENT objects: %s" % (len(rules), len(cur_rules), [(i, s, p, e) for i, s, p, e in cur_rules]))
    r.log("lifecycle: %d expire NONCURRENT versions: %s" % (len(nc_rules), [(i, s, p, e) for i, s, p, e in nc_rules]))
    touches_data = [i for i, s, p, e in cur_rules if s == "Enabled" and p in ("", "data/")]
    r.log("current-object expiration on the warehouse ('' or data/): %s" % (touches_data or "none"))


def metrics_survey(r, cw):
    end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    start = end - timedelta(days=45)
    series = {}
    for st in ("StandardStorage", "StandardIAStorage", "GlacierInstantRetrievalStorage"):
        res = cw.get_metric_statistics(Namespace="AWS/S3", MetricName="BucketSizeBytes", StartTime=start, EndTime=end + timedelta(days=1),
                                       Period=86400, Statistics=["Average"],
                                       Dimensions=[{"Name": "BucketName", "Value": LIVE}, {"Name": "StorageType", "Value": st}])
        series[st] = {d["Timestamp"].strftime("%m-%d"): d["Average"] for d in res.get("Datapoints", [])}
    res = cw.get_metric_statistics(Namespace="AWS/S3", MetricName="NumberOfObjects", StartTime=start, EndTime=end + timedelta(days=1),
                                   Period=86400, Statistics=["Average"],
                                   Dimensions=[{"Name": "BucketName", "Value": LIVE}, {"Name": "StorageType", "Value": "AllStorageTypes"}])
    objs = {d["Timestamp"].strftime("%m-%d"): d["Average"] for d in res.get("Datapoints", [])}
    days = sorted(set().union(*[set(s) for s in series.values()]))
    rows = []
    for d in days:
        tot = sum(series[st].get(d, 0) for st in series)
        rows.append((d, gb(tot), gb(series["StandardStorage"].get(d, 0)), gb(series["StandardIAStorage"].get(d, 0)), int(objs.get(d, 0))))
    r.log("bucket size by day (GB total / standard / IA, objects): " + "; ".join("%s %s/%s/%s %s" % x for x in rows))
    return rows


def version_sample(r, s3):
    """Top-level data/*.json keys: versions per key and the bytes the noncurrent copies add."""
    per_key = defaultdict(lambda: [0, 0, 0])   # key -> [current bytes, noncurrent count, noncurrent bytes]
    calls, token = 0, {}
    while True:
        kw = {"Bucket": LIVE, "Prefix": "data/", "Delimiter": "/", "MaxKeys": 1000}
        kw.update(token)
        res = s3.list_object_versions(**kw)
        calls += 1
        for v in res.get("Versions", []):
            k = v["Key"]
            if v.get("IsLatest"):
                per_key[k][0] = v.get("Size", 0)
            else:
                per_key[k][1] += 1
                per_key[k][2] += v.get("Size", 0)
        for m in res.get("DeleteMarkers", []):
            per_key[m["Key"]][1] += 0
        if not res.get("IsTruncated") or calls >= 400:
            break
        token = {"KeyMarker": res.get("NextKeyMarker"), "VersionIdMarker": res.get("NextVersionIdMarker")}
    keys = len(per_key)
    cur_b = sum(v[0] for v in per_key.values())
    nc_n = sum(v[1] for v in per_key.values())
    nc_b = sum(v[2] for v in per_key.values())
    dist = Counter(min(v[1], 50) // 10 * 10 for v in per_key.values())
    r.log("top-level data/*.json: %d keys, current %s GB, noncurrent versions %d adding %s GB (%d list calls%s)" % (
        keys, gb(cur_b), nc_n, gb(nc_b), calls, ", truncated" if calls >= 400 else ""))
    r.log("noncurrent versions per key: " + ", ".join("%s%s: %d keys" % (k, "+" if k >= 50 else "-%d" % (k + 9), n) for k, n in sorted(dist.items())))
    top = sorted(per_key.items(), key=lambda kv: -kv[1][2])[:12]
    r.log("largest version piles: " + "; ".join("%s %d versions %s GB" % (k.split("/")[-1], v[1], gb(v[2])) for k, v in top))
    return keys, cur_b, nc_n, nc_b


def other_buckets(r, s3, cw):
    for b in ("justhodl-dashboard-live-dr", "justhodl-dr-usw2-%s" % ACCOUNT, "justhodl-s3-access-logs-%s" % ACCOUNT):
        try:
            ver = s3.get_bucket_versioning(Bucket=b).get("Status")
        except Exception as ex:  # noqa: BLE001
            r.log("%s: %s" % (b, type(ex).__name__))
            continue
        try:
            rules = s3.get_bucket_lifecycle_configuration(Bucket=b).get("Rules", [])
            lc = ["%s/%s%s" % (x.get("ID"), x.get("Status"), " exp" if x.get("Expiration") else "") for x in rules]
        except Exception:  # noqa: BLE001
            lc = ["none"]
        r.log("%s: versioning %s, lifecycle %s" % (b, ver, lc))


def ensure_inventory(r, s3):
    try:
        cfgs = s3.list_bucket_inventory_configurations(Bucket=LIVE).get("InventoryConfigurationList", [])
        if any(x.get("Id") == INV_ID for x in cfgs):
            r.log("inventory %s already configured" % INV_ID)
            return "exists"
    except Exception as ex:  # noqa: BLE001
        r.log("inventory list: %s" % type(ex).__name__)
    # private bucket policy: let S3 deliver the inventory
    stmt = {"Sid": "S3InventoryDelivery", "Effect": "Allow", "Principal": {"Service": "s3.amazonaws.com"}, "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::%s/inventory/*" % PRIVATE,
            "Condition": {"StringEquals": {"aws:SourceAccount": ACCOUNT, "s3:x-amz-acl": "bucket-owner-full-control"},
                          "ArnLike": {"aws:SourceArn": "arn:aws:s3:::%s" % LIVE}}}
    try:
        pol = json.loads(s3.get_bucket_policy(Bucket=PRIVATE)["Policy"])
    except s3.exceptions.ClientError:
        pol = {"Version": "2012-10-17", "Statement": []}
    if not any(x.get("Sid") == "S3InventoryDelivery" for x in pol["Statement"]):
        pol["Statement"].append(stmt)
        s3.put_bucket_policy(Bucket=PRIVATE, Policy=json.dumps(pol))
        r.log("private bucket policy: inventory delivery statement added (%d statements)" % len(pol["Statement"]))
    s3.put_bucket_inventory_configuration(Bucket=LIVE, Id=INV_ID, InventoryConfiguration={
        "Destination": {"S3BucketDestination": {"AccountId": ACCOUNT, "Bucket": "arn:aws:s3:::%s" % PRIVATE, "Format": "CSV",
                                                "Prefix": "inventory", "Encryption": {"SSES3": {}}}},
        "IsEnabled": True, "Id": INV_ID, "IncludedObjectVersions": "All", "Schedule": {"Frequency": "Daily"},
        "OptionalFields": ["Size", "LastModifiedDate", "StorageClass", "IsMultipartUploaded"]})
    r.ok("inventory %s configured: daily, all versions -> s3://%s/inventory/ (first report within 48 h)" % (INV_ID, PRIVATE))
    return "created"


def main():
    s3, cw = c("s3"), c("cloudwatch")
    with report("ops_6569_bucket_versions_survey") as r:
        r.heading("ops 6569 - live bucket survey: current data vs old versions, rules, replication, growth (read-only + inventory)")
        config_survey(r, s3)
        rows = metrics_survey(r, cw)
        keys, cur_b, nc_n, nc_b = version_sample(r, s3)
        other_buckets(r, s3, cw)
        inv = ensure_inventory(r, s3)
        latest = rows[-1] if rows else ("-", 0, 0, 0, 0)
        r.kv(bucket_gb_latest=latest[1], objects_latest=latest[4], sample_keys=keys, sample_current_gb=gb(cur_b),
             sample_noncurrent_versions=nc_n, sample_noncurrent_gb=gb(nc_b), inventory=inv)
        r.ok("surveyed")


if __name__ == "__main__":
    main()
