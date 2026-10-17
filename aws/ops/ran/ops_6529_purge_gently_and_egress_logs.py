"""ops 6529 - (1) finish deleting the loop's dead audit versions, gently; (2) find who downloads 57 GB/day.

(1) ops 6526 hit S3 SlowDown: 24 concurrent DeleteObjects on one key prefix exceeded the per-prefix request rate
    (~3,500 writes/s), 248 batches exhausted their retries and a listing call failed.  Here: one lister per key,
    4 deleters total, explicit exponential backoff on SlowDown/503 for both DeleteObjects and the listing, the
    newest day-logs first (2026-10-16 and -15 only become lifecycle-eligible on Oct 17/18).  Same safety rules:
    noncurrent only, current version verified intact, versions larger than their current file kept, page-marker
    version carried to the next batch.  Bounded to 55 minutes; lifecycle finishes any remainder.

(2) ops 6527: S3 GetObject egress to the internet averaged 56.7 GB/day ($4.70/day) on normal days, but server
    access logs had nothing for the sample hour.  Read-only diagnosis: which day folders exist under the log
    prefix, the target bucket's policy/ownership/encryption (any of which silently stops delivery), and - from the
    newest hour that exists - egress bytes by requester, client and key prefix.
"""

from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
import concurrent.futures as cf
import json
import random
import re
import sys
import threading
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
PREFIX = "system-events/audit/"
KEY_RE = re.compile(r"^system-events/audit/(\d{4}-\d{2}-\d{2})\.jsonl$")
MIN_DAY = "2026-10-10"
DEADLINE = time.time() + 55 * 60
OUT_JSON = ROOT / "aws" / "ops" / "reports" / "6529.json"
s3 = boto3.client("s3", region_name=REGION,
                  config=Config(retries={"max_attempts": 4, "mode": "standard"}, max_pool_connections=32))
out = {"op": 6529, "ran_at": datetime.now(timezone.utc).isoformat(), "errors": {}}
lock = threading.Lock()


def slowdown(e):
    code = e.response.get("Error", {}).get("Code") if isinstance(e, ClientError) else ""
    return code in ("SlowDown", "503", "ServiceUnavailable", "InternalError", "RequestTimeout")


def with_backoff(fn, *a, **k):
    delay = 1.0
    for attempt in range(10):
        try:
            return fn(*a, **k)
        except ClientError as e:
            if not slowdown(e) or attempt == 9 or time.time() > DEADLINE:
                raise
            time.sleep(delay + random.random())
            delay = min(delay * 2, 30.0)


def delete_batch(objs):
    try:
        r = with_backoff(s3.delete_objects, Bucket=BUCKET, Delete={"Objects": objs, "Quiet": True})
        errs = r.get("Errors", [])
        retry = [{"Key": e["Key"], "VersionId": e["VersionId"]} for e in errs if e.get("Code") in ("SlowDown", "InternalError")]
        if retry:
            time.sleep(5)
            r2 = with_backoff(s3.delete_objects, Bucket=BUCKET, Delete={"Objects": retry, "Quiet": True})
            errs = [e for e in errs if e.get("Code") not in ("SlowDown", "InternalError")] + r2.get("Errors", [])
        return len(objs) - len(errs), len(errs)
    except ClientError as e:
        print("delete batch gave up: %s" % e.response.get("Error", {}).get("Code"))
        return 0, len(objs)


def purge_key(key, current_size, deleter):
    st = {"key": key, "current_bytes": current_size, "listed": 0, "deleted": 0, "deleted_bytes": 0,
          "kept_larger": 0, "errors": 0, "pages": 0, "complete": False, "stopped": None}
    km, vm, first, carry, futures = key, None, True, None, []
    while time.time() < DEADLINE:
        kw = {"Bucket": BUCKET, "Prefix": key, "MaxKeys": 1000}
        if not first:
            kw["KeyMarker"] = km
            if vm:
                kw["VersionIdMarker"] = vm
        try:
            page = with_backoff(s3.list_object_versions, **kw)
        except ClientError as e:
            st["stopped"] = "list %s" % e.response.get("Error", {}).get("Code")
            break
        first = False
        st["pages"] += 1
        batch = [carry] if carry else []
        carry = None
        for v in page.get("Versions", []):
            if v["Key"] != key:
                continue
            st["listed"] += 1
            if v.get("IsLatest"):
                continue
            if v["Size"] > current_size:
                st["kept_larger"] += 1
                continue
            batch.append({"Key": key, "VersionId": v["VersionId"]})
            st["deleted_bytes"] += v["Size"]
        if page.get("IsTruncated") and batch and batch[-1]["VersionId"] == page.get("NextVersionIdMarker"):
            carry = batch.pop()
        for i in range(0, len(batch), 1000):
            futures.append(deleter.submit(delete_batch, batch[i:i + 1000]))
        # back-pressure: never let more than 8 batches queue up per key
        while sum(1 for f in futures if not f.done()) > 8:
            time.sleep(0.2)
        if not page.get("IsTruncated"):
            st["complete"] = True
            break
        km, vm = page.get("NextKeyMarker"), page.get("NextVersionIdMarker")
    if carry:
        futures.append(deleter.submit(delete_batch, [carry]))
    for f in futures:
        ok, err = f.result()
        st["deleted"] += ok
        st["errors"] += err
    if st["errors"]:
        st["deleted_bytes"] = None  # some listed bytes were not deleted; the count of versions is the truth
    return st


def purge(r):
    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=PREFIX):
        for o in page.get("Contents", []):
            m = KEY_RE.match(o["Key"])
            if m and m.group(1) >= MIN_DAY and o["Size"] > 1_000_000:
                keys.append((o["Key"], o["Size"]))
    keys.sort(reverse=True)  # newest day-logs first
    r.log("day-logs in scope (newest first): %s" % [(k.split("/")[-1], round(sz / 1e6, 1)) for k, sz in keys])
    with cf.ThreadPoolExecutor(max_workers=4) as deleter, cf.ThreadPoolExecutor(max_workers=2) as listers:
        results = list(listers.map(lambda ks: purge_key(ks[0], ks[1], deleter), keys))
    out["purge"] = results
    for x in results:
        r.log("%s: listed %d, deleted %d, kept_larger %d, errors %d, pages %d, complete %s%s" % (
            x["key"].split("/")[-1], x["listed"], x["deleted"], x["kept_larger"], x["errors"], x["pages"], x["complete"],
            (", stopped: %s" % x["stopped"]) if x["stopped"] else ""))
    intact = all(s3.head_object(Bucket=BUCKET, Key=k)["ContentLength"] == sz for k, sz in keys)
    out["current_versions_intact"] = intact
    (r.ok if intact else r.fail)("current day-logs intact: %s" % intact)
    tot = sum(x["deleted"] for x in results)
    r.ok("deleted %d noncurrent versions this run (errors %d)" % (tot, sum(x["errors"] for x in results)))
    return intact


LOG_RE = re.compile(r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" "([^"]*)"')


def logs_diagnosis(r):
    lg = s3.get_bucket_logging(Bucket=BUCKET).get("LoggingEnabled") or {}
    tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
    out["logging"] = {"target": tb, "prefix": tp, "format": lg.get("TargetObjectKeyFormat")}
    if not tb:
        r.warn("server access logging is OFF for %s" % BUCKET)
        return
    base = "%s%s/%s/%s/" % (tp, ACCOUNT, REGION, BUCKET)
    days = []
    for month in ("2026/09/", "2026/10/"):
        resp = s3.list_objects_v2(Bucket=tb, Prefix=base + month, Delimiter="/")
        days += [p["Prefix"][len(base):].rstrip("/") for p in resp.get("CommonPrefixes", [])]
    out["log_days"] = days
    r.log("log day folders present: %s" % days)
    diag = {}
    for name, call in (("policy", lambda: json.loads(s3.get_bucket_policy(Bucket=tb)["Policy"])),
                       ("ownership", lambda: s3.get_bucket_ownership_controls(Bucket=tb)["OwnershipControls"]),
                       ("encryption", lambda: s3.get_bucket_encryption(Bucket=tb)["ServerSideEncryptionConfiguration"]),
                       ("object_lock", lambda: s3.get_object_lock_configuration(Bucket=tb).get("ObjectLockConfiguration")),
                       ("lifecycle", lambda: s3.get_bucket_lifecycle_configuration(Bucket=tb).get("Rules"))):
        try:
            diag[name] = call()
        except ClientError as e:
            diag[name] = "ERR:" + e.response["Error"]["Code"]
    out["log_bucket"] = diag
    pol = diag.get("policy")
    allows = isinstance(pol, dict) and any(
        "logging.s3.amazonaws.com" in json.dumps(st.get("Principal")) and st.get("Effect") == "Allow"
        for st in pol.get("Statement", []))
    out["policy_allows_log_delivery"] = allows
    r.log("target bucket policy allows logging.s3.amazonaws.com: %s; encryption %s; ownership %s" % (
        allows, json.dumps(diag.get("encryption"))[:160], json.dumps(diag.get("ownership"))[:120]))
    if not days:
        return
    newest = days[-1]
    keys, token = [], None
    while True:
        kw = {"Bucket": tb, "Prefix": base + newest + "/", "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        keys += [(o["Key"], o["LastModified"]) for o in resp.get("Contents", [])]
        if not resp.get("IsTruncated") or len(keys) > 200000:
            break
        token = resp.get("NextContinuationToken")
    out["newest_log_day"] = {"day": newest, "objects": len(keys),
                             "last_delivered": max(k[1] for k in keys).isoformat() if keys else None}
    r.log("newest log day %s: %d objects, last delivered %s" % (newest, len(keys), out["newest_log_day"]["last_delivered"]))
    if not keys:
        return
    hour_tag = sorted(k[0] for k in keys)[-1].split("/")[-1][:13]  # YYYY-MM-DD-HH of the newest hour
    sample = [k[0] for k in keys if k[0].split("/")[-1].startswith(hour_tag)][:8000]

    def read(k):
        try:
            return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            return ""

    by_req, by_client, by_prefix, by_ip, n_get, ops = Counter(), Counter(), Counter(), Counter(), Counter(), Counter()
    with cf.ThreadPoolExecutor(max_workers=32) as ex:
        for text in ex.map(read, sample):
            for line in text.splitlines():
                m = LOG_RE.match(line)
                if not m:
                    continue
                ops[m.group(7)] += 1
                if m.group(7) != "REST.GET.OBJECT":
                    continue
                b = int(m.group(12)) if m.group(12).isdigit() else 0
                req = m.group(5)
                who = req.split("/")[-1] if "assumed-role" in req else ("anonymous" if req == "-" else req.split(":")[-1])
                key = m.group(8)
                by_req[who] += b
                by_client[(m.group(17) or "-").split("/")[0][:40]] += b
                by_prefix["/".join(key.split("/")[:3])] += b
                by_ip[".".join(m.group(4).split(".")[:2]) + ".*"] += b
                n_get[who] += 1
    gb = lambda c: [(k, round(v / 1e9, 3)) for k, v in c.most_common(10)]  # noqa: E731
    out["egress_sample"] = {"hour": hour_tag, "log_objects": len(sample), "ops": ops.most_common(10),
                            "gb_by_requester": gb(by_req), "gets_by_requester": n_get.most_common(8),
                            "gb_by_client": gb(by_client), "gb_by_prefix": gb(by_prefix), "gb_by_ip": gb(by_ip)}
    r.log("egress sample hour %s (%d log objects): GB by requester %s" % (hour_tag, len(sample), gb(by_req)[:5]))
    r.log("  GB by client: %s" % gb(by_client)[:5])
    r.log("  GB by key prefix: %s" % gb(by_prefix)[:8])
    r.log("  GB by IP /16: %s" % gb(by_ip)[:6])


def main():
    with report("ops_6529_purge_gently_and_egress_logs") as r:
        r.heading("ops 6529 - gentle purge + egress attribution")
        ok = True
        r.section("Egress / access logs (read-only)")
        try:
            logs_diagnosis(r)
        except Exception as e:  # noqa: BLE001
            out["errors"]["logs"] = "%s: %s" % (type(e).__name__, str(e)[:300])
            r.warn("logs diagnosis failed: %s" % out["errors"]["logs"])
        r.section("Gentle purge")
        try:
            ok = purge(r)
        except Exception as e:  # noqa: BLE001
            out["errors"]["purge"] = "%s: %s" % (type(e).__name__, str(e)[:300])
            r.warn("purge stopped: %s (lifecycle finishes the rest)" % out["errors"]["purge"])
        OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
        r.ok("wrote %s" % OUT_JSON.relative_to(ROOT))
        if not ok:
            r.fail("a current day-log changed size - investigate")
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
