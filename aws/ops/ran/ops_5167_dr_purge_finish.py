"""ops_5167 -- finish the DR replica purge started by ops 5166.

ops 5166 (2026-09-03 19:16Z) removed 2,853,122 versions / 773 GB in its
30-minute budget with 0 errors, preserved the six quarantine zips to the
live bucket, right-sized backup/ to 30 days, and turned SnapStart off
(8 snapshotted versions deleted). It reported one failure: the lifecycle
rules it put on justhodl-dashboard-live-dr were "not readable back".
S3 lifecycle configuration is eventually consistent -- a GET right after
a PUT can return the previous document -- so this op re-reads with
patience and re-puts only if the rules are really missing.

Also here:
  * the 45 small prefixes ops 5166 HELD only because they had fewer
    than 8 sampled objects -- every one of them was 100% REPLICA and
    100% present in the live bucket with its newest write before the
    Aug-26 replication kill. They are purged on that evidence (they
    hold little; this is hygiene so the bucket ends up holding only
    backup/ and quarantine/).
  * a second sweep, 40-minute budget, with CHARACTER SHARDING for flat
    mega-prefixes (data/providers/eurostat/series/ has >1M keys under
    one prefix -- a single list stream caps at ~3-4 pages/s, which is
    what flattened the tail of the first sweep).
  * ledger-backed root-object cleanup and DR2 left alone (no REPLICA
    status on its objects -- it is a hand-made copy, $0.14/month).
  * verification of justhodl-repo / justhodl-census-us over the LAST
    60 MINUTES (the 6-hour window in ops 5166 straddled the fix).
"""
import json
import string
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

LIVE = "justhodl-dashboard-live"
DR = "justhodl-dashboard-live-dr"
KEEP = ("backup/", "quarantine/")
LEDGER_KEY = "data/ops/ops5166-dr-and-snapstart.json"
REPL_KILL_DATE = datetime(2026, 8, 27, tzinfo=timezone.utc)
SWEEP_BUDGET_S = 40 * 60
SHARD_ALPHABET = string.digits + string.ascii_lowercase + string.ascii_uppercase + "-_.~"
CFG = Config(retries={"max_attempts": 8, "mode": "adaptive"}, read_timeout=120, max_pool_connections=64)

s3e = boto3.client("s3", region_name="us-east-1", config=CFG)
s3w = boto3.client("s3", region_name="us-west-2", config=CFG)
cwe = boto3.client("cloudwatch", region_name="us-east-1", config=CFG)
NOW = datetime.now(timezone.utc)
T0 = time.time()
FAILS = []


def head(cli, bucket, key):
    try:
        return cli.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        return {"_missing": True, "_code": str(e.response.get("Error", {}).get("Code"))}
    except Exception as e:
        return {"_missing": True, "_code": str(e)[:60]}


with report("ops_5167_dr_purge_finish") as R:
    R.heading("ops 5167 -- finish the DR replica purge (lifecycle re-check, held prefixes, sharded sweep)")
    try:
        ledger = json.loads(s3e.get_object(Bucket=LIVE, Key=LEDGER_KEY)["Body"].read())
    except Exception as e:
        ledger = {}
        R.warn("ledger read: %s" % str(e)[:100])
    dr = ledger.get("dr", {})
    purged = list(dr.get("replica_prefixes") or [])
    held = list(dr.get("held_prefixes") or [])
    verdicts = (dr.get("gate") or {}).get("prefix_verdicts") or {}
    R.log("ledger: %d prefixes purge-approved, %d held, sweep so far %s" % (len(purged), len(held), dr.get("sweep")))

    # ================================================================ 1
    R.section("1. Held prefixes: re-gate on their own (small) samples")
    newly = []
    for p in held:
        v = verdicts.get(p) or {}
        if v.get("headed", 0) >= 1 and v.get("replica") == v.get("headed") and v.get("in_live") == v.get("headed"):
            try:
                newest = datetime.fromisoformat(str(v.get("newest")).replace("Z", "+00:00"))
            except Exception:
                newest = None
            if newest is not None and newest < REPL_KILL_DATE:
                # one more head to be sure the prefix is untouched since
                r = s3w.list_objects_v2(Bucket=DR, Prefix=p, MaxKeys=5)
                objs = r.get("Contents", [])
                fresh = [o for o in objs if o["LastModified"] >= REPL_KILL_DATE]
                still_rep = all(head(s3w, DR, o["Key"]).get("ReplicationStatus") == "REPLICA" for o in objs)
                if objs and not fresh and still_rep:
                    newly.append(p)
                    continue
        R.warn("   %-24s stays HELD (%s)" % (p, json.dumps(v, default=str)[:100]))
    R.ok("   re-gated for purge on 100%%-REPLICA evidence: %d prefixes %s" % (len(newly), newly))
    all_purge = sorted(set(purged) | set(newly))

    # ================================================================ 2
    R.section("2. Lifecycle on the DR bucket -- patient read, re-put only if missing")
    wanted = {}
    for p in all_purge:
        rid = "ops5166-purge-replica-" + p.strip("/").replace("/", "-")[:40]
        wanted[rid] = {"ID": rid, "Status": "Enabled", "Filter": {"Prefix": p},
                       "Expiration": {"Days": 1}, "NoncurrentVersionExpiration": {"NoncurrentDays": 1}}
    wanted["ops5166-backup-30d"] = {"ID": "ops5166-backup-30d", "Status": "Enabled", "Filter": {"Prefix": "backup/"},
                                    "Expiration": {"Days": 30}, "NoncurrentVersionExpiration": {"NoncurrentDays": 1}}
    wanted["ops5166-expired-markers"] = {"ID": "ops5166-expired-markers", "Status": "Enabled", "Filter": {"Prefix": ""},
                                         "Expiration": {"ExpiredObjectDeleteMarker": True}}
    rules = None
    for attempt in range(4):
        try:
            rules = s3w.get_bucket_lifecycle_configuration(Bucket=DR).get("Rules", [])
        except ClientError as e:
            if "NoSuchLifecycleConfiguration" in str(e):
                rules = []
            else:
                R.warn("   get lifecycle: %s" % str(e)[:100])
                rules = None
        if rules is not None:
            have = {r_.get("ID") for r_ in rules}
            missing = [k for k in wanted if k not in have]
            R.log("   attempt %d: %d rules on the bucket, %d of %d wanted present" % (attempt + 1, len(rules), len(wanted) - len(missing), len(wanted)))
            if not missing:
                break
            if attempt < 2:
                time.sleep(20)
                continue
            try:
                s3w.put_bucket_lifecycle_configuration(Bucket=DR, LifecycleConfiguration={"Rules": rules + [wanted[k] for k in missing]})
                R.ok("   put %d missing rules" % len(missing))
                time.sleep(25)
            except Exception as e:
                FAILS.append("put lifecycle: %s" % str(e)[:160])
                break
    if rules is not None:
        have = {r_.get("ID") for r_ in rules}
        missing = [k for k in wanted if k not in have]
        if missing:
            R.warn("   still not visible after put (propagation lag): %s -- verify tomorrow" % missing[:6])
        else:
            R.ok("   all %d ops5166/5167 lifecycle rules present on %s" % (len(wanted), DR))
        for r_ in rules:
            if not str(r_.get("ID", "")).startswith("ops5166"):
                R.log("   other rule: %s" % json.dumps(r_, default=str)[:160])

    # ================================================================ 3
    R.section("3. Second sweep -- %d-minute budget, character-sharded for flat mega-prefixes" % (SWEEP_BUDGET_S // 60))
    deleted = {"n": 0, "bytes": 0, "errors": 0, "tasks": 0, "shards": 0}
    lock = threading.Lock()
    deadline = time.time() + SWEEP_BUDGET_S

    def bump(**kw):
        with lock:
            for k, v in kw.items():
                deleted[k] += v

    def sweep(prefix, depth, shard_depth, pool, futures):
        kw = {"Bucket": DR, "Prefix": prefix, "Delimiter": "/", "MaxKeys": 1000}
        first = True
        while time.time() < deadline:
            try:
                r = s3w.list_object_versions(**kw)
            except Exception:
                bump(errors=1)
                return
            cps = r.get("CommonPrefixes", [])
            if depth < 8:
                for c in cps:
                    bump(tasks=1)
                    futures.append(pool.submit(sweep, c["Prefix"], depth + 1, 0, pool, futures))
            vers = r.get("Versions", [])
            objs = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in vers]
            objs += [{"Key": d["Key"], "VersionId": d["VersionId"]} for d in r.get("DeleteMarkers", [])]
            if objs:
                try:
                    resp = s3w.delete_objects(Bucket=DR, Delete={"Objects": objs, "Quiet": True})
                    e_ = len(resp.get("Errors", []))
                    bump(n=len(objs) - e_, bytes=sum(v.get("Size", 0) for v in vers), errors=e_)
                except Exception:
                    bump(errors=len(objs))
            if not r.get("IsTruncated"):
                return
            # flat and big: fan out by next character instead of one sequential stream
            if first and not cps and shard_depth < 8:
                for ch in SHARD_ALPHABET:
                    bump(tasks=1, shards=1)
                    futures.append(pool.submit(sweep, prefix + ch, depth, shard_depth + 1, pool, futures))
                return
            first = False
            kw["KeyMarker"] = r.get("NextKeyMarker")
            kw["VersionIdMarker"] = r.get("NextVersionIdMarker")

    futures = []
    with ThreadPoolExecutor(max_workers=24) as pool:
        for p in all_purge:
            bump(tasks=1)
            futures.append(pool.submit(sweep, p, 1, 0, pool, futures))
        # root-level replica objects, every version
        rr = set(dr.get("root_replica") or [])
        if rr:
            try:
                r = s3w.list_object_versions(Bucket=DR, Delimiter="/", MaxKeys=1000)
                objs = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in r.get("Versions", []) if v["Key"] in rr]
                objs += [{"Key": d["Key"], "VersionId": d["VersionId"]} for d in r.get("DeleteMarkers", []) if d["Key"] in rr]
                if objs:
                    s3w.delete_objects(Bucket=DR, Delete={"Objects": objs, "Quiet": True})
                    bump(n=len(objs))
                    R.log("   root-level replica versions deleted: %d" % len(objs))
            except Exception as e:
                R.warn("   root sweep: %s" % str(e)[:100])
        last = time.time()
        while True:
            if not any(not f.done() for f in futures):
                break
            if time.time() - last >= 120:
                last = time.time()
                R.log("   t+%4.0fs  deleted %s versions (%.1f GB)  tasks %d (shards %d)  errors %d"
                      % (time.time() - T0, "{:,}".format(deleted["n"]), deleted["bytes"] / 1e9,
                         deleted["tasks"], deleted["shards"], deleted["errors"]))
            time.sleep(5)
    R.log("   sweep done: %s versions / delete markers removed, %.1f GB, %d errors, %d tasks (%d shards), %.0fs"
          % ("{:,}".format(deleted["n"]), deleted["bytes"] / 1e9, deleted["errors"], deleted["tasks"], deleted["shards"], time.time() - T0))
    if time.time() >= deadline:
        R.warn("   budget reached -- lifecycle finishes the remainder")
    else:
        R.ok("   every purge-approved prefix fully drained inside the budget")
    # what is left at the top level?
    try:
        r = s3w.list_objects_v2(Bucket=DR, Delimiter="/", MaxKeys=1000)
        left = [c["Prefix"] for c in r.get("CommonPrefixes", [])]
        R.log("   top-level prefixes still holding CURRENT objects: %d %s" % (len(left), left[:40]))
    except Exception as e:
        R.warn("   listing: %s" % str(e)[:80])

    # ================================================================ 4
    R.section("4. Verification -- last 60 minutes (the ops-5166 window straddled the fix)")
    for fn in ("justhodl-repo", "justhodl-fundamental-census", "justhodl-census-us", "justhodl-boj-full", "justhodl-ecb-deep"):
        try:
            out = {}
            for m in ("Invocations", "Errors", "Duration"):
                res = cwe.get_metric_statistics(
                    Namespace="AWS/Lambda", MetricName=m,
                    Dimensions=[{"Name": "FunctionName", "Value": fn}],
                    StartTime=NOW - timedelta(minutes=60), EndTime=NOW, Period=3600, Statistics=["Sum"])
                out[m] = sum(p["Sum"] for p in res.get("Datapoints", []))
            R.log("   %-32s invocations %5d  errors %3d  %.2f Lambda-hours" % (fn, out["Invocations"], out["Errors"], out["Duration"] / 3.6e6))
            R.kv(section="verify_1h", function=fn, invocations=int(out["Invocations"]), errors=int(out["Errors"]))
        except Exception as e:
            R.warn("   %s: %s" % (fn, str(e)[:80]))

    ledger.setdefault("ops5167", {})
    ledger["ops5167"] = {"ts": NOW.isoformat(), "held_regated": newly, "sweep2": dict(deleted)}
    try:
        s3e.put_object(Bucket=LIVE, Key=LEDGER_KEY, Body=json.dumps(ledger, indent=1, default=str).encode(),
                       ContentType="application/json")
        R.ok("   ledger updated")
    except Exception as e:
        FAILS.append("ledger: %s" % str(e)[:100])
    if FAILS:
        for f in FAILS:
            R.fail(f)
        sys.exit(1)
    R.ok("ops 5167 complete in %.0fs" % (time.time() - T0))
