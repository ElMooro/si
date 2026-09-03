"""ops_5166 -- Khalid's two calls (2026-09-03): DR mirror and SnapStart.

  1. "delete (as long as it's replication data) -- double check first."
     The us-west-2 bucket justhodl-dashboard-live-dr is NOT purely
     replication data, so it is not simply deleted:
       * data/... (the ~2.7TB) is the CRR replica of the live bucket --
         stale since ops 4988 deleted replication on Aug 26, and full of
         the churn's dead versions. That is what Khalid approved.
       * backup/<date>/... is written DAILY by justhodl-dr-snapshot (all
         Lambda code+config, EventBridge, DDB, IAM) -- the platform's
         rebuild-from-zero insurance. It stays; retention is right-sized
         to 30 days (the engine's own doc says 90 via a lifecycle that was
         never configured).
       * quarantine/2026-08-01/*.zip are the only copies of six killed
         functions (config/quarantine-ledger.json). Copied to the live
         bucket under data/ops/archive/dr-quarantine/ and verified by
         size before anything is expired.
     GATE before any deletion: live bucket has no replication config,
     the DR bucket is not a replication source, sampled replica objects
     carry ReplicationStatus=REPLICA and exist in the live bucket, and
     the newest replica write predates the replication kill.
     PURGE: lifecycle (Expiration 1d + NoncurrentVersionExpiration 1d
     per replica prefix, free, finishes on its own) armed FIRST, then a
     time-boxed parallel delete sweep so the storage line drops today.
     Note: Standard-IA objects younger than 30 days carry the IA
     minimum-duration charge on deletion -- the same money as keeping
     them to day 30, so there is no reason to wait.
     The tiny justhodl-dr-usw2-857687956942 (11GB) gets the same gate.

  2. "no I don't need it" -- SnapStart on justhodl-ai-chat. ApplyOn=None
     stops NEW snapshots; the cached-GB-second line ($17/mo) bills for
     snapshots attached to PUBLISHED VERSIONS until those versions are
     deleted. So: ApplyOn=None, then delete every published version that
     holds a snapshot -- fleet-wide, since ops 4229 turned SnapStart off
     on seven batch functions without deleting their versions -- except
     versions referenced by an alias or a function URL.

  3. Verification read of the ops-5164 stand-down (repo / census-us /
     boj-full last 6h, live-bucket size).
"""
import json
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
DR2 = "justhodl-dr-usw2-857687956942"
KEEP_PREFIXES = ("backup/", "quarantine/")
QUAR_DEST = "data/ops/archive/dr-quarantine/"
LEDGER_KEY = "data/ops/ops5166-dr-and-snapstart.json"
REPL_KILL_DATE = datetime(2026, 8, 27, tzinfo=timezone.utc)
SWEEP_BUDGET_S = 30 * 60
CFG = Config(retries={"max_attempts": 8, "mode": "adaptive"}, read_timeout=120, max_pool_connections=48)

s3e = boto3.client("s3", region_name="us-east-1", config=CFG)
s3w = boto3.client("s3", region_name="us-west-2", config=CFG)
cwe = boto3.client("cloudwatch", region_name="us-east-1", config=CFG)
cww = boto3.client("cloudwatch", region_name="us-west-2", config=CFG)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)

NOW = datetime.now(timezone.utc)
T0 = time.time()
FAILS = []
LEDGER = {"ops": 5166, "ts": NOW.isoformat(), "dr": {}, "snapstart": {}, "verify": {}}


def bucket_size_gb(cw, bucket):
    tot, parts = 0.0, {}
    for stype in ("StandardStorage", "StandardIAStorage", "StandardIASizeOverhead",
                  "GlacierInstantRetrievalStorage", "DeepArchiveStorage"):
        try:
            res = cw.get_metric_statistics(
                Namespace="AWS/S3", MetricName="BucketSizeBytes",
                Dimensions=[{"Name": "BucketName", "Value": bucket}, {"Name": "StorageType", "Value": stype}],
                StartTime=NOW - timedelta(days=4), EndTime=NOW, Period=86400, Statistics=["Average"])
            pts = sorted(res.get("Datapoints", []), key=lambda p: p["Timestamp"])
            if pts:
                parts[stype] = pts[-1]["Average"] / 1e9
                tot += parts[stype]
        except Exception:
            pass
    return tot, parts


def head(cli, bucket, key, version=None):
    try:
        kw = {"Bucket": bucket, "Key": key}
        if version:
            kw["VersionId"] = version
        return cli.head_object(**kw)
    except ClientError as e:
        code = str(e.response.get("Error", {}).get("Code"))
        return {"_missing": True, "_code": code}
    except Exception as e:
        return {"_missing": True, "_code": str(e)[:60]}


def prefix_bytes(cli, bucket, prefix, max_pages=400):
    """Exact-ish bytes/counts for a prefix (current + noncurrent), capped."""
    cur_n = cur_b = non_n = non_b = dm = 0
    newest = None
    kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
    pages = 0
    truncated = False
    while True:
        r = cli.list_object_versions(**kw)
        pages += 1
        for v in r.get("Versions", []):
            if v.get("IsLatest"):
                cur_n += 1
                cur_b += v.get("Size", 0)
            else:
                non_n += 1
                non_b += v.get("Size", 0)
            lm = v.get("LastModified")
            if lm and (newest is None or lm > newest):
                newest = lm
        dm += len(r.get("DeleteMarkers", []))
        if not r.get("IsTruncated"):
            break
        if pages >= max_pages:
            truncated = True
            break
        kw["KeyMarker"] = r.get("NextKeyMarker")
        kw["VersionIdMarker"] = r.get("NextVersionIdMarker")
    return {"current": cur_n, "current_gb": cur_b / 1e9, "noncurrent": non_n,
            "noncurrent_gb": non_b / 1e9, "delete_markers": dm, "newest": newest,
            "pages": pages, "truncated": truncated}


with report("ops_5166_dr_purge_and_snapstart") as R:
    R.heading("ops 5166 -- DR mirror purge (evidence-gated) + SnapStart off")

    # ================================================================ A
    R.section("A. DR mirror census and evidence gate")
    gate = {}
    try:
        s3e.get_bucket_replication(Bucket=LIVE)
        gate["live_replication"] = "PRESENT"
        R.fail("   live bucket still has a replication configuration -- gate FAILS")
    except ClientError as e:
        if "ReplicationConfigurationNotFound" in str(e):
            gate["live_replication"] = "none"
            R.ok("   live bucket: no replication configuration (ops 4988 held)")
        else:
            gate["live_replication"] = "error:" + str(e)[:60]
            R.warn("   live replication read: %s" % str(e)[:100])
    for b in (DR, DR2):
        try:
            s3w.get_bucket_replication(Bucket=b)
            gate[b + ":source"] = "PRESENT"
            R.fail("   %s is itself a replication SOURCE -- gate FAILS for it" % b)
        except ClientError as e:
            gate[b + ":source"] = "none" if "ReplicationConfigurationNotFound" in str(e) else "error"
            R.log("   %s: replication-source config: %s" % (b, gate[b + ":source"]))
        try:
            gate[b + ":versioning"] = s3w.get_bucket_versioning(Bucket=b).get("Status")
        except Exception as e:
            gate[b + ":versioning"] = "error:" + str(e)[:40]
        try:
            s3w.get_object_lock_configuration(Bucket=b)
            gate[b + ":object_lock"] = "PRESENT"
            R.warn("   %s has Object Lock -- deletes may be refused" % b)
        except Exception:
            gate[b + ":object_lock"] = "none"
        try:
            nc = s3w.get_bucket_notification_configuration(Bucket=b)
            n = sum(len(nc.get(k, [])) for k in ("LambdaFunctionConfigurations", "QueueConfigurations", "TopicConfigurations"))
            gate[b + ":notifications"] = n
        except Exception:
            gate[b + ":notifications"] = "?"
        tot, parts = bucket_size_gb(cww, b)
        R.log("   %-40s versioning=%s object_lock=%s notifications=%s size=%.0f GB %s"
              % (b, gate[b + ":versioning"], gate[b + ":object_lock"], gate[b + ":notifications"], tot,
                 {k.replace("Storage", ""): round(v) for k, v in parts.items()}))
        LEDGER["dr"][b] = {"size_gb_before": round(tot, 1), "parts": {k: round(v, 1) for k, v in parts.items()}}

    # top-level layout of the DR bucket
    replica_prefixes, keep_found, root_objects = [], [], []
    try:
        r = s3w.list_objects_v2(Bucket=DR, Delimiter="/", MaxKeys=1000)
        tops = [c["Prefix"] for c in r.get("CommonPrefixes", [])]
        root_objects = [o["Key"] for o in r.get("Contents", [])]
        R.log("   top-level prefixes: %s ; root objects: %d %s" % (tops, len(root_objects), root_objects[:6]))
        for p in tops:
            (keep_found if p in KEEP_PREFIXES else replica_prefixes).append(p)
    except Exception as e:
        FAILS.append("DR top-level listing: %s" % str(e)[:120])

    # exact numbers for the kept prefixes, sampled numbers for the replica
    for p in keep_found:
        try:
            st = prefix_bytes(s3w, DR, p, max_pages=600)
            R.log("   KEEP %-14s current %s (%.1f GB) noncurrent %s (%.1f GB) newest=%s%s"
                  % (p, "{:,}".format(st["current"]), st["current_gb"], "{:,}".format(st["noncurrent"]),
                     st["noncurrent_gb"], str(st["newest"])[:16], " (capped)" if st["truncated"] else ""))
            LEDGER["dr"].setdefault("keep", {})[p] = {k: (round(v, 2) if isinstance(v, float) else str(v)) for k, v in st.items()}
            R.kv(section="A_keep", prefix=p, current=st["current"], current_gb=round(st["current_gb"], 2),
                 noncurrent=st["noncurrent"], noncurrent_gb=round(st["noncurrent_gb"], 2))
        except Exception as e:
            R.warn("   %s census: %s" % (p, str(e)[:100]))

    # replica evidence, PER PREFIX: a prefix is purged only on its own evidence
    candidates, replica_prefixes, held = list(replica_prefixes), [], []
    gate["prefix_verdicts"] = {}
    base_ok = (gate.get("live_replication") == "none" and gate.get(DR + ":source") == "none"
               and gate.get(DR + ":object_lock") == "none")
    for p in candidates:
        sample = []
        try:
            r = s3w.list_objects_v2(Bucket=DR, Prefix=p, MaxKeys=20)
            sample += [(o["Key"], o["LastModified"]) for o in r.get("Contents", [])]
            for sub in ("_state/", "ops/", "warm/", "providers/", "raw/"):
                r2 = s3w.list_objects_v2(Bucket=DR, Prefix=p + sub, MaxKeys=8)
                sample += [(o["Key"], o["LastModified"]) for o in r2.get("Contents", [])]
        except Exception as e:
            R.warn("   sample %s: %s" % (p, str(e)[:80]))
        seen = set()
        sample = [x for x in sample if not (x[0] in seen or seen.add(x[0]))][:48]
        n_head = n_rep = n_live = 0
        newest = None
        for key, lm in sample:
            h = head(s3w, DR, key)
            if h.get("_missing"):
                continue
            n_head += 1
            n_rep += 1 if h.get("ReplicationStatus") == "REPLICA" else 0
            n_live += 0 if head(s3e, LIVE, key).get("_missing") else 1
            newest = lm if (newest is None or lm > newest) else newest
        ok = (base_ok and n_head >= 8 and n_rep / max(n_head, 1) >= 0.90
              and n_live / max(n_head, 1) >= 0.90 and newest is not None and newest < REPL_KILL_DATE)
        gate["prefix_verdicts"][p] = {"headed": n_head, "replica": n_rep, "in_live": n_live,
                                      "newest": str(newest), "purge": ok}
        (R.ok if ok else R.warn)("   %-22s headed %2d  REPLICA %2d  in-live %2d  newest %s  -> %s"
                                 % (p, n_head, n_rep, n_live, str(newest)[:16], "PURGE" if ok else "HELD"))
        (replica_prefixes if ok else held).append(p)
    # root-level objects: individually
    root_replica = []
    for key in root_objects:
        h = head(s3w, DR, key)
        if h.get("ReplicationStatus") == "REPLICA" and not head(s3e, LIVE, key).get("_missing"):
            root_replica.append(key)
    if root_objects:
        R.log("   root objects: %d, replica-and-present-in-live: %d" % (len(root_objects), len(root_replica)))
    gate_ok = base_ok and bool(replica_prefixes)
    LEDGER["dr"]["gate"] = gate
    LEDGER["dr"]["replica_prefixes"] = replica_prefixes
    LEDGER["dr"]["held_prefixes"] = held
    LEDGER["dr"]["root_replica"] = root_replica
    if gate_ok:
        R.ok("   GATE PASS for %s -- stale CRR replica of the live bucket; purge approved by Khalid 2026-09-03; held: %s"
             % (replica_prefixes, held))
    else:
        R.fail("   GATE FAIL: %s -- nothing deleted" % json.dumps(gate, default=str)[:400])

    # DR2 gate (small bucket)
    dr2_ok = False
    try:
        r = s3w.list_objects_v2(Bucket=DR2, MaxKeys=60)
        objs = r.get("Contents", [])
        tops2 = sorted({o["Key"].split("/")[0] + "/" for o in objs})
        rep2 = sum(1 for o in objs[:40] if head(s3w, DR2, o["Key"]).get("ReplicationStatus") == "REPLICA")
        inlive2 = sum(1 for o in objs[:40] if not head(s3e, LIVE, o["Key"]).get("_missing"))
        n2 = min(len(objs), 40)
        R.log("   %s: prefixes %s ; sampled %d, REPLICA %d, in live %d" % (DR2, tops2, n2, rep2, inlive2))
        dr2_ok = n2 > 0 and rep2 / n2 >= 0.90 and inlive2 / n2 >= 0.90 and gate.get(DR2 + ":source") == "none"
        LEDGER["dr"][DR2].update({"prefixes": tops2, "sampled": n2, "replica": rep2, "in_live": inlive2, "gate_ok": dr2_ok})
        if not dr2_ok:
            R.warn("   %s: gate not met -- left untouched" % DR2)
    except Exception as e:
        R.warn("   %s scan: %s" % (DR2, str(e)[:100]))

    # ================================================================ B
    R.section("B. Preserve what is NOT replica: quarantine zips -> live bucket; backup/ retention 30d")
    preserved = []
    if "quarantine/" in keep_found:
        try:
            for page in s3w.get_paginator("list_objects_v2").paginate(Bucket=DR, Prefix="quarantine/"):
                for o in page.get("Contents", []):
                    dest = QUAR_DEST + o["Key"][len("quarantine/"):]
                    s3e.copy_object(Bucket=LIVE, Key=dest, CopySource={"Bucket": DR, "Key": o["Key"]},
                                    MetadataDirective="COPY")
                    hd = head(s3e, LIVE, dest)
                    ok = (not hd.get("_missing")) and hd.get("ContentLength") == o.get("Size")
                    preserved.append({"src": o["Key"], "dest": dest, "bytes": o.get("Size"), "verified": ok})
                    (R.ok if ok else R.fail)("   %s -> s3://%s/%s (%s bytes)%s"
                                             % (o["Key"], LIVE, dest, o.get("Size"), "" if ok else " SIZE MISMATCH"))
                    if not ok:
                        FAILS.append("quarantine copy mismatch: %s" % o["Key"])
        except Exception as e:
            FAILS.append("quarantine preserve: %s" % str(e)[:140])
    LEDGER["dr"]["preserved"] = preserved

    # backup/ snapshot dates and per-day size
    try:
        days = []
        r = s3w.list_objects_v2(Bucket=DR, Prefix="backup/", Delimiter="/", MaxKeys=1000)
        days = sorted(c["Prefix"] for c in r.get("CommonPrefixes", []))
        R.log("   backup/ snapshot days: %d (%s .. %s)" % (len(days), days[0] if days else "-", days[-1] if days else "-"))
        if days:
            st = prefix_bytes(s3w, DR, days[-1], max_pages=40)
            R.log("   latest snapshot %s: %s objects, %.2f GB -> ~%.1f GB per 30 days"
                  % (days[-1], "{:,}".format(st["current"]), st["current_gb"], st["current_gb"] * 30))
            LEDGER["dr"]["backup_days"] = len(days)
            LEDGER["dr"]["backup_latest_gb"] = round(st["current_gb"], 2)
    except Exception as e:
        R.warn("   backup/ census: %s" % str(e)[:100])

    # ================================================================ C
    R.section("C. Lifecycle first -- the purge completes on its own even if the sweep below is cut short")
    if gate_ok and replica_prefixes:
        try:
            try:
                rules = s3w.get_bucket_lifecycle_configuration(Bucket=DR).get("Rules", [])
            except ClientError as e:
                rules = [] if "NoSuchLifecycleConfiguration" in str(e) else None
                if rules is None:
                    raise
            have = {r_.get("ID") for r_ in rules}
            new = []
            for p in replica_prefixes:
                rid = "ops5166-purge-replica-" + p.strip("/").replace("/", "-")[:40]
                if rid not in have:
                    new.append({"ID": rid, "Status": "Enabled", "Filter": {"Prefix": p},
                                "Expiration": {"Days": 1},
                                "NoncurrentVersionExpiration": {"NoncurrentDays": 1}})
            if "ops5166-backup-30d" not in have:
                new.append({"ID": "ops5166-backup-30d", "Status": "Enabled", "Filter": {"Prefix": "backup/"},
                            "Expiration": {"Days": 30},
                            "NoncurrentVersionExpiration": {"NoncurrentDays": 1}})
            if "ops5166-expired-markers" not in have:
                new.append({"ID": "ops5166-expired-markers", "Status": "Enabled", "Filter": {"Prefix": ""},
                            "Expiration": {"ExpiredObjectDeleteMarker": True}})
            if new:
                s3w.put_bucket_lifecycle_configuration(Bucket=DR, LifecycleConfiguration={"Rules": rules + new})
                back = {r_.get("ID") for r_ in s3w.get_bucket_lifecycle_configuration(Bucket=DR).get("Rules", [])}
                missing = [n_["ID"] for n_ in new if n_["ID"] not in back]
                if missing:
                    FAILS.append("lifecycle rules not readable back: %s" % missing)
                else:
                    R.ok("   %d lifecycle rules armed on %s: %s" % (len(new), DR, [n_["ID"] for n_ in new]))
                    LEDGER["dr"]["lifecycle_added"] = new
            else:
                R.ok("   lifecycle already armed")
        except Exception as e:
            FAILS.append("DR lifecycle: %s" % str(e)[:160])
    if dr2_ok:
        try:
            try:
                rules2 = s3w.get_bucket_lifecycle_configuration(Bucket=DR2).get("Rules", [])
            except ClientError as e:
                rules2 = [] if "NoSuchLifecycleConfiguration" in str(e) else None
                if rules2 is None:
                    raise
            if not any(r_.get("ID") == "ops5166-purge-all" for r_ in rules2):
                rules2.append({"ID": "ops5166-purge-all", "Status": "Enabled", "Filter": {"Prefix": ""},
                               "Expiration": {"Days": 1}, "NoncurrentVersionExpiration": {"NoncurrentDays": 1}})
                s3w.put_bucket_lifecycle_configuration(Bucket=DR2, LifecycleConfiguration={"Rules": rules2})
                R.ok("   lifecycle purge-all armed on %s" % DR2)
        except Exception as e:
            R.warn("   %s lifecycle: %s" % (DR2, str(e)[:100]))

    # ================================================================ D
    R.section("D. Accelerated sweep -- parallel version delete, %d-minute budget" % (SWEEP_BUDGET_S // 60))
    deleted = {"n": 0, "bytes": 0, "errors": 0, "tasks": 0}
    lock = threading.Lock()
    deadline = time.time() + SWEEP_BUDGET_S

    def sweep(bucket, prefix, depth, pool, futures):
        kw = {"Bucket": bucket, "Prefix": prefix, "Delimiter": "/", "MaxKeys": 1000}
        n = b = errs = 0
        while time.time() < deadline:
            try:
                r = s3w.list_object_versions(**kw)
            except Exception:
                errs += 1
                break
            if depth < 7:
                for c in r.get("CommonPrefixes", []):
                    with lock:
                        deleted["tasks"] += 1
                    futures.append(pool.submit(sweep, bucket, c["Prefix"], depth + 1, pool, futures))
            objs = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in r.get("Versions", [])]
            b_page = sum(v.get("Size", 0) for v in r.get("Versions", []))
            objs += [{"Key": d["Key"], "VersionId": d["VersionId"]} for d in r.get("DeleteMarkers", [])]
            if objs:
                try:
                    resp = s3w.delete_objects(Bucket=bucket, Delete={"Objects": objs, "Quiet": True})
                    e_ = len(resp.get("Errors", []))
                    n += len(objs) - e_
                    b += b_page
                    errs += e_
                except Exception:
                    errs += len(objs)
            if not r.get("IsTruncated"):
                break
            kw["KeyMarker"] = r.get("NextKeyMarker")
            kw["VersionIdMarker"] = r.get("NextVersionIdMarker")
        with lock:
            deleted["n"] += n
            deleted["bytes"] += b
            deleted["errors"] += errs

    if gate_ok and replica_prefixes:
        futures = []
        with ThreadPoolExecutor(max_workers=16) as pool:
            for p in replica_prefixes:
                deleted["tasks"] += 1
                futures.append(pool.submit(sweep, DR, p, 1, pool, futures))
            if root_replica:
                # root-level replica objects (no prefix for lifecycle to filter on) -- explicit, all versions
                try:
                    rr = set(root_replica)
                    r = s3w.list_object_versions(Bucket=DR, Delimiter="/", MaxKeys=1000)
                    objs = [{"Key": v["Key"], "VersionId": v["VersionId"]} for v in r.get("Versions", [])
                            if v["Key"] in rr]
                    objs += [{"Key": d["Key"], "VersionId": d["VersionId"]} for d in r.get("DeleteMarkers", [])
                             if d["Key"] in rr]
                    if objs:
                        s3w.delete_objects(Bucket=DR, Delete={"Objects": objs, "Quiet": True})
                        deleted["n"] += len(objs)
                        R.log("   root-level replica versions deleted: %d" % len(objs))
                except Exception as e:
                    R.warn("   root sweep: %s" % str(e)[:100])
            if dr2_ok:
                deleted["tasks"] += 1
                futures.append(pool.submit(sweep, DR2, "", 1, pool, futures))
            last = time.time()
            while True:
                pending = [f for f in futures if not f.done()]
                if not pending:
                    break
                if time.time() - last >= 120:
                    last = time.time()
                    R.log("   t+%4.0fs  deleted %s versions (%.1f GB)  tasks %d  errors %d"
                          % (time.time() - T0, "{:,}".format(deleted["n"]), deleted["bytes"] / 1e9,
                             deleted["tasks"], deleted["errors"]))
                time.sleep(5)
        R.log("   sweep done: %s versions / delete markers removed, %.1f GB of version bytes, %d errors, %d prefix tasks, %.0fs"
              % ("{:,}".format(deleted["n"]), deleted["bytes"] / 1e9, deleted["errors"], deleted["tasks"], time.time() - T0))
        if time.time() >= deadline:
            R.warn("   budget reached -- the lifecycle rules finish the remainder within ~24-48h")
        LEDGER["dr"]["sweep"] = dict(deleted)
    else:
        R.log("   sweep skipped (gate not passed or nothing to purge)")

    # ================================================================ E
    R.section("E. SnapStart: justhodl-ai-chat off + delete every snapshotted published version (alias/URL-safe)")
    try:
        c = lam.get_function_configuration(FunctionName="justhodl-ai-chat")
        R.log("   ai-chat SnapStart before: %s  runtime=%s" % (c.get("SnapStart"), c.get("Runtime")))
        if (c.get("SnapStart") or {}).get("ApplyOn") == "PublishedVersions":
            lam.update_function_configuration(FunctionName="justhodl-ai-chat", SnapStart={"ApplyOn": "None"})
            for _ in range(30):
                time.sleep(3)
                c = lam.get_function_configuration(FunctionName="justhodl-ai-chat")
                if c.get("LastUpdateStatus") in (None, "Successful"):
                    break
            R.ok("   ai-chat SnapStart now: %s (LastUpdateStatus=%s)" % (c.get("SnapStart"), c.get("LastUpdateStatus")))
        LEDGER["snapstart"]["ai_chat"] = c.get("SnapStart")
    except Exception as e:
        FAILS.append("ai-chat SnapStart: %s" % str(e)[:140])

    del_versions, kept = [], []
    try:
        for page in lam.get_paginator("list_functions").paginate():
            for f in page["Functions"]:
                fn = f["FunctionName"]
                vers = []
                for vp in lam.get_paginator("list_versions_by_function").paginate(FunctionName=fn):
                    vers += [v for v in vp["Versions"] if v["Version"] != "$LATEST"]
                snap_vers = [v for v in vers if (v.get("SnapStart") or {}).get("OptimizationStatus") == "On"]
                if not snap_vers:
                    continue
                protected = set()
                try:
                    for ap in lam.get_paginator("list_aliases").paginate(FunctionName=fn):
                        for a in ap["Aliases"]:
                            protected.add(a["FunctionVersion"])
                            for v_ in ((a.get("RoutingConfig") or {}).get("AdditionalVersionWeights") or {}):
                                protected.add(v_)
                except Exception:
                    pass
                try:
                    for u in lam.list_function_url_configs(FunctionName=fn).get("FunctionUrlConfigs", []):
                        q = u.get("FunctionArn", "").split(":")[-1]
                        if q and q != fn:
                            protected.add(q)
                except Exception:
                    pass
                for v in snap_vers:
                    if v["Version"] in protected:
                        kept.append("%s:%s" % (fn, v["Version"]))
                        continue
                    try:
                        lam.delete_function(FunctionName=fn, Qualifier=v["Version"])
                        del_versions.append("%s:%s" % (fn, v["Version"]))
                    except Exception as e:
                        R.warn("   delete %s:%s: %s" % (fn, v["Version"], str(e)[:80]))
        R.ok("   snapshotted versions deleted: %d %s" % (len(del_versions), del_versions[:16]))
        if kept:
            R.warn("   kept (alias/URL-referenced): %s" % kept)
        LEDGER["snapstart"]["deleted_versions"] = del_versions
        LEDGER["snapstart"]["kept"] = kept
    except Exception as e:
        FAILS.append("snapshot version sweep: %s" % str(e)[:140])

    # ================================================================ F
    R.section("F. Verification of the ops-5164 stand-down (last 6h)")
    for fn in ("justhodl-repo", "justhodl-fundamental-census", "justhodl-census-us", "justhodl-boj-full",
               "justhodl-ecb-deep", "justhodl-sdmx-walker"):
        try:
            out = {}
            for m in ("Invocations", "Errors", "Duration"):
                res = cwe.get_metric_statistics(
                    Namespace="AWS/Lambda", MetricName=m,
                    Dimensions=[{"Name": "FunctionName", "Value": fn}],
                    StartTime=NOW - timedelta(hours=6), EndTime=NOW, Period=21600, Statistics=["Sum"])
                out[m] = sum(p["Sum"] for p in res.get("Datapoints", []))
            R.log("   %-32s invocations %6d  errors %4d  %.1f Lambda-hours" % (fn, out["Invocations"], out["Errors"], out["Duration"] / 3.6e6))
            LEDGER["verify"][fn] = out
        except Exception as e:
            R.warn("   %s: %s" % (fn, str(e)[:80]))
    tot, parts = bucket_size_gb(cwe, LIVE)
    R.log("   live bucket size (latest daily point): %.0f GB" % tot)
    tot2, _ = bucket_size_gb(cww, DR)
    R.log("   DR bucket size (latest daily point, lags a day): %.0f GB" % tot2)

    # ================================================================ G
    try:
        s3e.put_object(Bucket=LIVE, Key=LEDGER_KEY, Body=json.dumps(LEDGER, indent=1, default=str).encode(),
                       ContentType="application/json")
        R.ok("   ledger written to s3://%s/%s" % (LIVE, LEDGER_KEY))
    except Exception as e:
        FAILS.append("ledger: %s" % str(e)[:100])
    (ROOT / "aws" / "ops" / "reports" / "5166_dr_purge_and_snapstart.json").write_text(
        json.dumps(LEDGER, indent=1, default=str), encoding="utf-8")
    if FAILS:
        for f in FAILS:
            R.fail(f)
        sys.exit(1)
    R.ok("ops 5166 complete in %.0fs" % (time.time() - T0))
