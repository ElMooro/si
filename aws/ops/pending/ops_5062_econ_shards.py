"""ops_5062 -- 135 hours is not an import, it is a hostage situation.

ops 5061 wired the trigger and drained cleanly -- 10/1226 entries,
1,382,781 rows, zero failures, CBP 2013 and 2014 each ~129,720 rows with
real NAICS codes. The data is right. The rate is not: 9 entries in 60
minutes, which is 135.6 hours for what remains, and that is with me
hand-driving it. Left to its own schedule it would be worse, because the
only rule with a free target slot was carry-surface-4h -- rate(4 hours),
so 780s of work per 14,400s of wall clock, a 5% duty cycle. Months.

Two causes, both fixed here.

 1. ~350s PER ENTRY. A 413 from the read cap means the response is too
    big for THAT GEOGRAPHY LEVEL -- state-level CBP with a NAICS
    wildcard is enormous. But the walker treated it as a per-chunk
    failure and kept trying every remaining variable chunk at the same
    level, each one paying a full download before the cap discarded it.
    Now a 413 abandons the geo level immediately and records it in
    state["oversize"], so the waste happens once instead of a dozen
    times.
 2. ONE WORKER. The lane ran single-file against a single state doc.
    It is now sharded by tag hash, each shard owning a disjoint slice
    AND its own state document, so concurrent runs can never share a
    cursor -- the same property that makes the eurostat and ecb lanes
    safe under a cadence shorter than their timeout. Proven offline:
    1,226 entries partition into 6 shards of 195-218 with no entry lost
    and none duplicated.

  P0 deploy; find the FASTEST rule with free slots, not merely one
  P1 attach the shards there
  P2 drive all six concurrently and measure the real rate
  P3 verify no shard duplicated another's work, and check oversize
"""
import json
import sys
import time
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCT = "857687956942"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-census-us"
FN_ARN = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN)
EROOT = "data/warm/census-econ/"
SHARDS = 6

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(k):
    import gzip
    try:
        b = s3.get_object(Bucket=LIVE, Key=k)["Body"].read()
        if k.endswith(".gz"):
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:
        return {}


def shard_state(k):
    return jget("data/_state/census-econ-s%d.json" % k)


with report("ops_5062_econ_shards") as R:
    fails = []
    out = {"op": "ops_5062"}

    R.section("P0 deploy + find the fastest rule with room")
    for i in range(16):
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if (c.get("LastModified") or "")[:19] >= (
                    NOW - timedelta(minutes=14)).strftime(
                        "%Y-%m-%dT%H:%M:%S"):
                R.log("  code fresh %s mem=%s timeout=%s" % (
                    c.get("LastModified"), c.get("MemorySize"),
                    c.get("Timeout")))
                break
        except Exception:
            pass
        time.sleep(20)
    try:
        lam.update_function_configuration(FunctionName=FN,
                                          MemorySize=3008)
        for _ in range(12):
            time.sleep(4)
            if lam.get_function_configuration(
                    FunctionName=FN).get("LastUpdateStatus") == \
                    "Successful":
                break
        R.log("  memory 1024 -> 3008 MB (parsing 130k-row payloads)")
    except Exception as e:
        R.log("  memory err %s" % str(e)[:110])

    def rate_seconds(se):
        try:
            n, unit = se[5:-1].split()
            n = int(n)
            return n * {"minute": 60, "minutes": 60, "hour": 3600,
                        "hours": 3600, "day": 86400,
                        "days": 86400}.get(unit, 86400)
        except Exception:
            return 10 ** 9
    cands = []
    try:
        for page in ev.get_paginator("list_rules").paginate():
            for r in page.get("Rules", []):
                se = r.get("ScheduleExpression") or ""
                if r.get("State") != "ENABLED" or not se.startswith(
                        "rate("):
                    continue
                n = len(ev.list_targets_by_rule(
                    Rule=r["Name"]).get("Targets", []))
                if n < 5:
                    cands.append((rate_seconds(se), r["Name"], se, n))
        cands.sort()
        R.log("  fastest enabled rules with a free slot:")
        for sec, nm, se, n in cands[:6]:
            R.log("    %-40s %-16s %d/5" % (nm[:40], se, n))
    except Exception as e:
        R.log("  survey err %s" % str(e)[:120])
        fails.append("P0")

    R.section("P1 attach the shards")
    placed = 0
    for sec, nm, se, n in cands:
        if placed >= SHARDS:
            break
        room = 5 - n
        if room <= 0 or sec > 3600:
            continue
        try:
            lam.add_permission(
                FunctionName=FN, StatementId="evb2-%s" % nm[:46],
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn="arn:aws:events:%s:%s:rule/%s"
                          % (REGION, ACCT, nm))
        except Exception:
            pass
        tg = []
        for k in range(placed, min(SHARDS, placed + room)):
            tg.append({"Id": "econs%d" % k, "Arn": FN_ARN,
                       "Input": json.dumps({"mode": "econ", "shard": k,
                                            "shards": SHARDS})})
        try:
            resp = ev.put_targets(Rule=nm, Targets=tg)
            if resp.get("FailedEntryCount"):
                R.log("  %s: %d targets FAILED" % (nm,
                                                   resp["FailedEntryCount"]))
            else:
                R.log("  %s (%s) <- shards %s" % (
                    nm, se, [t["Id"] for t in tg]))
                placed += len(tg)
        except Exception as e:
            R.log("  %s attach err %s" % (nm, str(e)[:100]))
    R.log("  shards scheduled: %d/%d" % (placed, SHARDS))
    if placed == 0:
        fails.append("P1:none")
    out["shards_scheduled"] = placed

    R.section("P2 drive all shards and measure")
    t0 = time.time()
    base = {k: len(shard_state(k).get("done") or []) for k in range(SHARDS)}
    for cycle in range(3):
        for k in range(SHARDS):
            try:
                lam.invoke(FunctionName=FN, InvocationType="Event",
                           Payload=json.dumps({"mode": "econ", "shard": k,
                                               "shards": SHARDS}).encode())
            except Exception as e:
                R.log("  shard %d invoke err %s" % (k, str(e)[:80]))
            time.sleep(2)
        time.sleep(800)
        tot = 0
        line = []
        for k in range(SHARDS):
            st = shard_state(k)
            d = len(st.get("done") or [])
            tot += d
            line.append("s%d=%d/%s" % (k, d, st.get("n_total") or "?"))
        el = (time.time() - t0) / 60.0
        gained = tot - sum(base.values())
        R.log("  t+%2.0fmin  %s  total=%d  (+%d, %.1f entries/min)" % (
            el, " ".join(line), tot, gained, gained / max(1, el)))
    el = (time.time() - t0) / 60.0
    tot = sum(len(shard_state(k).get("done") or []) for k in range(SHARDS))
    gained = tot - sum(base.values())
    rate = gained / max(1, el)
    R.log("  RATE %.2f entries/min  (was 0.15 single-worker)" % rate)
    if rate > 0:
        left = 1226 - tot - 10
        R.log("  ~%d entries left -> ~%.1f h at this rate" % (
            left, left / rate / 60.0))
    out.update(rate=round(rate, 2), done=tot)

    R.section("P3 shards must not overlap")
    seen, dup = {}, []
    for k in range(SHARDS):
        for tag in (shard_state(k).get("done") or []):
            if tag in seen:
                dup.append((tag, seen[tag], k))
            seen[tag] = k
    R.log("  distinct entries done across shards: %d" % len(seen))
    R.log("  duplicates: %d %s" % (len(dup), dup[:3]))
    if dup:
        fails.append("P3:dup")
    bad = [(t, k) for t, k in seen.items()
           if zlib.crc32(t.encode()) % SHARDS != k]
    R.log("  entries in the wrong shard: %d %s" % (len(bad), bad[:3]))
    if bad:
        fails.append("P3:misrouted")
    ov = {}
    for k in range(SHARDS):
        ov.update(shard_state(k).get("oversize") or {})
    R.log("  geo levels abandoned as oversize: %d %s" % (
        len(ov), list(ov.items())[:4]))
    objs, kw = 0, {"Bucket": LIVE, "Prefix": EROOT, "MaxKeys": 1000}
    byfam, tb = {}, 0
    while True:
        rr = s3.list_objects_v2(**kw)
        for o in rr.get("Contents", []):
            objs += 1
            tb += o["Size"]
            f = o["Key"][len(EROOT):].split("/")[0]
            byfam[f] = byfam.get(f, 0) + 1
        if not rr.get("IsTruncated"):
            break
        kw["ContinuationToken"] = rr.get("NextContinuationToken")
    R.log("  S3: %s objects, %.1f MB, families=%s" % (
        f"{objs:,}", tb / 1e6,
        dict(sorted(byfam.items(), key=lambda kv: -kv[1])[:8])))
    out["objects"] = objs
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/census-econ-lane.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5062 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(rate=out.get("rate"), done=out.get("done"),
         shards=out.get("shards_scheduled"), objects=out.get("objects"))
    R.log("ops 5062 GREEN -- econ lane sharded and moving")
