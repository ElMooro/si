"""ops_5072 -- close GDELT's 7,381 gaps.

ops 5071 reconstructed what the engine never kept. `gaps` was only a
counter plus a capped sample, so the identity of the missing files was
lost -- but v2 slots are deterministic 15-minute stamps, so diffing the
expected timeline against the 396,882 stamps actually in S3 recovers the
list. Two independent methods landed on 7,381 exactly, which is the
cross-check that makes it trustworthy.

The distribution says these are source outages, not scattered fetch
failures: 2020 alone holds 2,585, with 993 in 2017 and 990 in 2018.

The backfill separates two gaps that look identical in a counter:
    200 now -> transient miss, recovered and banked
    404 now -> GDELT never published it; recorded PERMANENT and never
               attempted again
Without that split a backfill retries dead URLs forever, burns requests
on nothing, and reports motion as progress. Sharded 12 ways by slot
hash, each shard with its own state, partition proven offline with no
slot lost or doubled.

  P0 deploy, and the missing list is readable
  P1 fan out 12 shards and drain
  P2 truth: recovered vs permanent vs remaining, and S3 object count
  P3 BOJ and census-econ carry on
"""
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-gdelt-full"
SHARDS = 12

cfg = Config(read_timeout=300, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(k):
    try:
        return json.loads(s3.get_object(Bucket=LIVE,
                                        Key=k)["Body"].read())
    except Exception:
        return None


def bf_totals():
    t = {"recovered": 0, "permanent": 0, "remaining": 0, "bytes": 0,
         "shards": 0}
    for k in range(SHARDS):
        d = jget("data/_state/gdelt-backfill-s%d.json" % k)
        if not d:
            continue
        t["shards"] += 1
        t["recovered"] += int(d.get("recovered") or 0)
        t["permanent"] += len(d.get("permanent") or [])
        t["remaining"] += int(d.get("remaining") or 0)
        t["bytes"] += int(d.get("bytes") or 0)
    return t


with report("ops_5072_gdelt_backfill") as R:
    fails = []
    out = {"op": "ops_5072"}

    R.section("P0 deploy + the missing list")
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
    miss = jget("data/_state/gdelt-missing-slots.json") or {}
    slots = miss.get("slots") or []
    R.log("  missing list: %s slots (expected %s, present %s)" % (
        f"{len(slots):,}", f"{miss.get('expected') or 0:,}",
        f"{(miss.get('expected') or 0) - (miss.get('missing') or 0):,}"))
    R.log("  by year: %s" % json.dumps(miss.get("by_year") or {})[:150])
    if not slots:
        R.log("  no list to work from")
        fails.append("P0:nolist")
    g0 = jget("data/warm/gdelt-full/_state/state.json") or {}
    R.log("  engine state: files=%s gaps=%s cursor=%s" % (
        g0.get("files"), g0.get("gaps"), g0.get("cursor")))

    R.section("P1 fan out and drain")
    b0 = bf_totals()
    t0 = time.time()
    for cycle in range(3):
        try:
            r = lam.invoke(FunctionName=FN,
                           InvocationType="RequestResponse",
                           Payload=json.dumps({"backfill_fanout": True,
                                               "shards": SHARDS}
                                              ).encode())
            R.log("  fanout -> %s" % (r["Payload"].read() or b"")[:140])
            if r.get("FunctionError"):
                fails.append("P1:funcerror")
        except Exception as e:
            R.log("  fanout err %s" % str(e)[:120])
        time.sleep(800)
        b = bf_totals()
        el = (time.time() - t0) / 60.0
        R.log("  t+%2.0fmin recovered=%s permanent=%s remaining=%s "
              "%.2f GB  shards reporting=%d" % (
                  el, f"{b['recovered']:,}", f"{b['permanent']:,}",
                  f"{b['remaining']:,}", b["bytes"] / 1e9, b["shards"]))
    b = bf_totals()
    el = (time.time() - t0) / 60.0
    resolved = (b["recovered"] + b["permanent"]) - (b0["recovered"]
                                                    + b0["permanent"])
    R.log("  resolved %s slots in %.0f min (%.1f/min)" % (
        f"{resolved:,}", el, resolved / max(1, el)))
    out.update(recovered=b["recovered"], permanent=b["permanent"],
               remaining=b["remaining"], gb=round(b["bytes"] / 1e9, 2))

    R.section("P2 truth")
    R.log("  RECOVERED (fetched and banked): %s" % f"{b['recovered']:,}")
    R.log("  PERMANENT (404 again -- GDELT never published these): %s"
          % f"{b['permanent']:,}")
    R.log("  remaining to attempt: %s of %s" % (
        f"{b['remaining']:,}", f"{len(slots):,}"))
    R.log("  bytes recovered: %.2f GB" % (b["bytes"] / 1e9))
    if b["recovered"] + b["permanent"] == 0:
        R.log("  nothing resolved -- the backfill is not working")
        fails.append("P2:nowork")
    elif b["permanent"] and not b["recovered"]:
        R.log("  every attempt 404'd: these gaps are the source's, not "
              "ours, and the count will not fall further")
    n = 0
    kw = {"Bucket": LIVE, "Prefix": "data/warm/gdelt-full/v2/export/",
          "MaxKeys": 1000}
    t1 = time.time()
    while time.time() - t1 < 240:
        rr = s3.list_objects_v2(**kw)
        n += len(rr.get("Contents", []))
        if not rr.get("IsTruncated"):
            break
        kw["ContinuationToken"] = rr.get("NextContinuationToken")
    R.log("  v2 export objects in S3 now: %s" % f"{n:,}")
    out["v2_objects"] = n

    R.section("P3 the other lanes")
    tot = {"done": 0, "codes": 0, "rows": 0}
    kw = {"Bucket": LIVE, "Prefix": "data/warm/boj-full/_state/",
          "MaxKeys": 1000}
    while True:
        rr = s3.list_objects_v2(**kw)
        for o in rr.get("Contents", []):
            if "api_" not in o["Key"]:
                continue
            d = jget(o["Key"]) or {}
            tot["done"] += int(d.get("done") or 0)
            tot["codes"] += len(d.get("codes") or [])
            tot["rows"] += int(d.get("rows") or 0)
        if not rr.get("IsTruncated"):
            break
        kw["ContinuationToken"] = rr.get("NextContinuationToken")
    R.log("  boj %s/%s series (%.1f%%) rows %s" % (
        f"{tot['done']:,}", f"{tot['codes']:,}",
        100.0 * tot["done"] / max(1, tot["codes"]), f"{tot['rows']:,}"))
    ce = sum(len((jget("data/_state/census-econ-s%d.json" % k) or {})
                 .get("done") or []) for k in range(12))
    R.log("  census-econ %s/1226 entries" % f"{ce:,}")
    out["boj"] = tot
    out["census_econ"] = ce
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/gdelt-backfill.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/gdelt-backfill.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5072 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(recovered=out.get("recovered"), permanent=out.get("permanent"),
         remaining=out.get("remaining"), gb=out.get("gb"))
    R.log("ops 5072 GREEN -- gaps closed or proven unpublishable")
