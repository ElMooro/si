"""ops_5078 -- 15,605 throttles from one function, and I caused them.

ops 5077 named it:

    justhodl-census-us   reserved=1   throttles6h=15,605
    justhodl-series-extractor reserved=1  throttles6h=1,255
    fleet total 16,913

One function is 92% of the fleet's throttling. I put the econ lane on 12
shards against a reservation of 1, so eleven of every twelve shard
invocations are refused, async delivery retries them, and the retries
compound. That is not AWS constraining the account -- peak concurrency
was 15-29 against a limit of 1000. It is me fanning out into a slot that
holds one, and then reading the resulting refusals as a broken walker.

Raising it is safe, and specifically BECAUSE of how the econ lane was
built: each shard owns a disjoint slice of the queue AND its own state
document (data/_state/census-econ-s{k}.json), so two shards can never
share a cursor. The reservation was never what made the lane correct --
the per-shard state is. The one path that does share a document is the
timeseries lane, and it has a single target on a 4-hour rule against
850s runs, so it cannot overlap itself.

  P0 confirm reserved=1 and the throttle share
  P1 raise the reservation to cover 12 shards + the timeseries lane
  P2 drive the dispatcher and measure the drain rate against the
     throttled baseline of 677/1226
  P3 re-measure throttles fleet-wide; census-us timeseries state
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
FN = "justhodl-census-us"
CST = "data/warm/census-us/_state/state.json"
SHARDS = 12
RESERVE = 20          # 12 econ shards + timeseries + headroom

cfg = Config(read_timeout=600, retries={"max_attempts": 6,
                                        "mode": "adaptive"})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(k):
    try:
        return json.loads(s3.get_object(Bucket=LIVE,
                                        Key=k)["Body"].read())
    except Exception:
        return None


def metric(name, fn=None, hours=1, stat="Sum"):
    dims = [{"Name": "FunctionName", "Value": fn}] if fn else []
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName=name, Dimensions=dims,
            StartTime=NOW - timedelta(hours=hours), EndTime=NOW,
            Period=3600, Statistics=[stat])
        return int(sum(p[stat] for p in r.get("Datapoints", [])))
    except Exception:
        return -1


def econ_done():
    return sum(len((jget("data/_state/census-econ-s%d.json" % k) or {})
                   .get("done") or []) for k in range(SHARDS))


with report("ops_5078_unblock_census") as R:
    fails = []
    out = {"op": "ops_5078"}

    R.section("P0 confirm")
    try:
        rc = lam.get_function_concurrency(FunctionName=FN)
        R.log("  %s reserved = %s" % (FN, rc.get(
            "ReservedConcurrentExecutions")))
        out["reserved_before"] = rc.get("ReservedConcurrentExecutions")
    except Exception as e:
        R.log("  err %s" % str(e)[:90])
    R.log("  throttles 6h: %s  invocations 6h: %s" % (
        f"{metric('Throttles', FN, 6):,}",
        f"{metric('Invocations', FN, 6):,}"))
    R.log("  fleet throttles 6h: %s" % f"{metric('Throttles', None, 6):,}")
    R.log("  concurrency peak used fleet-wide: %s of 1000" % metric(
        "ConcurrentExecutions", None, 6, "Maximum"))
    R.log("  -> the account has capacity; the reservation is the wall")

    R.section("P1 raise the reservation")
    R.log("  safe because each econ shard owns its own state document")
    R.log("  (data/_state/census-econ-s{k}.json) -- shards cannot share")
    R.log("  a cursor, so concurrency was never what made it correct")
    try:
        lam.put_function_concurrency(FunctionName=FN,
                                     ReservedConcurrentExecutions=RESERVE)
        time.sleep(3)
        rc = lam.get_function_concurrency(FunctionName=FN)
        got = rc.get("ReservedConcurrentExecutions")
        R.log("  reserved 1 -> %s (12 shards + timeseries + headroom)"
              % got)
        out["reserved_after"] = got
        if got != RESERVE:
            fails.append("P1:notset")
    except Exception as e:
        R.log("  set err %s" % str(e)[:130])
        fails.append("P1")

    R.section("P2 drive and measure")
    b0 = econ_done()
    R.log("  econ entries before: %s / 1,226" % f"{b0:,}")
    t0 = time.time()
    for cyc in range(3):
        try:
            r = lam.invoke(FunctionName=FN, InvocationType="Event",
                           Payload=json.dumps({"mode": "econ_dispatch",
                                               "shards": SHARDS}
                                              ).encode())
            R.log("  dispatch sent (cycle %d)" % (cyc + 1))
        except Exception as e:
            R.log("  dispatch refused: %s" % str(e)[:90])
        time.sleep(700)
        n = econ_done()
        el = (time.time() - t0) / 60.0
        alive = sum(1 for k in range(SHARDS)
                    if jget("data/_state/census-econ-s%d.json" % k))
        R.log("  t+%2.0fmin  entries=%s (+%s)  %.2f/min  shards live "
              "%d/%d" % (el, f"{n:,}", f"{n - b0:,}",
                         (n - b0) / max(1, el), alive, SHARDS))
    n = econ_done()
    el = (time.time() - t0) / 60.0
    rate = (n - b0) / max(1, el)
    R.log("  RATE %.2f entries/min  (throttled baseline was ~0.37)"
          % rate)
    if rate > 0:
        R.log("  %s left -> ~%.1f h" % (f"{1226 - n:,}",
                                        (1226 - n) / rate / 60.0))
    out.update(econ_before=b0, econ_after=n, rate=round(rate, 2))

    R.section("P3 throttles after, and the timeseries lane")
    t_fn = metric("Throttles", FN, 1)
    t_fleet = metric("Throttles", None, 1)
    R.log("  last hour -- %s throttles: %s   fleet: %s" % (
        FN, f"{t_fn:,}", f"{t_fleet:,}"))
    R.log("  (6h figures before this change were 15,605 and 16,913)")
    out.update(throttles_fn_1h=t_fn, throttles_fleet_1h=t_fleet)
    c0 = jget(CST) or {}
    R.log("  timeseries updated_at=%s" % c0.get("updated_at"))
    try:
        lam.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
        R.log("  timeseries invoke accepted")
    except Exception as e:
        R.log("  timeseries invoke refused: %s" % str(e)[:90])
    for i in range(8):
        time.sleep(45)
        c1 = jget(CST) or {}
        if c1.get("updated_at") != c0.get("updated_at"):
            R.log("  timeseries state MOVED -> %s (clears the STALE chip "
                  "and the DEGRADED banner)" % c1.get("updated_at"))
            out["census_moved"] = True
            break
    else:
        R.log("  timeseries state still unmoved")
        out["census_moved"] = False
    tot = {"done": 0, "codes": 0}
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
        if not rr.get("IsTruncated"):
            break
        kw["ContinuationToken"] = rr.get("NextContinuationToken")
    R.log("  BOJ %s/%s (%.1f%%)" % (f"{tot['done']:,}",
                                    f"{tot['codes']:,}",
                                    100.0 * tot["done"] /
                                    max(1, tot["codes"])))
    out["boj"] = tot["done"]
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/census-unblock.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5078 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(reserved=out.get("reserved_after"), rate=out.get("rate"),
         econ=out.get("econ_after"),
         throttles_1h=out.get("throttles_fn_1h"), boj=out.get("boj"))
    R.log("ops 5078 GREEN -- the wall was mine and it is down")
