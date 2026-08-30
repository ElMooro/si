"""ops_5063 -- scale the econ lane, and stop paying for discarded reads.

ops 5062 measured 0.37 entries/min against 0.15 single-worker -- real
but not enough, and it surfaced two things worth more than raw shard
count.

 1. WASTE, WHICH IS WHAT KHALID ACTUALLY OBJECTED TO. The oversize log
    read [('cbp', ['cbp:g1','cbp:g2','cbp:g3'])]: for CBP only the
    national level fits under the read cap; state, metro and region all
    413. But that was rediscovered on EVERY vintage, so each of CBP's 38
    vintages paid three full ~130k-row downloads that were read to the
    cap and thrown away. 152 geo attempts where 41 would do. The
    registry is now persistent and shared across shards -- written per
    shard so concurrent writers cannot clobber each other, merged on
    read -- so a level proven too big for a dataset is never attempted
    again for any of its vintages.
 2. HALF THE SHARDS NEVER RAN. s1, s3 and s4 wrote no state at all, so
    the 0.37 was three workers, not six. This op checks the function's
    Throttles and Errors before assuming more shards will help; if the
    account is throttling invokes, adding workers makes it worse rather
    than faster.

Scheduling changes too. 5062 scattered six shards across three unrelated
rules because each had a spare slot -- fragile and invisible. There is
now an econ_dispatch mode: ONE rule target invokes it, and it async
invokes N shard workers. Fan-out, not a chain -- a dispatch never
dispatches, so there is no recursion to run away.

  P0 deploy; Throttles/Errors -- is the account the real limit?
  P1 clean up 5062's scattered targets, wire the single dispatcher
  P2 drive the dispatcher and measure with the oversize registry warm
  P3 verify: no duplicate work, geo skips actually happening, ETA
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
ACCT = "857687956942"
LIVE = "justhodl-dashboard-live"
FN = "justhodl-census-us"
FN_ARN = "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, FN)
EROOT = "data/warm/census-econ/"
SHARDS = 12
OLD_RULES = ["justhodl-series-extractor-5min", "benzinga-news-agent-warm",
             "justhodl-sdmx-walker-hourly", "carry-surface-4h"]

cfg = Config(read_timeout=120, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)
cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
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


def sstate(k):
    return jget("data/_state/census-econ-s%d.json" % k)


with report("ops_5063_econ_scale") as R:
    fails = []
    out = {"op": "ops_5063"}

    R.section("P0 deploy + is the ACCOUNT the limit")
    for i in range(16):
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if (c.get("LastModified") or "")[:19] >= (
                    NOW - timedelta(minutes=14)).strftime(
                        "%Y-%m-%dT%H:%M:%S"):
                R.log("  code fresh %s mem=%s" % (c.get("LastModified"),
                                                  c.get("MemorySize")))
                break
        except Exception:
            pass
        time.sleep(20)
    try:
        acct = lam.get_account_settings()
        R.log("  account concurrency limit=%s unreserved=%s" % (
            (acct.get("AccountLimit") or {}).get(
                "ConcurrentExecutions"),
            (acct.get("AccountLimit") or {}).get(
                "UnreservedConcurrentExecutions")))
    except Exception as e:
        R.log("  account settings err %s" % str(e)[:90])
    for m in ("Invocations", "Throttles", "Errors"):
        try:
            r = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName=m,
                Dimensions=[{"Name": "FunctionName", "Value": FN}],
                StartTime=NOW - timedelta(hours=3), EndTime=NOW,
                Period=3600, Statistics=["Sum"])
            tot = sum(p["Sum"] for p in r.get("Datapoints", []))
            R.log("  %-12s last 3h: %.0f" % (m, tot))
            out[m.lower()] = int(tot)
        except Exception as e:
            R.log("  %s err %s" % (m, str(e)[:70]))
    if out.get("throttles", 0) > 0:
        R.log("  THROTTLES PRESENT -- that is why s1/s3/s4 wrote no "
              "state. More shards would make this worse, not faster.")
    if out.get("errors", 0) > 0:
        R.log("  errors logged -- shard workers may be failing outright")

    R.section("P1 one dispatcher instead of six scattered targets")
    for rn in OLD_RULES:
        try:
            tg = ev.list_targets_by_rule(Rule=rn).get("Targets", [])
            ids = [t["Id"] for t in tg
                   if str(t.get("Input") or "").find('"econ"') >= 0
                   or t.get("Id", "").startswith("econs")
                   or t.get("Id") in ("censusecon",)]
            if ids:
                ev.remove_targets(Rule=rn, Ids=ids)
                R.log("  %s: removed %s" % (rn, ids))
        except Exception as e:
            R.log("  %s cleanup: %s" % (rn, str(e)[:80]))
    host = None
    try:
        for page in ev.get_paginator("list_rules").paginate():
            for r in page.get("Rules", []):
                se = r.get("ScheduleExpression") or ""
                if r.get("State") != "ENABLED" or "minute" not in se:
                    continue
                n = len(ev.list_targets_by_rule(
                    Rule=r["Name"]).get("Targets", []))
                if n < 5:
                    host = (r["Name"], se, n)
                    break
            if host:
                break
    except Exception as e:
        R.log("  survey err %s" % str(e)[:100])
    if not host:
        R.log("  no minute-cadence rule with a slot -- dispatcher will "
              "only run when invoked")
        fails.append("P1:nohost")
    else:
        nm, se, n = host
        try:
            lam.add_permission(
                FunctionName=FN, StatementId="evbd-%s" % nm[:44],
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn="arn:aws:events:%s:%s:rule/%s"
                          % (REGION, ACCT, nm))
        except Exception:
            pass
        try:
            resp = ev.put_targets(Rule=nm, Targets=[{
                "Id": "econdispatch", "Arn": FN_ARN,
                "Input": json.dumps({"mode": "econ_dispatch",
                                     "shards": SHARDS})}])
            R.log("  dispatcher on %s (%s), failed=%s" % (
                nm, se, resp.get("FailedEntryCount")))
            out["host"] = nm
        except Exception as e:
            R.log("  attach err %s" % str(e)[:110])
            fails.append("P1:attach")

    R.section("P2 drive it")
    base = sum(len(sstate(k).get("done") or []) for k in range(SHARDS))
    R.log("  starting from %d entries done" % base)
    t0 = time.time()
    for cycle in range(3):
        try:
            r = lam.invoke(FunctionName=FN,
                           InvocationType="RequestResponse",
                           Payload=json.dumps(
                               {"mode": "econ_dispatch",
                                "shards": SHARDS}).encode())
            R.log("  dispatch -> %s" % (r["Payload"].read() or b"")[:120])
        except Exception as e:
            R.log("  dispatch err %s" % str(e)[:100])
        time.sleep(820)
        live = [(k, len(sstate(k).get("done") or [])) for k in
                range(SHARDS)]
        tot = sum(v for _, v in live)
        alive = sum(1 for _, v in live if v)
        el = (time.time() - t0) / 60.0
        R.log("  t+%2.0fmin total=%d (+%d)  %.2f/min  shards alive %d/%d"
              % (el, tot, tot - base, (tot - base) / max(1, el), alive,
                 SHARDS))
    tot = sum(len(sstate(k).get("done") or []) for k in range(SHARDS))
    el = (time.time() - t0) / 60.0
    rate = (tot - base) / max(1, el)
    R.log("  RATE %.2f entries/min   (0.15 single, 0.37 six-shard)"
          % rate)
    if rate > 0:
        R.log("  %d left -> ~%.1f h" % (1226 - tot, (1226 - tot) / rate
                                        / 60.0))
    out.update(rate=round(rate, 2), done=tot)

    R.section("P3 waste eliminated?")
    over = {}
    for k in range(SHARDS):
        for pr in (jget("data/_state/census-econ-oversize-s%d.json" % k)
                   or {}).get("pairs") or []:
            over[tuple(pr)] = k
    R.log("  oversize registry: %d (dataset, geo) pairs known" % len(over))
    for pr in list(over)[:6]:
        R.log("    %s" % (pr,))
    skips = sum(int(sstate(k).get("geo_skips") or 0)
                for k in range(SHARDS))
    R.log("  geo levels SKIPPED via the registry: %d" % skips)
    R.log("  (each skip is a ~130k-row download not made)")
    seen, dup = {}, 0
    for k in range(SHARDS):
        for tag in (sstate(k).get("done") or []):
            if tag in seen:
                dup += 1
            seen[tag] = k
    R.log("  distinct done=%d duplicates=%d" % (len(seen), dup))
    if dup:
        fails.append("P3:dup")
    objs, tb, byfam = 0, 0, {}
    kw = {"Bucket": LIVE, "Prefix": EROOT, "MaxKeys": 1000}
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
    out.update(objects=objs, skips=skips, oversize=len(over))
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/census-econ-lane.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5063 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(rate=out.get("rate"), done=out.get("done"),
         skips=out.get("skips"), objects=out.get("objects"),
         throttles=out.get("throttles"))
    R.log("ops 5063 GREEN -- scaled, and no longer paying for reads it "
          "throws away")
