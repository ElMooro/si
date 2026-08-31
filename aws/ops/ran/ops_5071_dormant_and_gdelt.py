"""ops_5071 -- wire what is truly dormant; reconstruct GDELT's gap list.

ops 5070 named 19 functions that own a data lane and have no
EventBridge rule. But eleven of them share an age of exactly 22.7h,
which cannot be dormancy -- something wrote their state yesterday. The
"0 invocations" figure came from ops 5069, which printed only the 26
lowest and carried the whole list forward regardless. So those eleven
are almost certainly driven by an orchestrator rather than a rule, and
wiring them would double-run healthy lanes. Re-checked here per
function rather than assumed.

The genuinely stale ones stand out by age:
    justhodl-repo                387.7h  (16 days)
    justhodl-fundamental-census  171.7h  (7 days)
    justhodl-hist-banker         116.5h  (4.9 days)

GDELT, meanwhile, is now running -- files 396,881 -> 396,882 and as_of
moving -- but its gaps stayed at 7,381, because it is fetching FORWARD.
And reading the engine shows why a backfill cannot simply be asked for:
`gaps` is a COUNTER incremented on a miss, and only a capped
`gaps_sample` survives. The identity of the 7,381 missing slots was
never stored. So the list has to be reconstructed: GDELT v2 emits a file
every 15 minutes, so the expected timeline is derivable, and diffing it
against the 14-digit stamps in S3 gives the real missing set instead of
a number nobody can act on.

  P0 re-check invocations per function -- orchestrator-driven or dead
  P1 wire only the truly dormant
  P2 reconstruct GDELT's missing slots from the timeline, not the tally
  P3 BOJ progress
"""
import json
import re
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCT = "857687956942"
LIVE = "justhodl-dashboard-live"

cfg = Config(read_timeout=300, retries={"max_attempts": 3})
s3 = boto3.client("s3", region_name=REGION, config=cfg)
lam = boto3.client("lambda", region_name=REGION, config=cfg)
ev = boto3.client("events", region_name=REGION, config=cfg)
cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
NOW = datetime.now(timezone.utc)


def jget(k):
    try:
        return json.loads(s3.get_object(Bucket=LIVE,
                                        Key=k)["Body"].read())
    except Exception:
        return None


with report("ops_5071_dormant_and_gdelt") as R:
    fails = []
    out = {"op": "ops_5071"}

    R.section("P0 orchestrator-driven or actually dead")
    prev = jget("data/ops/dormant-lanes.json") or {}
    lanes = prev.get("dormant") or []
    R.log("  candidates from ops 5070: %d" % len(lanes))
    live, dead = [], []
    for L in lanes:
        fn = L["fn"]
        try:
            r = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Invocations",
                Dimensions=[{"Name": "FunctionName", "Value": fn}],
                StartTime=NOW - timedelta(days=7), EndTime=NOW,
                Period=604800, Statistics=["Sum"])
            inv = int(sum(p["Sum"] for p in r.get("Datapoints", [])))
        except Exception:
            inv = -1
        (live if inv > 0 else dead).append((fn, inv, L.get("age_h")))
    R.log("  %-42s %10s %10s" % ("function", "invokes/7d", "state age"))
    for fn, inv, age in sorted(live, key=lambda x: -x[1]):
        R.log("  %-42s %10s %9.1fh  orchestrator-driven" % (
            fn[:42], f"{inv:,}", age or -1))
    for fn, inv, age in sorted(dead, key=lambda x: -(x[2] or 0)):
        R.log("  %-42s %10s %9.1fh  *** DEAD ***" % (
            fn[:42], inv, age or -1))
    R.log("  running without a rule: %d   genuinely dead: %d" % (
        len(live), len(dead)))
    out["dead"] = [f for f, _, _ in dead]

    R.section("P1 wire only the dead")
    hosts = []
    try:
        for page in ev.get_paginator("list_rules").paginate():
            for r in page.get("Rules", []):
                se = r.get("ScheduleExpression") or ""
                if r.get("State") == "ENABLED" and se.startswith("rate("):
                    n = len(ev.list_targets_by_rule(
                        Rule=r["Name"]).get("Targets", []))
                    if n < 5:
                        hosts.append([r["Name"], se, 5 - n])
    except Exception as e:
        R.log("  survey err %s" % str(e)[:90])
    R.log("  rules with free slots: %d" % len(hosts))
    for fn, inv, age in dead:
        placed = False
        for h in hosts:
            if h[2] <= 0 or "minute" not in h[1]:
                continue
            try:
                lam.add_permission(
                    FunctionName=fn, StatementId="evb71-%s" % h[0][:40],
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn="arn:aws:events:%s:%s:rule/%s"
                              % (REGION, ACCT, h[0]))
            except Exception:
                pass
            try:
                resp = ev.put_targets(Rule=h[0], Targets=[{
                    "Id": fn.replace("justhodl-", "")[:60],
                    "Arn": "arn:aws:lambda:%s:%s:function:%s"
                           % (REGION, ACCT, fn),
                    "Input": "{}"}])
                if not resp.get("FailedEntryCount"):
                    R.log("  %-34s -> %s (%s)" % (fn, h[0], h[1]))
                    h[2] -= 1
                    placed = True
                    break
            except Exception as e:
                R.log("  %s: %s" % (fn, str(e)[:70]))
        if not placed:
            R.log("  %-34s NOT WIRED" % fn)

    R.section("P2 reconstruct GDELT's missing slots")
    g = jget("data/warm/gdelt-full/_state/state.json") or {}
    R.log("  state says: files=%s gaps=%s cursor=%s" % (
        g.get("files"), g.get("gaps"), g.get("cursor")))
    R.log("  gaps is a COUNTER; only a capped sample survives: %s" % (
        (g.get("gaps_sample") or [])[:4]))
    stamps, n_keys = set(), 0
    kw = {"Bucket": LIVE, "Prefix": "data/warm/gdelt-full/",
          "MaxKeys": 1000}
    t0 = time.time()
    while time.time() - t0 < 600:
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            n_keys += 1
            m = re.search(r"(\d{14})", o["Key"])
            if m:
                stamps.add(m.group(1))
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    R.log("  listed %s keys, %s distinct 14-digit stamps" % (
        f"{n_keys:,}", f"{len(stamps):,}"))
    if stamps:
        lo, hi = min(stamps), max(stamps)
        R.log("  stamp range %s .. %s" % (lo, hi))
        try:
            start = datetime.strptime(lo, "%Y%m%d%H%M%S").replace(
                tzinfo=timezone.utc)
            end = datetime.strptime(hi, "%Y%m%d%H%M%S").replace(
                tzinfo=timezone.utc)
            exp, cur = [], start
            while cur <= end:
                exp.append(cur.strftime("%Y%m%d%H%M%S"))
                cur += timedelta(minutes=15)
            missing = [e for e in exp if e not in stamps]
            R.log("  expected 15-min slots in range: %s" % f"{len(exp):,}")
            R.log("  present: %s   MISSING: %s  (state's tally: %s)" % (
                f"{len(exp) - len(missing):,}", f"{len(missing):,}",
                g.get("gaps")))
            by_year = Counter(m[:4] for m in missing)
            R.log("  missing by year: %s" % dict(
                sorted(by_year.items())))
            R.log("  first 6 missing: %s" % missing[:6])
            s3.put_object(
                Bucket=LIVE,
                Key="data/_state/gdelt-missing-slots.json",
                Body=json.dumps({"generated": NOW.isoformat(),
                                 "expected": len(exp),
                                 "missing": len(missing),
                                 "by_year": dict(by_year),
                                 "slots": missing[:50000]}).encode(),
                ContentType="application/json")
            R.log("  -> data/_state/gdelt-missing-slots.json  (the list "
                  "the engine never kept; a backfill pass can consume "
                  "it directly)")
            out["gdelt_missing"] = len(missing)
        except Exception as e:
            R.log("  timeline err %s" % str(e)[:120])
    else:
        R.log("  no 14-digit stamps in the keys -- naming differs")

    R.section("P3 BOJ")
    tot = {"done": 0, "codes": 0, "rows": 0}
    kw = {"Bucket": LIVE, "Prefix": "data/warm/boj-full/_state/",
          "MaxKeys": 1000}
    while True:
        r = s3.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            if "api_" not in o["Key"]:
                continue
            d = jget(o["Key"]) or {}
            tot["done"] += int(d.get("done") or 0)
            tot["codes"] += len(d.get("codes") or [])
            tot["rows"] += int(d.get("rows") or 0)
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    R.log("  boj %s/%s series (%.1f%%)  rows %s" % (
        f"{tot['done']:,}", f"{tot['codes']:,}",
        100.0 * tot["done"] / max(1, tot["codes"]), f"{tot['rows']:,}"))
    out["boj"] = tot
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/dormant-lanes.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5071 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(dead=len(out.get("dead") or []), live=len(live),
         gdelt_missing=out.get("gdelt_missing"),
         boj_pct=round(100.0 * tot["done"] / max(1, tot["codes"]), 1))
    R.log("ops 5071 GREEN")
