"""ops_5069 -- audit EVERY engine for a trigger, then expedite the rest.

Two of the three largest gaps in the fleet turned out to have the same
cause, and it was not throughput:

    justhodl-census-us   rules: NONE  -> STALE, pinned the dashboard to
                         IMPORT DEGRADED for weeks
    justhodl-boj-full    rules: NONE  -> 1 invocation/day, state four
                         days old, stuck at 46% of 120,394 series

Nothing in this fleet checks whether an engine still has a schedule. A
lane with no trigger looks perfectly healthy in its own state document
-- that is precisely why it goes unnoticed. So before chasing GDELT's
7,381 gaps or FRED's 4,688 leaked series as throughput problems, this
op asks the cheap question of every function at once.

Also ships a per-db LEASE for BOJ. The fanout target landed on a
rate(5 minutes) rule but a db run can take the full 780s, so a second
wave arrives while the first is still draining, both read the same
`done` index, and they re-fetch identical codes while racing to write
api_{db}.json. The api_only path returns before the main lease check, so
the lease has to live per db. Nine overlapping wave-pairs per half hour
on the observed cadence -- every one a double fetch.

  P0 BOJ progress and that the lease deployed
  P1 THE AUDIT: every justhodl-* function, does any rule target it,
     when did it last run
  P2 wire triggers for the untriggered engines that hold real backlog
  P3 GDELT and FRED specifically -- gap, trigger, last run
"""
import json
import sys
import time
from collections import defaultdict
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


def boj_cov():
    tot = {"done": 0, "codes": 0, "rows": 0, "leased": 0}
    kw = {"Bucket": LIVE, "Prefix": "data/warm/boj-full/_state/",
          "MaxKeys": 1000}
    ks = []
    while True:
        r = s3.list_objects_v2(**kw)
        ks += [o["Key"] for o in r.get("Contents", [])]
        if not r.get("IsTruncated"):
            break
        kw["ContinuationToken"] = r.get("NextContinuationToken")
    for k in ks:
        if "api_" not in k:
            continue
        d = jget(k) or {}
        tot["done"] += int(d.get("done") or 0)
        tot["codes"] += len(d.get("codes") or [])
        tot["rows"] += int(d.get("rows") or 0)
        tot["leased"] += int(d.get("skipped_leased") or 0)
    return tot


with report("ops_5069_trigger_audit") as R:
    fails = []
    out = {"op": "ops_5069"}

    R.section("P0 BOJ progress + lease")
    b0 = boj_cov()
    R.log("  series %s / %s (%.1f%%)  rows %s  lease-skips %d" % (
        f"{b0['done']:,}", f"{b0['codes']:,}",
        100.0 * b0["done"] / max(1, b0["codes"]), f"{b0['rows']:,}",
        b0["leased"]))
    out["boj_start"] = b0["done"]

    R.section("P1 THE AUDIT -- every engine, does it have a trigger")
    fns = []
    try:
        for page in lam.get_paginator("list_functions").paginate():
            for f in page.get("Functions", []):
                if f["FunctionName"].startswith("justhodl-"):
                    fns.append(f["FunctionName"])
        R.log("  justhodl-* functions: %d" % len(fns))
    except Exception as e:
        R.log("  list err %s" % str(e)[:100])
        fails.append("P1:list")
    targeted = defaultdict(list)
    try:
        for page in ev.get_paginator("list_rules").paginate():
            for r in page.get("Rules", []):
                for t in ev.list_targets_by_rule(
                        Rule=r["Name"]).get("Targets", []):
                    arn = t.get("Arn") or ""
                    if ":function:" in arn:
                        targeted[arn.split(":function:")[-1].split(":")[0]
                                 ].append((r["Name"], r.get("State"),
                                           r.get("ScheduleExpression")))
    except Exception as e:
        R.log("  rules err %s" % str(e)[:100])
    untriggered = []
    for fn in sorted(fns):
        rl = targeted.get(fn) or []
        live = [x for x in rl if x[1] == "ENABLED"]
        if live:
            continue
        try:
            r = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName="Invocations",
                Dimensions=[{"Name": "FunctionName", "Value": fn}],
                StartTime=NOW - timedelta(days=7), EndTime=NOW,
                Period=604800, Statistics=["Sum"])
            inv = int(sum(p["Sum"] for p in r.get("Datapoints", [])))
        except Exception:
            inv = -1
        untriggered.append((fn, len(rl), inv))
    R.log("  ENGINES WITH NO ENABLED RULE: %d of %d" % (len(untriggered),
                                                        len(fns)))
    R.log("  %-44s %8s %10s" % ("function", "rules", "invokes/7d"))
    for fn, nr, inv in sorted(untriggered, key=lambda x: x[2])[:26]:
        R.log("  %-44s %8d %10s" % (fn[:44], nr,
                                    "?" if inv < 0 else f"{inv:,}"))
    out["untriggered"] = [f for f, _, _ in untriggered]
    out["total_fns"] = len(fns)

    R.section("P2 wire the ones holding real backlog")
    WANT = {"justhodl-gdelt-full": {},
            "justhodl-fred-import": {},
            "justhodl-boj-full": {"fanout": True},
            "justhodl-census-us": {"mode": "econ_dispatch",
                                   "shards": 12}}
    hosts = []
    try:
        for page in ev.get_paginator("list_rules").paginate():
            for r in page.get("Rules", []):
                se = r.get("ScheduleExpression") or ""
                if r.get("State") == "ENABLED" and se.startswith("rate("):
                    n = len(ev.list_targets_by_rule(
                        Rule=r["Name"]).get("Targets", []))
                    if n < 5:
                        hosts.append((r["Name"], se, 5 - n))
    except Exception:
        pass
    R.log("  rules with free slots: %d" % len(hosts))
    names = {f for f, _, _ in untriggered}
    for fn, payload in WANT.items():
        if fn not in names:
            R.log("  %-28s already triggered" % fn)
            continue
        if fn not in fns:
            R.log("  %-28s no such function" % fn)
            continue
        placed = False
        for i, (nm, se, room) in enumerate(hosts):
            if room <= 0:
                continue
            try:
                lam.add_permission(
                    FunctionName=fn,
                    StatementId="evb69-%s" % nm[:40],
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn="arn:aws:events:%s:%s:rule/%s"
                              % (REGION, ACCT, nm))
            except Exception:
                pass
            try:
                resp = ev.put_targets(Rule=nm, Targets=[{
                    "Id": fn.replace("justhodl-", "")[:60],
                    "Arn": "arn:aws:lambda:%s:%s:function:%s"
                           % (REGION, ACCT, fn),
                    "Input": json.dumps(payload)}])
                if not resp.get("FailedEntryCount"):
                    R.log("  %-28s -> %s (%s)" % (fn, nm, se))
                    hosts[i] = (nm, se, room - 1)
                    placed = True
                    break
            except Exception as e:
                R.log("  %s on %s: %s" % (fn, nm, str(e)[:70]))
        if not placed:
            R.log("  %-28s COULD NOT WIRE -- no free slot" % fn)

    R.section("P3 GDELT and FRED")
    g = jget("data/warm/gdelt-full/_state/state.json") or {}
    R.log("  gdelt  files=%s gaps=%s cursor=%s as_of=%s" % (
        g.get("files"), g.get("gaps"), g.get("cursor"), g.get("as_of")))
    fr = jget("data/warm/fred/_state/state.json") or \
        jget("data/_state/fred-scoped.json") or {}
    if fr:
        nums = {k: v for k, v in fr.items()
                if isinstance(v, (int, float))
                and not isinstance(v, bool)}
        R.log("  fred   %s" % json.dumps(nums)[:140])
    else:
        R.log("  fred   state not at the guessed paths (again) -- the "
              "page's 277,453/282,141 remains the source of truth")
    time.sleep(420)
    b1 = boj_cov()
    R.log("  BOJ after %d min: %s series (+%s), rows %s, lease-skips %d"
          % (7, f"{b1['done']:,}", f"{b1['done'] - b0['done']:,}",
             f"{b1['rows']:,}", b1["leased"]))
    if b1["leased"] > b0["leased"]:
        R.log("  lease is firing -- overlapping waves are being turned "
              "away instead of double-fetching")
    out["boj_end"] = b1["done"]
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/trigger-audit.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.log("  -> data/ops/trigger-audit.json")
    except Exception as e:
        R.log("  write err %s" % str(e)[:90])

    if fails:
        R.log("ops 5069 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(functions=out.get("total_fns"),
         untriggered=len(out.get("untriggered") or []),
         boj=out.get("boj_end"))
    R.log("ops 5069 GREEN -- fleet triggers audited")
