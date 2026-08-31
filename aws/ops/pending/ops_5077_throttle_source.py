"""ops_5077 -- the throttling is ours, not AWS's.

ops 5076 measured the ceiling and the numbers rule out the obvious
explanation:

    ConcurrentExecutions limit 1000, unreserved 783
    peak concurrency actually used: 15-29
    Throttles ~2,900/h against Invocations ~1,350/h

Twice as many refusals as successes while using 3% of the limit. An
account out of capacity looks nothing like that. A function pinned to
reserved concurrency 1 and driven by minute-cadence targets looks
exactly like that: every tick arriving during a run is refused, async
delivery retries it, and the retries pile up into tens of thousands of
throttle events -- which then spill onto the shared Invoke request rate
and refuse unrelated calls like the census-us one I misread as a broken
walker.

justhodl-series-extractor is the prime suspect. It carries reserved
concurrency 1 deliberately -- that interlock is what makes two runs
sharing one page counter impossible -- and it was left on a 1-2 minute
cadence for the Tier-1 build with four targets on it. The interlock is
right; the cadence behind it is not, and I set it.

  P0 per-function throttles: name the source instead of guessing
  P1 fix the cadence; Tier-1 status decides whether it can stand down
  P2 re-measure throttles
  P3 census-us invoke, now that the Invoke rate should be free
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
EXT = "justhodl-series-extractor"
RULE = "justhodl-series-extractor-5min"

cfg = Config(read_timeout=600, retries={"max_attempts": 6,
                                        "mode": "adaptive"})
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


def throttles(fn=None, hours=6):
    dims = [{"Name": "FunctionName", "Value": fn}] if fn else []
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Throttles",
            Dimensions=dims, StartTime=NOW - timedelta(hours=hours),
            EndTime=NOW, Period=3600, Statistics=["Sum"])
        return int(sum(p["Sum"] for p in r.get("Datapoints", [])))
    except Exception:
        return -1


with report("ops_5077_throttle_source") as R:
    fails = []
    out = {"op": "ops_5077"}

    R.section("P0 name the source")
    fleet = throttles(None, 6)
    R.log("  fleet throttles (6h): %s" % f"{fleet:,}")
    cands = []
    try:
        for page in lam.get_paginator("list_functions").paginate():
            for f in page.get("Functions", []):
                n = f["FunctionName"]
                if not n.startswith("justhodl-"):
                    continue
                try:
                    rc = lam.get_function_concurrency(FunctionName=n)
                    res = rc.get("ReservedConcurrentExecutions")
                except Exception:
                    res = None
                if res is not None:
                    cands.append((n, res))
        R.log("  functions WITH reserved concurrency: %d" % len(cands))
        for n, res in cands:
            t = throttles(n, 6)
            R.log("    %-44s reserved=%-4s throttles6h=%s" % (
                n[:44], res, f"{t:,}" if t >= 0 else "?"))
            out.setdefault("reserved", {})[n] = {"reserved": res,
                                                 "throttles6h": t}
    except Exception as e:
        R.log("  scan err %s" % str(e)[:120])
        fails.append("P0")
    worst = max((v["throttles6h"], k) for k, v in
                (out.get("reserved") or {}).items()) if out.get(
                    "reserved") else (0, None)
    R.log("  worst offender: %s with %s throttles in 6h" % (
        worst[1], f"{worst[0]:,}"))
    R.log("  fleet total %s -- so this one function is %.0f%% of it" % (
        f"{fleet:,}", 100.0 * worst[0] / max(1, fleet)))

    R.section("P1 fix the cadence")
    t1e = jget("data/_state/t1-eurostat.json") or {}
    t1c = jget("data/_state/t1-ecb.json") or {}
    R.log("  tier1 eurostat: flows=%d left=%s schema=%s" % (
        len(t1e.get("flows_done") or []), t1e.get("candidates_left"),
        t1e.get("entry_schema")))
    R.log("  tier1 ecb     : flows=%d left=%s schema=%s" % (
        len(t1c.get("flows_done") or []), t1c.get("candidates_left"),
        t1c.get("entry_schema")))
    done = (t1e.get("candidates_left") == 0)
    try:
        d0 = ev.describe_rule(Name=RULE)
        tg = ev.list_targets_by_rule(Rule=RULE).get("Targets", [])
        R.log("  rule now: %s with %d targets %s" % (
            d0.get("ScheduleExpression"), len(tg),
            [t.get("Id") for t in tg]))
        newsched = "rate(1 hour)" if done else "rate(20 minutes)"
        ev.put_rule(Name=RULE, ScheduleExpression=newsched,
                    State="ENABLED")
        R.log("  cadence %s -> %s%s" % (
            d0.get("ScheduleExpression"), newsched,
            "" if done else "  (tier1 still building, so slowed rather "
                            "than stopped)"))
        R.log("  with reserved concurrency 1 and runs up to 900s, a "
              "1-2 minute cadence guarantees refusals; %s leaves room "
              "for a run to finish before the next tick" % newsched)
        out["cadence"] = newsched
    except Exception as e:
        R.log("  rule err %s" % str(e)[:120])
        fails.append("P1")

    R.section("P2 re-measure")
    R.log("  waiting for the retry backlog to drain...")
    time.sleep(900)
    after = throttles(None, 1)
    wa = throttles(worst[1], 1) if worst[1] else -1
    R.log("  fleet throttles in the last hour: %s" % f"{after:,}")
    R.log("  %s in the last hour: %s" % (worst[1],
                                         f"{wa:,}" if wa >= 0 else "?"))
    inv = 0
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            StartTime=NOW - timedelta(hours=1), EndTime=NOW,
            Period=3600, Statistics=["Sum"])
        inv = int(sum(p["Sum"] for p in r.get("Datapoints", [])))
    except Exception:
        pass
    R.log("  invocations in the last hour: %s" % f"{inv:,}")
    out.update(throttles_after=after, invocations_after=inv)

    R.section("P3 census-us, with the Invoke rate freed")
    c0 = jget("data/warm/census-us/_state/state.json") or {}
    R.log("  before updated_at=%s" % c0.get("updated_at"))
    got = False
    for a in range(5):
        try:
            lam.invoke(FunctionName="justhodl-census-us",
                       InvocationType="Event", Payload=b"{}")
            R.log("  accepted on attempt %d" % (a + 1))
            got = True
            break
        except Exception as e:
            R.log("  refused %d: %s" % (a + 1, str(e)[:80]))
            time.sleep(15 * (a + 1))
    for i in range(10):
        time.sleep(45)
        c1 = jget("data/warm/census-us/_state/state.json") or {}
        if c1.get("updated_at") != c0.get("updated_at"):
            R.log("  MOVED -> %s : the walker was fine all along" %
                  c1.get("updated_at"))
            out["census_moved"] = True
            break
    else:
        R.log("  still unmoved (accepted=%s)" % got)
        out["census_moved"] = False
    h = jget("data/import-health.json") or {}
    dl = next((p for p in (h.get("pipelines") or [])
               if p.get("name") == "dead-lanes"), None)
    R.log("  dead-lanes chip: %s" % (dl.get("status") if dl
                                     else "still absent"))
    R.log("  overall=%s worst=%s" % (h.get("overall"), h.get("worst")))
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/throttle-source.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5077 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(worst=worst[1], throttles_after=out.get("throttles_after"),
         cadence=out.get("cadence"),
         census_moved=out.get("census_moved"))
    R.log("ops 5077 GREEN -- throttling traced to our own cadence")
