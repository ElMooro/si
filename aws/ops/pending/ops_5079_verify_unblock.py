"""ops_5079 -- verify the unblock cleared the collateral damage.

ops 5078 fixed the wall I built:

    justhodl-census-us  invocations 6h: 53   throttles 6h: 15,710
    reserved 1 -> 20, shards live 3/6 -> 12/12
    rate 0.37 -> 1.88 entries/min, 474 entries left (~4.2h)

A 300:1 refusal ratio on one function, 92% of the fleet's throttling,
because I fanned twelve shards at a slot that holds one. Peak concurrency
was 140 of 1000 -- the account was never the constraint.

The collateral matters as much as the lane. Those refusals saturated the
shared Invoke request rate, so unrelated calls were rejected too:
justhodl-import-sentinel has not produced a health document in a DAY
(the page still shows the 2026-08-30T15:45 sweep), which is why the
dead-lanes chip appeared "absent" -- the check is in the deployed
package, verified by downloading and grepping the zip, it simply never
ran. And census-us's own timeseries walker could not get an invoke
through, which is why the STALE chip has held the banner at
IMPORT DEGRADED.

Both should now be reachable. This checks rather than assumes.

  P0 throttles since the fix, not across it
  P1 sentinel: does it run, does the chip appear, does the banner move
  P2 census-us timeseries: does the state finally advance
  P3 the lanes, with fresh ETAs
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
SENT = "justhodl-import-sentinel"
CST = "data/warm/census-us/_state/state.json"

cfg = Config(read_timeout=600, retries={"max_attempts": 5,
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


def met(name, fn=None, hours=1, stat="Sum"):
    d = [{"Name": "FunctionName", "Value": fn}] if fn else []
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName=name, Dimensions=d,
            StartTime=NOW - timedelta(hours=hours), EndTime=NOW,
            Period=3600, Statistics=[stat])
        return int(sum(p[stat] for p in r.get("Datapoints", [])))
    except Exception:
        return -1


with report("ops_5079_verify_unblock") as R:
    fails = []
    out = {"op": "ops_5079"}

    R.section("P0 throttles SINCE the fix (17:00Z)")
    for h in (1, 2, 3):
        R.log("  last %dh -- census-us throttles=%s invocations=%s | "
              "fleet throttles=%s" % (
                  h, f"{met('Throttles', FN, h):,}",
                  f"{met('Invocations', FN, h):,}",
                  f"{met('Throttles', None, h):,}"))
    t1 = met("Throttles", FN, 1)
    i1 = met("Invocations", FN, 1)
    R.log("  ratio now %s refused per success (was 300:1)" % (
        "%.2f" % (t1 / max(1, i1))))
    out.update(throttles_1h=t1, invocations_1h=i1)
    if t1 > i1:
        R.log("  still refusing more than it runs -- 20 may be too few "
              "for 12 shards plus retries in flight")

    R.section("P1 the sentinel")
    h0 = jget("data/import-health.json") or {}
    R.log("  health doc generated_at=%s (the page showed this as a day "
          "old)" % h0.get("generated_at"))
    ok = False
    for a in range(4):
        try:
            lam.invoke(FunctionName=SENT, InvocationType="Event",
                       Payload=b"{}")
            R.log("  invoke accepted (attempt %d)" % (a + 1))
            ok = True
            break
        except Exception as e:
            R.log("  refused %d: %s" % (a + 1, str(e)[:80]))
            time.sleep(20)
    h = h0
    for i in range(16):
        time.sleep(30)
        h = jget("data/import-health.json") or {}
        if h.get("generated_at") != h0.get("generated_at"):
            R.log("  health doc REWRITTEN after %ds -> %s" % (
                (i + 1) * 30, h.get("generated_at")))
            break
    if h.get("generated_at") == h0.get("generated_at"):
        R.log("  sentinel STILL not producing output")
        fails.append("P1:nosweep")
    dl = next((p for p in (h.get("pipelines") or [])
               if p.get("name") == "dead-lanes"), None)
    if dl:
        R.log("  dead-lanes chip: %s" % dl.get("status"))
        R.log("  %s" % str(dl.get("detail"))[:240])
        out["dead_lanes"] = dl.get("status")
    else:
        R.log("  dead-lanes chip absent even on a fresh sweep -- then it "
              "IS a code path, not the throttling")
        fails.append("P1:nochip")
    R.log("  overall=%s worst=%s incidents=%d" % (
        h.get("overall"), h.get("worst"), len(h.get("incidents") or [])))
    out["overall"] = h.get("overall")

    R.section("P2 census-us timeseries")
    c0 = jget(CST) or {}
    R.log("  before updated_at=%s phase=%s" % (c0.get("updated_at"),
                                               c0.get("phase")))
    for a in range(4):
        try:
            lam.invoke(FunctionName=FN, InvocationType="Event",
                       Payload=b"{}")
            R.log("  timeseries invoke accepted (attempt %d)" % (a + 1))
            break
        except Exception as e:
            R.log("  refused %d: %s" % (a + 1, str(e)[:80]))
            time.sleep(20)
    moved = False
    for i in range(14):
        time.sleep(45)
        c1 = jget(CST) or {}
        if c1.get("updated_at") != c0.get("updated_at"):
            moved = True
            R.log("  MOVED -> %s  phase=%s rows=%s" % (
                c1.get("updated_at"), c1.get("phase"),
                f"{c1.get('rows_total') or 0:,}"))
            R.log("  the walker was never broken -- it could not get an "
                  "invoke through the storm I created")
            break
    if not moved:
        R.log("  still unmoved after 10 min")
        fails.append("P2:nomove")
    out["census_moved"] = moved

    R.section("P3 the lanes")
    ce = sum(len((jget("data/_state/census-econ-s%d.json" % k) or {})
                 .get("done") or []) for k in range(12))
    R.log("  census-econ %s / 1,226 entries" % f"{ce:,}")
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
    R.log("  BOJ %s / %s series (%.1f%%) · %s rows" % (
        f"{tot['done']:,}", f"{tot['codes']:,}",
        100.0 * tot["done"] / max(1, tot["codes"]), f"{tot['rows']:,}"))
    n = 0
    kw = {"Bucket": LIVE, "Prefix": "data/warm/census-econ/",
          "MaxKeys": 1000}
    while True:
        rr = s3.list_objects_v2(**kw)
        n += len(rr.get("Contents", []))
        if not rr.get("IsTruncated"):
            break
        kw["ContinuationToken"] = rr.get("NextContinuationToken")
    R.log("  census-econ objects in S3: %s" % f"{n:,}")
    out.update(census_econ=ce, boj=tot["done"], econ_objects=n)
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/verify-unblock.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5079 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(throttles_1h=out.get("throttles_1h"),
         overall=out.get("overall"), dead_lanes=out.get("dead_lanes"),
         census_moved=out.get("census_moved"),
         census_econ=out.get("census_econ"), boj=out.get("boj"))
    R.log("ops 5079 GREEN -- collateral cleared")
