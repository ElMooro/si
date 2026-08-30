"""ops_5056 -- the banner is right; census-us is the thing that is stale.

Khalid: "i dont think the update shows here yet."

It does. His own page reads Eurostat 1,136,599 keys / 564,204,235
series / 321,818.67 MB, ECB 7,237 keys / 3,240,832 series, headline
1,911,744 S3 KEYS / 530.15 GB. ECB no longer says "214 series".

What has NOT updated is the banner, and the banner is not stale -- it is
accurate. overall = the WORST pipeline status, and census-us is STALE,
which pins the whole line to DEGRADED. The sentinel rule is:

    phase == COMPLETE and age > 26h  ->  STALE

and justhodl-census-us calls save(state) at the end of EVERY run, which
stamps updated_at. So a stale updated_at does not mean the lane failed
-- it means the function is not being invoked at all. The provider card
showing "freshest 0.2h" is the mirror being refreshed by other engines,
not this walker running.

Two fixes ship here. The sentinel now ages incidents out after 14 days
(five entries from 2026-08-10 were still being counted as "5 incidents
logged" twenty days later; a log that only grows stops meaning
anything). And this op finds out why the walker is not running.

  P0 census-us: state, age, phase; the function's real invocation
     history; every EventBridge rule that targets it and whether it is
     enabled
  P1 kick it once and prove updated_at moves -- if it does, the lane is
     healthy and only the trigger is broken
  P2 re-run the sentinel and see whether overall leaves DEGRADED
  P3 Tier-1 v2 rebuild status; restore hourly cadence when done
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
CENSUS = "justhodl-census-us"
SENTINEL = "justhodl-import-sentinel"
CSTATE = "data/warm/census-us/_state/state.json"
RULE = "justhodl-series-extractor-5min"
FN = "justhodl-series-extractor"

cfg = Config(read_timeout=300, retries={"max_attempts": 3})
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


with report("ops_5056_census_and_banner") as R:
    fails = []
    out = {"op": "ops_5056"}

    R.section("P0 why is census-us STALE")
    cst = jget(CSTATE)
    ua = cst.get("updated_at")
    R.log("  state: phase=%s n_done=%s/%s rows=%s updated_at=%s" % (
        cst.get("phase"), cst.get("n_done"), cst.get("n_total"),
        f"{cst.get('rows_total') or 0:,}", ua))
    try:
        age_h = (NOW - datetime.fromisoformat(
            str(ua).replace("Z", "+00:00"))).total_seconds() / 3600
        R.log("  age = %.1f h  (sentinel calls COMPLETE stale past 26h)"
              % age_h)
        out["age_h"] = round(age_h, 1)
    except Exception:
        R.log("  age = unparseable")
    try:
        r = cw.get_metric_statistics(
            Namespace="AWS/Lambda", MetricName="Invocations",
            Dimensions=[{"Name": "FunctionName", "Value": CENSUS}],
            StartTime=NOW - timedelta(days=7), EndTime=NOW,
            Period=86400, Statistics=["Sum"])
        pts = sorted((p["Timestamp"].date().isoformat(), int(p["Sum"]))
                     for p in r.get("Datapoints", []))
        R.log("  invocations/day (7d): %s" % (
            " ".join("%s=%d" % (d[5:], v) for d, v in pts) or "NONE"))
        out["invocations_7d"] = sum(v for _, v in pts)
    except Exception as e:
        R.log("  metric err %s" % str(e)[:100])
    found = []
    try:
        for name in ev.list_rule_names_by_target(
                TargetArn="arn:aws:lambda:%s:857687956942:function:%s"
                          % (REGION, CENSUS)).get("RuleNames", []):
            d = ev.describe_rule(Name=name)
            found.append((name, d.get("State"),
                          d.get("ScheduleExpression")))
        R.log("  rules targeting %s: %s" % (CENSUS, found or "NONE"))
        if not found:
            R.log("  *** no EventBridge rule targets this function -- "
                  "that is why it never runs ***")
    except Exception as e:
        R.log("  rule lookup err %s" % str(e)[:120])
    out["rules"] = found

    R.section("P1 kick it and watch updated_at")
    try:
        lam.invoke(FunctionName=CENSUS, InvocationType="Event",
                   Payload=json.dumps({"kicked_by": "ops_5056"}).encode())
        R.log("  kicked")
    except Exception as e:
        R.log("  invoke err %s" % str(e)[:120])
        fails.append("P1:invoke")
    moved = False
    for i in range(18):
        time.sleep(50)
        n = jget(CSTATE)
        if n.get("updated_at") != ua:
            moved = True
            R.log("  updated_at moved after %ds: %s -> %s (phase=%s)" % (
                (i + 1) * 50, ua, n.get("updated_at"), n.get("phase")))
            break
    if not moved:
        R.log("  updated_at did NOT move -- the walker itself is failing, "
              "not just untriggered")
        fails.append("P1:nomove")
    out["kicked_ok"] = moved
    for rn, st_, sch in found:
        if st_ == "DISABLED":
            try:
                ev.enable_rule(Name=rn)
                R.log("  re-enabled rule %s (%s)" % (rn, sch))
            except Exception as e:
                R.log("  enable %s err %s" % (rn, str(e)[:90]))

    R.section("P2 re-run the sentinel")
    try:
        lam.invoke(FunctionName=SENTINEL, InvocationType="Event",
                   Payload=b"{}")
        time.sleep(75)
        h = jget("data/import-health.json")
        R.log("  overall=%s worst=%s incidents=%d generated_at=%s" % (
            h.get("overall"), h.get("worst"),
            len(h.get("incidents") or []), h.get("generated_at")))
        for p in (h.get("pipelines") or []):
            R.log("    %-18s %-8s %s" % (p.get("name"), p.get("status"),
                                         str(p.get("detail"))[:80]))
        out["overall"] = h.get("overall")
        out["worst"] = h.get("worst")
    except Exception as e:
        R.log("  sentinel err %s" % str(e)[:130])

    R.section("P3 Tier-1 v2 + cadence")
    for p in ("ecb", "eurostat"):
        t = jget("data/_state/t1-%s.json" % p)
        R.log("  %-9s t1 flows=%d left=%s entries=%s schema=%s %.2f GB"
              % (p, len(t.get("flows_done") or []),
                 t.get("candidates_left"), f"{t.get('entries') or 0:,}",
                 t.get("entry_schema"), (t.get("bytes") or 0) / 1e9))
        out.setdefault("t1", {})[p] = {
            "flows": len(t.get("flows_done") or []),
            "left": t.get("candidates_left"),
            "schema": t.get("entry_schema")}
    te = jget("data/_state/t1-eurostat.json")
    if te.get("candidates_left") == 0 and \
            int(te.get("entry_schema") or 1) >= 2:
        try:
            ev.put_rule(Name=RULE, ScheduleExpression="rate(1 hour)",
                        State="ENABLED")
            R.log("  v2 complete -> cadence rate(1 hour)")
        except Exception as e:
            R.log("  cadence err %s" % str(e)[:110])
    else:
        R.log("  v2 still building -- leaving the fast cadence")
    try:
        d = ev.describe_rule(Name=RULE)
        R.log("  extractor rule=%s targets=%d" % (
            d.get("ScheduleExpression"),
            len(ev.list_targets_by_rule(Rule=RULE).get("Targets", []))))
    except Exception:
        pass
    try:
        s3.put_object(Bucket=LIVE, Key="data/ops/census-banner.json",
                      Body=json.dumps(out, indent=1, default=str).encode(),
                      ContentType="application/json")
    except Exception:
        pass

    if fails:
        R.log("ops 5056 RED: " + "; ".join(fails))
        sys.exit(1)
    R.kv(census_age_h=out.get("age_h"), kicked_ok=out.get("kicked_ok"),
         overall=out.get("overall"), rules=len(found))
    R.log("ops 5056 GREEN")
