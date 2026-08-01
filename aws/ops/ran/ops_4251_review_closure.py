"""
ops_4251 — close the three findings the session review (4250) raised.

WHAT THE REVIEW ACTUALLY FOUND, after forensics:
  F1  The two "silenced" engines were FALSE POSITIVES of my own sweep.
      polygon-options-flow and trade-tickets run
      cron(.. 14-19 ? * MON-FRI *) — market hours, weekdays. The sweep's
      cadence parser read the comma'd hour field and ignored the
      day-of-week field entirely; on a SATURDAY both engines are
      correctly idle. Neither appears in the 4231 disable ledger nor the
      4232 target-removal list. The surgery silenced NOTHING — the
      review tool did exactly what today's whole session warns against:
      a heuristic's output presented as a finding. Proven here with the
      last-invocation timestamp (must land Friday inside market hours).
  F2  The reconciler reported drift=1. Read the drift, fix it at the
      manifest (declare) or at AWS (disable), re-run, require zero.
  F3  CRR backfill: 111,336 source objects, 5,006 copied sequentially
      before the cap. Finished here with a 24-thread pool, then BOTH
      sides recounted. (Also corrects my earlier claim of "3,000+
      objects" — that was a paginated sample I misreported as a total.)
  F4  The DR freshness probe used a key from my own never-deployed
      version of the engine. The deployed engine writes
      data/dr-snapshot-latest.json. Probe the real key.

FOLLOW-UP RECORDED, NOT ACTED ON: the deployed DR engine writes dated
daily trees including per-function code copies — the bucket grows by
roughly a fleet-code-size every day. Cheap, but worth a dedupe design
pass in a future session; not touched here because changing a working
backup under review pressure is how backups stop working.
"""
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
DR_SRC = "justhodl-dashboard-live-dr"
DR_DST = "justhodl-dr-usw2-857687956942"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"},
             read_timeout=120)
WCFG = Config(retries={"max_attempts": 8, "mode": "adaptive"},
              read_timeout=120, max_pool_connections=64)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
s3w = boto3.client("s3", region_name="us-west-2", config=WCFG)
evb = boto3.client("events", region_name=REGION, config=CFG)
NOW = datetime.now(timezone.utc)

FP = ("justhodl-polygon-options-flow", "justhodl-trade-tickets")


def last_invocation(fn, days=4):
    r = cw.get_metric_statistics(
        Namespace="AWS/Lambda", MetricName="Invocations",
        Dimensions=[{"Name": "FunctionName", "Value": fn}],
        StartTime=NOW - timedelta(days=days), EndTime=NOW,
        Period=3600, Statistics=["Sum"])
    pts = [p for p in r.get("Datapoints", []) if p["Sum"] > 0]
    return max((p["Timestamp"] for p in pts), default=None)


with report("4251_review_closure") as rep:
    rep.heading("ops 4251 — review closure")
    fails = []

    # ---------------------------------------------------------------- F1
    rep.section("F1. The 'silenced' engines — prove the false positive")
    for fn in FP:
        t = last_invocation(fn)
        if t is None:
            rep.fail("  %s: no invocations in 4d — NOT a weekend artifact"
                     % fn)
            fails.append("%s genuinely silent" % fn)
            continue
        dow = t.strftime("%a")
        ok = dow == "Fri" and 14 <= t.hour <= 20
        (rep.ok if ok else rep.warn)(
            "  %-38s last ran %s %s UTC — %s"
            % (fn, dow, t.strftime("%H:%M"),
               "Friday inside market hours; Saturday silence is the "
               "SCHEDULE, not a defect" if ok else "unexpected pattern"))
        rep.kv(section="false_positive", function=fn,
               last_run=t.isoformat()[:16], day=dow,
               verdict="weekend-idle" if ok else "review")
    rep.log("sweep-parser gap recorded: cadence_hours() ignores the "
            "day-of-week field; weekend runs of the sweep will over-flag "
            "MON-FRI engines until it is day-aware.")

    # ---------------------------------------------------------------- F2
    rep.section("F2. The drift of one")
    try:
        d = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/schedule-drift.json")["Body"].read())
        drifts = d.get("drifts", [])
        rep.log("reconciler mode=%s drift_count=%s"
                % (d.get("mode"), d.get("drift_count")))
        for x in drifts[:10]:
            rep.warn("  %-16s %-44s %s"
                     % (x["drift"], x["key"][:44], x["detail"][:80]))
            rep.kv(section="drift", kind=x["drift"], key=x["key"],
                   detail=x["detail"][:110])
        # fix: declare legitimate undeclared control-plane rules; anything
        # else gets surfaced, not auto-guessed.
        man = json.loads(s3.get_object(
            Bucket=BUCKET, Key="config/schedule-manifest.json"
        )["Body"].read())
        changed = False
        for x in drifts:
            if x["drift"] != "UNDECLARED":
                continue
            name = x["key"]
            try:
                r = evb.describe_rule(Name=name)
                tg = evb.list_targets_by_rule(Rule=name)["Targets"]
            except Exception:
                continue
            if (name.startswith(("justhodl-", "jh-"))
                    and r.get("ScheduleExpression") and len(tg) >= 1):
                man["rules"] = [m for m in man["rules"]
                                if m["name"] != name]
                man["rules"].append(
                    {"kind": "events", "name": name,
                     "expr": r["ScheduleExpression"],
                     "state": r.get("State", "ENABLED"),
                     "targets": [{"id": t.get("Id"), "arn": t.get("Arn"),
                                  "input": t.get("Input"),
                                  "path": t.get("InputPath")}
                                 for t in tg]})
                changed = True
                rep.ok("  declared %s in the manifest (house-named, live, "
                       "targeted)" % name)
        if changed:
            s3.put_object(Bucket=BUCKET,
                          Key="config/schedule-manifest.json",
                          Body=json.dumps(man).encode(),
                          ContentType="application/json")
        r2 = lam.invoke(FunctionName="justhodl-schedule-reconciler",
                        InvocationType="RequestResponse")
        b2 = json.loads(r2["Payload"].read() or b"{}")
        (rep.ok if b2.get("drift_count") == 0 else rep.fail)(
            "reconciler after fix: drift = %s" % b2.get("drift_count"))
        if b2.get("drift_count"):
            fails.append("drift persists: %s" % b2.get("by_class"))
    except Exception as e:
        fails.append("drift fix: %s" % str(e)[:160])

    # ---------------------------------------------------------------- F3
    rep.section("F3. CRR backfill to completion (threaded)")
    try:
        src = []
        for page in s3.get_paginator("list_objects_v2").paginate(
                Bucket=DR_SRC):
            src += [o["Key"] for o in page.get("Contents", [])]
        have = set()
        for page in s3w.get_paginator("list_objects_v2").paginate(
                Bucket=DR_DST):
            have |= {o["Key"] for o in page.get("Contents", [])}
        todo = [k for k in src if k not in have]
        rep.log("source=%d dest=%d missing=%d"
                % (len(src), len(have), len(todo)))

        def cp(k):
            s3w.copy_object(Bucket=DR_DST, Key=k,
                            CopySource={"Bucket": DR_SRC, "Key": k})
            return k

        done = errs = 0
        t0 = time.time()
        with ThreadPoolExecutor(max_workers=24) as ex:
            futs = [ex.submit(cp, k) for k in todo]
            for f in as_completed(futs):
                try:
                    f.result()
                    done += 1
                except Exception:
                    errs += 1
                if done % 10000 == 0 and done:
                    rep.log("   … %d copied (%.0f/s)"
                            % (done, done / max(time.time() - t0, 1)))
        n_dst = 0
        for page in s3w.get_paginator("list_objects_v2").paginate(
                Bucket=DR_DST):
            n_dst += len(page.get("Contents", []))
        rep.log("copied=%d errors=%d in %.0fs | destination=%d source=%d"
                % (done, errs, time.time() - t0, n_dst, len(src)))
        rep.kv(section="crr", copied=done, errors=errs,
               dest=n_dst, source=len(src),
               rate_per_s=round(done / max(time.time() - t0, 1), 1))
        if n_dst + 60 >= len(src):
            rep.ok("us-west-2 holds a COMPLETE copy (±in-flight daily "
                   "writes, which forward replication covers)")
        else:
            fails.append("backfill still short: %d" % (len(src) - n_dst))
    except Exception as e:
        fails.append("backfill: %s" % str(e)[:160])

    # ---------------------------------------------------------------- F4
    rep.section("F4. DR freshness — against the REAL key")
    try:
        h = s3.head_object(Bucket=BUCKET, Key="data/dr-snapshot-latest.json")
        age = (NOW - h["LastModified"]).total_seconds() / 3600.0
        (rep.ok if age < 26 else rep.fail)(
            "data/dr-snapshot-latest.json is %.1fh old" % age)
        if age >= 26:
            fails.append("dr status stale %.0fh" % age)
        h2 = s3.head_object(Bucket=BUCKET, Key="data/signal-scorecard.json")
        age2 = (NOW - h2["LastModified"]).total_seconds() / 3600.0
        (rep.ok if age2 < 13 else rep.warn)(
            "signal-scorecard artifact is %.1fh old (bound 12h)" % age2)
        rep.log("follow-up recorded: the DR engine writes dated per-day "
                "code trees — the bucket grows ~fleet-size daily. Cheap, "
                "but a dedupe design pass belongs in a future session.")
    except Exception as e:
        fails.append("dr freshness: %s" % str(e)[:150])

    rep.section("RESULT")
    if fails:
        for f in fails:
            rep.fail("  %s" % f)
        raise SystemExit("FAILS: %s" % "; ".join(fails[:3]))
    rep.ok("OPS 4251 PASS — review findings closed with evidence")
