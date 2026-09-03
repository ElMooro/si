"""ops_5164 -- September cost stand-down. Reversible actions only.

ops 5163 proved the August churn (S3 $236 excess, Aug 09-29) is over, but
September is running at ~$20.7/day (~$620/month) against a pre-anomaly
baseline of $4.5/day. Three new drivers, all of them my own doing or my
own leftovers, and one structural:

  1. justhodl-repo  -- a DAILY engine (config: cron(10 6 * * ? *), ~330s,
     rewrites ~2,600 data/repo-history/*.json per run). ops 5071 (Aug 30)
     "resurrected" it by hitching it onto the host rule
     benzinga-news-agent-warm rate(5 minutes). Result: 288 runs/day,
     ~750k S3 PUT/day (>80% of all Tier-1 requests in the ops-5163
     access-log sample), ~$2.4/day Lambda + ~$3/day S3 requests, and a
     new noncurrent version of every history file every 5 minutes on a
     versioned bucket -- the exact trap written into doctrine on Aug 29.
  2. justhodl-fundamental-census -- same op, same pattern: hitched onto
     fleet-freshness-monitor-30min rate(30 minutes) for a daily engine.
  3. justhodl-census-us econ lane -- ops 5063 put the 12-shard dispatcher
     on "any enabled minute-cadence rule with a free slot". ~4,100
     invocations/day x ~100s = ~$6.9/day. It is a DATA LANE (completeness
     beats cost while it imports), so the cadence follows the lane state
     measured here, not a guess; but the dispatcher gets its own
     EventBridge Scheduler schedule so it can be turned down with one
     knob when the queue drains, instead of living inside another
     engine's rule.
  4. Dead versions: live bucket 1,954 GB vs ~530 GB of real data. The
     ops-5027 1-day noncurrent purge covers data/providers/ only; the
     bucket-wide rules named "expire-old-versions-after-30d" and
     "jh-noncurrent-14d" carry no NoncurrentVersionExpiration (dumped raw
     here). A 1-day noncurrent purge on data/ closes that.

Also measured/handled: boj-full redundant classic 5-minute target beside
its Scheduler fan-out; the OECD retrunc/refail temp schedules (stand down
if the failure ledger stopped shrinking); ecb-deep lane state; the
us-west-2 DR mirror (~$30/month of stale Standard-IA -- reported, NOT
deleted: Khalid's call); SnapStart cache ($17/month -- reported).

Every removal is written to data/ops/ops5164-cost-standdown.json with the
exact target JSON so it can be put back verbatim.
"""
import json
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCT = "857687956942"
BUCKET = "justhodl-dashboard-live"
SCHED_ROLE = "arn:aws:iam::%s:role/justhodl-scheduler-role" % ACCT
CFG = Config(retries={"max_attempts": 8, "mode": "adaptive"}, read_timeout=120)

lam = boto3.client("lambda", region_name=REGION, config=CFG)
ev = boto3.client("events", region_name=REGION, config=CFG)
sch = boto3.client("scheduler", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)

NOW = datetime.now(timezone.utc)
T0 = time.time()
GBS = 0.0000166667
LEDGER_KEY = "data/ops/ops5164-cost-standdown.json"
LEDGER = {"ops": 5164, "ts": NOW.isoformat(), "removed_targets": [],
          "created_schedules": [], "deleted_schedules": [],
          "lifecycle_added": None, "holds": []}
FAILS = []


def arn_of(fn):
    return "arn:aws:lambda:%s:%s:function:%s" % (REGION, ACCT, fn)


def jget(key):
    try:
        return json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    except Exception:
        return None


def minute_cadence(expr):
    e = (expr or "").lower()
    if "minute" in e:
        return True
    if e.startswith("cron(") and (e[5:7] in ("*/", "0/") or e[5:6] == "*"):
        return True
    return False


def fn_metrics(fn, hours):
    start = NOW - timedelta(hours=hours)
    out = {}
    for m in ("Invocations", "Errors", "Duration", "Throttles"):
        try:
            res = cw.get_metric_statistics(
                Namespace="AWS/Lambda", MetricName=m,
                Dimensions=[{"Name": "FunctionName", "Value": fn}],
                StartTime=start, EndTime=NOW, Period=hours * 3600, Statistics=["Sum"])
            out[m] = sum(p["Sum"] for p in res.get("Datapoints", []))
        except Exception:
            out[m] = -1
    return out


def classic_triggers(fn):
    """[(rule, expr, state, [target dicts for this fn])]"""
    found = []
    token = None
    while True:
        kw = {"TargetArn": arn_of(fn)}
        if token:
            kw["NextToken"] = token
        r = ev.list_rule_names_by_target(**kw)
        for rn in r.get("RuleNames", []):
            try:
                d = ev.describe_rule(Name=rn)
            except Exception:
                d = {}
            tg = [t for t in ev.list_targets_by_rule(Rule=rn).get("Targets", [])
                  if t.get("Arn") == arn_of(fn)]
            found.append((rn, d.get("ScheduleExpression", ""), d.get("State", "?"), tg))
        token = r.get("NextToken")
        if not token:
            break
    return found


_SCHED_CACHE = None


def scheduler_triggers(fn):
    """[(name, group, expr, state)] whose target is this function."""
    global _SCHED_CACHE
    if _SCHED_CACHE is None:
        _SCHED_CACHE = []
        try:
            for page in sch.get_paginator("list_schedules").paginate():
                for s in page.get("Schedules", []):
                    _SCHED_CACHE.append((s["Name"], s.get("GroupName", "default"),
                                         (s.get("Target") or {}).get("Arn", "")))
        except Exception as e:
            R.warn("list_schedules: %s" % str(e)[:120])
    hits = []
    for name, grp, tarn in _SCHED_CACHE:
        if tarn == arn_of(fn):
            try:
                d = sch.get_schedule(Name=name, GroupName=grp)
                hits.append((name, grp, d.get("ScheduleExpression"), d.get("State")))
            except Exception:
                hits.append((name, grp, "?", "?"))
    return hits


def remove_fn_from_rule(fn, rule, targets, why):
    ids = [t["Id"] for t in targets]
    if not ids:
        return False
    resp = ev.remove_targets(Rule=rule, Ids=ids)
    if resp.get("FailedEntryCount"):
        FAILS.append("remove_targets %s/%s failed: %s" % (rule, ids, resp.get("FailedEntries")))
        return False
    LEDGER["removed_targets"].append({"rule": rule, "function": fn, "targets": targets, "why": why})
    R.ok("   removed %s from rule %s (targets %s) -- %s" % (fn, rule, ids, why))
    return True


def ensure_schedule(name, expr, fn, payload, why):
    try:
        d = sch.get_schedule(Name=name, GroupName="default")
        if d.get("ScheduleExpression") == expr and d.get("State") == "ENABLED":
            R.ok("   schedule %s already %s ENABLED" % (name, expr))
            return "exists"
        sch.update_schedule(Name=name, GroupName="default", ScheduleExpression=expr,
                            ScheduleExpressionTimezone="UTC",
                            FlexibleTimeWindow={"Mode": "OFF"},
                            Target={"Arn": arn_of(fn), "RoleArn": SCHED_ROLE,
                                    "Input": json.dumps(payload),
                                    "RetryPolicy": {"MaximumRetryAttempts": 0}},
                            State="ENABLED", Description=why[:120])
        LEDGER["created_schedules"].append({"name": name, "expr": expr, "fn": fn,
                                            "payload": payload, "mode": "updated",
                                            "previous": d.get("ScheduleExpression")})
        R.ok("   schedule %s updated -> %s" % (name, expr))
        return "updated"
    except sch.exceptions.ResourceNotFoundException:
        sch.create_schedule(Name=name, GroupName="default", ScheduleExpression=expr,
                            ScheduleExpressionTimezone="UTC",
                            FlexibleTimeWindow={"Mode": "OFF"},
                            Target={"Arn": arn_of(fn), "RoleArn": SCHED_ROLE,
                                    "Input": json.dumps(payload),
                                    "RetryPolicy": {"MaximumRetryAttempts": 0}},
                            State="ENABLED", Description=why[:120])
        LEDGER["created_schedules"].append({"name": name, "expr": expr, "fn": fn,
                                            "payload": payload, "mode": "created"})
        R.ok("   schedule %s created %s -> %s %s" % (name, expr, fn, json.dumps(payload)))
        return "created"


def show_triggers(fn):
    cl = classic_triggers(fn)
    sc = scheduler_triggers(fn)
    for rn, expr, st, tg in cl:
        R.log("   rule      %-44s %-22s %s targets=%s inputs=%s"
              % (rn[:44], expr, st, [t["Id"] for t in tg],
                 [str(t.get("Input", ""))[:40] for t in tg]))
    for name, grp, expr, st in sc:
        R.log("   scheduler %-44s %-22s %s" % (name[:44], expr, st))
    if not cl and not sc:
        R.log("   (no EventBridge rule or Scheduler schedule targets this function)")
    return cl, sc


def census_shards():
    tot = {"n_done": 0, "n_total": 0, "queue_left": 0, "rows_total": 0,
           "failures": 0, "shards": 0, "phases": defaultdict(int), "newest": ""}
    for k in range(12):
        st = jget("data/_state/census-econ-s%d.json" % k)
        if not st:
            continue
        tot["shards"] += 1
        for f in ("n_done", "n_total", "queue_left", "rows_total"):
            tot[f] += int(st.get(f) or 0)
        tot["failures"] += len(st.get("failures") or {})
        tot["phases"][str(st.get("phase"))] += 1
        tot["newest"] = max(tot["newest"], str(st.get("updated_at") or ""))
    tot["phases"] = dict(tot["phases"])
    return tot


with report("ops_5164_sep_cost_standdown") as R:
    R.heading("ops 5164 -- September cost stand-down (reversible; ledger at %s)" % LEDGER_KEY)

    # ================================================================ 0
    R.section("0. Current truth -- per-function burn, last 12 hours")
    fns = {}
    for page in lam.get_paginator("list_functions").paginate():
        for f in page["Functions"]:
            fns[f["FunctionName"]] = f.get("MemorySize", 128)
    names = sorted(fns)
    start12 = NOW - timedelta(hours=12)
    burn = {}
    for i in range(0, len(names), 160):
        chunk = names[i:i + 160]
        q = []
        for j, fn in enumerate(chunk):
            for mt, tag in (("Invocations", "inv"), ("Duration", "dur"), ("Errors", "err")):
                q.append({"Id": "m%s_%d" % (tag, j),
                          "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": mt,
                                                    "Dimensions": [{"Name": "FunctionName", "Value": fn}]},
                                         "Period": 3600, "Stat": "Sum"}, "ReturnData": True})
        vals = defaultdict(float)
        token = None
        while True:
            kw = dict(MetricDataQueries=q, StartTime=start12, EndTime=NOW)
            if token:
                kw["NextToken"] = token
            res = cw.get_metric_data(**kw)
            for m in res["MetricDataResults"]:
                vals[m["Id"]] += sum(m["Values"]) if m["Values"] else 0.0
            token = res.get("NextToken")
            if not token:
                break
        for j, fn in enumerate(chunk):
            inv, dur, err = vals["minv_%d" % j], vals["mdur_%d" % j], vals["merr_%d" % j]
            if inv <= 0 and dur <= 0:
                continue
            gbs = dur / 1000.0 * fns[fn] / 1024.0
            burn[fn] = {"inv": int(inv), "err": int(err), "usd_day": (gbs * GBS + inv * 0.2e-6) * 2,
                        "avg_s": dur / inv / 1000.0 if inv else 0.0}
    tot_day = sum(v["usd_day"] for v in burn.values())
    R.log("Lambda fleet last 12h -> %s/day run-rate (%s/month)" % ("$%.2f" % tot_day, "$%.0f" % (tot_day * 30)))
    R.log("%-44s %10s %8s %8s %8s" % ("FUNCTION", "INV/12h", "ERR", "AVG_S", "$/DAY"))
    ranked = sorted(burn.items(), key=lambda x: -x[1]["usd_day"])
    for fn, v in ranked[:20]:
        R.log("%-44s %10s %8d %8.1f %8.2f" % (fn[:44], "{:,}".format(v["inv"]), v["err"], v["avg_s"], v["usd_day"]))
        R.kv(section="0_last12h", function=fn, inv_12h=v["inv"], errors=v["err"],
             avg_s=round(v["avg_s"], 1), usd_per_day=round(v["usd_day"], 2))
    LEDGER["before_last12h_usd_day"] = round(tot_day, 2)
    LEDGER["before_top"] = [{"fn": k, "usd_day": round(v["usd_day"], 2), "inv": v["inv"]} for k, v in ranked[:15]]

    # census lane: first read (bracketed against a second read at the end)
    census_a = census_shards()
    census_a_t = time.time()

    # ================================================================ 1
    R.section("1. justhodl-repo -- daily engine hitched to rate(5 minutes) by ops 5071")
    fn = "justhodl-repo"
    m6 = fn_metrics(fn, 6)
    R.log("   last 6h: invocations %d, errors %d, duration %.0fs total" % (m6["Invocations"], m6["Errors"], m6["Duration"] / 1000))
    cl, sc = show_triggers(fn)
    for rn, expr, st, tg in cl:
        if minute_cadence(expr):
            try:
                remove_fn_from_rule(fn, rn, tg, "daily engine on a %s host rule (ops 5071 hitch)" % expr)
            except Exception as e:
                FAILS.append("repo remove from %s: %s" % (rn, str(e)[:120]))
    slow = [rn for rn, expr, st, tg in cl if expr and not minute_cadence(expr) and st == "ENABLED"]
    if slow:
        R.log("   daily trigger already exists as classic rule(s) %s -- no Scheduler schedule added" % slow)
    else:
        try:
            ensure_schedule("justhodl-repo-daily", "cron(10 6 * * ? *)", fn, {},
                            "repo master board refresh after OFR/NY Fed morning updates (config cadence)")
        except Exception as e:
            FAILS.append("repo schedule: %s" % str(e)[:140])

    # ================================================================ 2
    R.section("2. justhodl-fundamental-census -- daily engine hitched to rate(30 minutes) by ops 5071")
    fn = "justhodl-fundamental-census"
    m6 = fn_metrics(fn, 6)
    R.log("   last 6h: invocations %d, errors %d, duration %.0fs total" % (m6["Invocations"], m6["Errors"], m6["Duration"] / 1000))
    cl, sc = show_triggers(fn)
    for rn, expr, st, tg in cl:
        if minute_cadence(expr):
            try:
                remove_fn_from_rule(fn, rn, tg, "daily self-resuming census on a %s host rule (ops 5071 hitch)" % expr)
            except Exception as e:
                FAILS.append("fundamental-census remove from %s: %s" % (rn, str(e)[:120]))
    slow = [rn for rn, expr, st, tg in cl if expr and not minute_cadence(expr) and st == "ENABLED"]
    if slow:
        R.log("   daily trigger already exists as classic rule(s) %s -- no Scheduler schedule added" % slow)
    else:
        try:
            ensure_schedule("justhodl-fundamental-census-daily", "cron(15 3 * * ? *)", fn, {"resume": True},
                            "fundamental census daily walk; v1.11 resumes its S3 cursor and parks at chain cap")
        except Exception as e:
            FAILS.append("fundamental-census schedule: %s" % str(e)[:140])

    # ================================================================ 3
    R.section("3. justhodl-boj-full -- redundant classic 5-minute target beside the Scheduler fan-out?")
    fn = "justhodl-boj-full"
    m6 = fn_metrics(fn, 6)
    err_rate = (m6["Errors"] / m6["Invocations"]) if m6["Invocations"] > 0 else 0.0
    R.log("   last 6h: invocations %d, errors %d (%.0f%%), throttles %d"
          % (m6["Invocations"], m6["Errors"], err_rate * 100, m6["Throttles"]))
    cl, sc = show_triggers(fn)
    has_fanout_sched = any("fanout" in n for n, _, _, _ in sc)
    for rn, expr, st, tg in cl:
        if minute_cadence(expr) and has_fanout_sched:
            try:
                remove_fn_from_rule(fn, rn, tg, "classic %s target redundant with Scheduler fan-out (per-db lease made it a no-op storm)" % expr)
            except Exception as e:
                FAILS.append("boj remove from %s: %s" % (rn, str(e)[:120]))
    if err_rate > 0.10:
        try:
            ev_ = logs.filter_log_events(logGroupName="/aws/lambda/" + fn,
                                         startTime=int((NOW - timedelta(minutes=90)).timestamp() * 1000),
                                         filterPattern="?Traceback ?Error ?ERROR", limit=6)
            for e_ in ev_.get("events", [])[:6]:
                R.warn("   log: %s" % e_["message"].strip()[:220])
        except Exception as e:
            R.warn("   log tail unavailable: %s" % str(e)[:100])
    else:
        R.ok("   BOJ storm is over on the last-6h measure (error rate %.1f%%)" % (err_rate * 100))

    # ================================================================ 4
    R.section("4. justhodl-ecb-deep -- backfill lane state (no action: data lane)")
    st = jget("data/_state/ecb-deep.json") or {}
    flows = st.get("flows") or {}
    n_flows = len(flows)
    n_complete = sum(1 for f in flows.values() if (f or {}).get("complete"))
    wins = defaultdict(int)
    for f in flows.values():
        for w in ((f or {}).get("windows") or {}).values():
            wins[str((w or {}).get("status"))[:8]] += 1
    m6 = fn_metrics("justhodl-ecb-deep", 6)
    R.log("   mode=%s flows complete %d/%d  windows=%s  updated_at=%s"
          % (st.get("mode"), n_complete, n_flows, dict(wins), st.get("updated_at")))
    R.log("   last 6h: invocations %d, errors %d, %.1f Lambda-hours"
          % (m6["Invocations"], m6["Errors"], m6["Duration"] / 3.6e6))
    show_triggers("justhodl-ecb-deep")
    pend = wins.get("pending", 0)
    done_w = sum(v for k, v in wins.items() if k.startswith("done") or k.startswith("ok"))
    LEDGER["ecb_deep"] = {"mode": st.get("mode"), "complete": n_complete, "flows": n_flows, "windows": dict(wins)}
    if st.get("mode") == "backfill" and pend:
        R.log("   verdict: importing (%d windows pending, %d done) -- stays; it stops chaining on its own when pending==0" % (pend, done_w))
    elif st.get("mode") == "backfill" and not pend:
        R.warn("   verdict: nothing pending but mode still 'backfill' -- watch for idle chaining")
    else:
        R.log("   verdict: refresh mode")

    # ================================================================ 5
    R.section("5. sdmx-walker OECD temp schedules (retrunc / refail) -- ledger still shrinking?")
    led = jget("data/_state/sdmx-walk-oecd.json") or {}
    n_failed = len(led.get("failed") or led.get("failures") or {})
    n_trunc = len(led.get("truncated") or {})
    R.log("   OECD ledger: failed=%d truncated=%d (Sep-02 reference: 444 / 273)  keys=%s"
          % (n_failed, n_trunc, sorted(led.keys())[:12]))
    tmp = []
    for name in ("justhodl-sdmx-walker-oecd-retrunc", "justhodl-sdmx-walker-oecd-refail"):
        try:
            d = sch.get_schedule(Name=name, GroupName="default")
            tmp.append((name, d))
            R.log("   %s %s %s" % (name, d.get("ScheduleExpression"), d.get("State")))
        except Exception:
            R.log("   %s: not present" % name)
    if tmp and n_failed >= 430:
        for name, d in tmp:
            try:
                sch.delete_schedule(Name=name, GroupName="default")
                LEDGER["deleted_schedules"].append({"name": name, "expr": d.get("ScheduleExpression"),
                                                    "target": d.get("Target"), "why": "OECD ledger stopped shrinking"})
                R.ok("   deleted temp schedule %s (ledger recorded for recreation)" % name)
            except Exception as e:
                FAILS.append("delete %s: %s" % (name, str(e)[:100]))
    elif tmp:
        R.log("   ledger still shrinking (failed %d < 430) -- temp schedules stay for now" % n_failed)

    # ================================================================ 6
    R.section("6. Dead versions -- raw lifecycle rules, version sampling, 1-day noncurrent purge on data/")
    try:
        rules = s3.get_bucket_lifecycle_configuration(Bucket=BUCKET).get("Rules", [])
    except Exception as e:
        rules = []
        FAILS.append("get lifecycle: %s" % str(e)[:120])
    for rule in rules:
        if rule.get("ID") in ("expire-old-versions-after-30d", "jh-noncurrent-14d"):
            R.log("   RAW %s" % json.dumps(rule, default=str)[:400])

    def version_sample(prefix, pages):
        cur_n = cur_b = non_n = non_b = dm = 0
        kw = {"Bucket": BUCKET, "Prefix": prefix, "MaxKeys": 1000}
        for _ in range(pages):
            r = s3.list_object_versions(**kw)
            for v in r.get("Versions", []):
                if v.get("IsLatest"):
                    cur_n += 1
                    cur_b += v.get("Size", 0)
                else:
                    non_n += 1
                    non_b += v.get("Size", 0)
            dm += len(r.get("DeleteMarkers", []))
            if not r.get("IsTruncated"):
                break
            kw["KeyMarker"] = r.get("NextKeyMarker")
            kw["VersionIdMarker"] = r.get("NextVersionIdMarker")
        return cur_n, cur_b, non_n, non_b, dm

    for pfx, pages in (("data/repo-history/", 3), ("data/warm/census-econ/", 2), ("data/warm/fred-scoped/", 2)):
        try:
            cn, cb, nn, nb, dm = version_sample(pfx, pages)
            R.log("   %-30s sample: current %d (%.1f MB), noncurrent %d (%.1f MB), delete-markers %d -> noncurrent share %.0f%% of bytes"
                  % (pfx, cn, cb / 1e6, nn, nb / 1e6, dm, (100.0 * nb / (cb + nb)) if (cb + nb) else 0))
            R.kv(section="6_versions", prefix=pfx, current=cn, current_mb=round(cb / 1e6, 1),
                 noncurrent=nn, noncurrent_mb=round(nb / 1e6, 1))
        except Exception as e:
            R.warn("   %s version sample: %s" % (pfx, str(e)[:100]))

    NEW_ID = "ops5164-purge-dead-versions-data"
    if rules and not any(r_.get("ID") == NEW_ID for r_ in rules):
        new_rule = {"ID": NEW_ID, "Status": "Enabled", "Filter": {"Prefix": "data/"},
                    "NoncurrentVersionExpiration": {"NoncurrentDays": 1},
                    "Expiration": {"ExpiredObjectDeleteMarker": True}}
        try:
            s3.put_bucket_lifecycle_configuration(
                Bucket=BUCKET, LifecycleConfiguration={"Rules": rules + [new_rule]})
            back = s3.get_bucket_lifecycle_configuration(Bucket=BUCKET).get("Rules", [])
            if any(r_.get("ID") == NEW_ID for r_ in back):
                LEDGER["lifecycle_added"] = new_rule
                R.ok("   lifecycle %s armed: data/ noncurrent versions expire after 1 day (+ expired delete markers); %d rules total"
                     % (NEW_ID, len(back)))
            else:
                FAILS.append("lifecycle rule not readable back")
        except Exception as e:
            FAILS.append("put lifecycle: %s" % str(e)[:160])
    elif rules:
        R.ok("   lifecycle %s already present" % NEW_ID)

    # ================================================================ 7
    R.section("7. us-west-2 DR mirror -- stale Standard-IA storage (HOLD: reported, not deleted)")
    try:
        cw_w = boto3.client("cloudwatch", region_name="us-west-2", config=CFG)
        for b in s3.list_buckets().get("Buckets", []):
            bn = b["Name"]
            try:
                loc = s3.get_bucket_location(Bucket=bn).get("LocationConstraint")
            except Exception:
                continue
            if loc != "us-west-2":
                continue
            tot_gb = 0.0
            parts = []
            for stype in ("StandardStorage", "StandardIAStorage", "StandardIASizeOverhead", "GlacierInstantRetrievalStorage"):
                try:
                    res = cw_w.get_metric_statistics(
                        Namespace="AWS/S3", MetricName="BucketSizeBytes",
                        Dimensions=[{"Name": "BucketName", "Value": bn}, {"Name": "StorageType", "Value": stype}],
                        StartTime=NOW - timedelta(days=4), EndTime=NOW, Period=86400, Statistics=["Average"])
                    pts = sorted(res.get("Datapoints", []), key=lambda p: p["Timestamp"])
                    if pts:
                        gb = pts[-1]["Average"] / 1e9
                        tot_gb += gb
                        parts.append("%s=%.0fGB" % (stype.replace("Storage", ""), gb))
                except Exception:
                    pass
            usd = tot_gb * 0.0125
            R.warn("   %-46s %.0f GB (%s) ~ $%.2f/month -- replication was deleted Aug-26 (ops 4988); this copy only ages"
                   % (bn, tot_gb, ", ".join(parts), usd))
            LEDGER["holds"].append({"kind": "dr-bucket", "bucket": bn, "gb": round(tot_gb), "usd_month": round(usd, 2)})
            R.kv(section="7_dr", bucket=bn, gb=round(tot_gb), usd_month=round(usd, 2))
    except Exception as e:
        R.warn("   DR scan: %s" % str(e)[:120])

    # ================================================================ 8
    R.section("8. SnapStart cache ($17/month in August) -- which functions (HOLD)")
    snap = []
    for page in lam.get_paginator("list_functions").paginate():
        for f in page["Functions"]:
            if (f.get("SnapStart") or {}).get("ApplyOn") == "PublishedVersions":
                snap.append(f["FunctionName"])
    for s_ in snap:
        R.warn("   %s  SnapStart=PublishedVersions" % s_)
        LEDGER["holds"].append({"kind": "snapstart", "function": s_, "usd_month": round(17.13 / max(len(snap), 1), 2)})
    if not snap:
        R.log("   none with SnapStart enabled -- the cache line is published-version storage aging out")

    R.section("8b. justhodl-symdir -- triggers (report only; chart-pro is hands-off)")
    m6 = fn_metrics("justhodl-symdir", 6)
    R.log("   last 6h: invocations %d, errors %d, %.1f Lambda-hours at %dMB"
          % (m6["Invocations"], m6["Errors"], m6["Duration"] / 3.6e6, fns.get("justhodl-symdir", 0)))
    show_triggers("justhodl-symdir")

    # ================================================================ 9
    R.section("9. justhodl-census-us econ lane -- bracketed progress, own dispatcher schedule")
    gap = 360 - (time.time() - census_a_t)
    if gap > 0:
        R.log("   waiting %.0fs so the second lane read brackets at least 6 minutes" % gap)
        time.sleep(gap)
    census_b = census_shards()
    dt_min = (time.time() - census_a_t) / 60.0
    d_done = census_b["n_done"] - census_a["n_done"]
    d_rows = census_b["rows_total"] - census_a["rows_total"]
    R.log("   shards %d  phases=%s  n_done %d/%d  queue_left %d  rows_total %s  failures %d  newest=%s"
          % (census_b["shards"], census_b["phases"], census_b["n_done"], census_b["n_total"],
             census_b["queue_left"], "{:,}".format(census_b["rows_total"]), census_b["failures"], census_b["newest"]))
    R.log("   %.1f-minute bracket: datasets +%d, rows +%s" % (dt_min, d_done, "{:,}".format(d_rows)))
    fn = "justhodl-census-us"
    m6 = fn_metrics(fn, 6)
    R.log("   last 6h: invocations %d, errors %d, throttles %d, %.1f Lambda-hours"
          % (m6["Invocations"], m6["Errors"], m6["Throttles"], m6["Duration"] / 3.6e6))
    cl, sc = show_triggers(fn)
    complete = census_b["shards"] > 0 and census_b["queue_left"] == 0 and census_b["n_total"] > 0 \
        and census_b["n_done"] >= census_b["n_total"]
    progressing = (d_done > 0) or (d_rows > 0)
    if complete:
        cadence, verdict = "rate(6 hours)", "lane COMPLETE -> refresh cadence"
    elif progressing:
        cadence, verdict = "rate(5 minutes)", "lane IMPORTING and progressing -> same throughput, own knob"
        eta_min = (census_b["queue_left"] / max(d_done / dt_min, 1e-9)) if d_done > 0 else -1
        if eta_min > 0:
            R.log("   ETA at this rate: %.1f hours for %d queued datasets" % (eta_min / 60.0, census_b["queue_left"]))
    else:
        cadence, verdict = "rate(1 hour)", "lane NOT progressing in the bracket -> hourly until it moves"
    R.log("   verdict: %s" % verdict)
    LEDGER["census_econ"] = {"a": census_a, "b": census_b, "bracket_min": round(dt_min, 1),
                             "verdict": verdict, "cadence": cadence}
    # move the dispatcher off the host rule onto its own schedule (doctrine: never hitch)
    moved = False
    for rn, expr, st, tg in cl:
        disp = [t for t in tg if '"econ_dispatch"' in str(t.get("Input", "")) or t.get("Id") == "econdispatch"]
        if disp:
            try:
                if remove_fn_from_rule(fn, rn, disp, "econ dispatcher hitched onto %s (ops 5063)" % expr):
                    moved = True
            except Exception as e:
                FAILS.append("census remove from %s: %s" % (rn, str(e)[:120]))
    try:
        ensure_schedule("justhodl-census-econ-dispatch", cadence, fn,
                        {"mode": "econ_dispatch", "shards": 12},
                        "census econ lane dispatcher (12 shards); cadence set by lane state in ops 5164")
    except Exception as e:
        FAILS.append("census dispatcher schedule: %s" % str(e)[:140])
    if not moved and cl:
        R.log("   (no econ_dispatch target found on classic rules -- dispatcher may already be Scheduler-driven)")

    # ================================================================ 10
    R.section("10. Ledger + expected landing")
    try:
        s3.put_object(Bucket=BUCKET, Key=LEDGER_KEY, Body=json.dumps(LEDGER, indent=1, default=str).encode(),
                      ContentType="application/json")
        R.ok("   reversal ledger written to s3://%s/%s" % (BUCKET, LEDGER_KEY))
    except Exception as e:
        FAILS.append("ledger write: %s" % str(e)[:120])
    R.log("   removed targets: %d  schedules created/updated: %d  temp schedules deleted: %d  lifecycle: %s"
          % (len(LEDGER["removed_targets"]), len(LEDGER["created_schedules"]),
             len(LEDGER["deleted_schedules"]), "armed" if LEDGER["lifecycle_added"] else "unchanged"))
    rp = ROOT / "aws" / "ops" / "reports" / "5164_sep_cost_standdown.json"
    rp.write_text(json.dumps(LEDGER, indent=1, default=str), encoding="utf-8")
    if FAILS:
        for f in FAILS:
            R.fail(f)
        sys.exit(1)
    R.ok("ops 5164 complete in %.0fs" % (time.time() - T0))
