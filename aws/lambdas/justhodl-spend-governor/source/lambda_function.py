"""justhodl-spend-governor - enforces the monthly AWS spend cap.  Model: governor_core.py; levers: governor_levers.py.

Triggers
  * EventBridge Scheduler justhodl-spend-governor-tick  cron(7 * * * ? *)        {"action": "tick"}
  * SNS justhodl-spend-governor  <- AWS Budget JustHodl-Monthly-Hard-Cap-500 alerts  -> tick
  * direct invoke from ops: {"action": "status" | "plan" | "force" | "restore" | "selftest" | "fuse_check"}

Private state (reserved concurrency 1 = single writer):
  s3://$STATE_BUCKET/spend-governor/state.json    stage, ledger of every change with its BEFORE value
  s3://$STATE_BUCKET/spend-governor/control.json  mode (monitor|enforce), per-month cap overrides, exempt list
  s3://$STATE_BUCKET/spend-governor/events/       immutable record of every escalation / restore / fuse trip

Operator levers (all reversible): raise the AWS Budget amount (the governor releases on its next tick), or invoke
{"action": "restore", "suppress_hours": N}.  Exempt a function permanently via control.json exempt_functions.
"""
import concurrent.futures as cf
import json
import os
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

import governor_core as core
import governor_levers as lv

REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT = os.environ.get("ACCOUNT_ID", "857687956942")
SELF = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "justhodl-spend-governor")
BUDGET_NAME = os.environ.get("BUDGET_NAME", "JustHodl-Monthly-Hard-Cap-500")
STATE_BUCKET = os.environ.get("STATE_BUCKET", "justhodl-ops-private-857687956942")
PREFIX = "spend-governor/"
ALERT_TOPIC = os.environ.get("ALERT_TOPIC_ARN", "arn:aws:sns:us-east-1:857687956942:justhodl-fleet-alerts")
METRIC_NS = "JustHodl/SpendGovernor"
CE_INTERVAL_H = float(os.environ.get("CE_INTERVAL_H", "6"))
GUARD_ALARMS = ("justhodl-guard-lambda-invocations-1h", "justhodl-guard-lambda-concurrency",
                "justhodl-guard-lambda-recursion-dropped-account")
CANARY_PREFIX = "justhodl-spend-governor-canary"
# v1.1.0: CloudWatch alarms that mean "the fleet has been far above normal for 15 minutes" (thresholds
# calibrated by ops 6534 from Sep 15 - Oct 11 vs the Oct 12-16 storm).  Their ALARM notification reaches this
# function through SNS justhodl-spend-governor and runs the fast fuse.  The older GUARD_ALARMS fire on short
# legitimate bursts too (2026-10-10 16:33, a 4-minute fan-out), so they only speed up the hourly fuse.
RUNAWAY_ALARMS = ("justhodl-runaway-concurrency", "justhodl-runaway-invocations", "justhodl-runaway-duration",
                  "justhodl-event-breaker-open", "justhodl-s3-live-bucket-size")
FAST_FUSE_MIN_GAP_S = 300
DEFAULT_CONTROL = {
    "mode": "monitor",
    "exempt_functions": ["justhodl-guardrail-notify", "justhodl-supabase-keepalive", "justhodl-dr-snapshot"],
    "exempt_prefixes": [],
    "brake_always": ["justhodl-ai", "justhodl-student-rsi", "justhodl-factory-grader"],
    "cap_overrides": {},
    "thresholds": {},
    "fuse_interval_h": 6,
}
# standard (not adaptive) retries: adaptive mode rate-limits the CLIENT after throttles, and one expected
# throttle (the canary probe) or an API burst must never slow every later call in a warm container
BOTO_CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=60)
PROBE_CFG = Config(retries={"max_attempts": 1}, connect_timeout=10, read_timeout=30)  # a throttle is the expected answer
_clients = {}


def client(name):
    """'lambda-probe' is a no-retry Lambda client for calls whose expected answer is a throttle."""
    if name not in _clients:
        if name == "lambda-probe":
            _clients[name] = boto3.client("lambda", region_name=REGION, config=PROBE_CFG)
        else:
            _clients[name] = boto3.client(name, region_name=REGION, config=BOTO_CFG)
    return _clients[name]


def now_utc():
    return datetime.now(timezone.utc)


class Ctx:
    def __init__(self, control, ledger, context):
        self.control, self.ledger, self.context = control, ledger, context
        self.cache, self.logs = {}, []

    def c(self, name):
        return client(name)

    def exempt(self, fn):
        return core.is_exempt(fn, self.control, SELF)

    def log(self, msg):
        self.logs.append(msg)
        print("[governor] " + msg)

    def time_left(self):
        return self.context.get_remaining_time_in_millis() / 1000.0 if self.context else 900.0


# ------------------------------------------------------------------ storage --
def s3_get_json(key, default):
    try:
        return json.loads(client("s3").get_object(Bucket=STATE_BUCKET, Key=PREFIX + key)["Body"].read())
    except ClientError as e:
        if lv.err_code(e) in ("NoSuchKey", "404", "NotFound"):
            return default
        raise


def s3_put_json(key, obj):
    client("s3").put_object(Bucket=STATE_BUCKET, Key=PREFIX + key, ContentType="application/json",
                            ServerSideEncryption="AES256",
                            Body=json.dumps(obj, default=str, indent=1, sort_keys=True).encode())


def load_control():
    c = dict(DEFAULT_CONTROL)
    c.update(s3_get_json("control.json", {}) or {})
    return c


def new_state(now):
    return {"version": core.VERSION, "month": core.month_key(now), "stage": "NORMAL", "stage_since": now.isoformat(),
            "cap_at_stage": None, "ledger": {}, "released": [], "ce": {}, "alerts": {}, "suppress_until": None,
            "observations": [], "fuse": {}}


def record_event(kind, body, now):
    s3_put_json("events/%s-%s.json" % (now.strftime("%Y%m%dT%H%M%SZ"), kind), dict(body, kind=kind, at=now.isoformat()))


def notify(subject, lines, mode):
    tag = "" if mode == "enforce" else "[monitor] "
    try:
        client("sns").publish(TopicArn=ALERT_TOPIC, Subject=(tag + subject)[:99],
                              Message="\n".join(lines)[:60000])
    except Exception as e:  # noqa: BLE001 - alerting must never break enforcement
        print("[governor] notify failed: %s" % lv.err_code(e))


def put_metrics(mtd, stage):
    try:
        client("cloudwatch").put_metric_data(Namespace=METRIC_NS, MetricData=[
            {"MetricName": "Heartbeat", "Value": 1, "Unit": "Count"},
            {"MetricName": "MonthToDateUSD", "Value": float(mtd or 0), "Unit": "None"},
            {"MetricName": "StageLevel", "Value": core.LEVEL.get(stage, 0), "Unit": "None"}])
    except Exception as e:  # noqa: BLE001
        print("[governor] metrics failed: %s" % lv.err_code(e))


# -------------------------------------------------------------- spend data --
def read_budget():
    b = client("budgets").describe_budget(AccountId=ACCOUNT, BudgetName=BUDGET_NAME)["Budget"]
    cs = b.get("CalculatedSpend") or {}
    act = float(cs["ActualSpend"]["Amount"]) if cs.get("ActualSpend") else None
    fc = float(cs["ForecastedSpend"]["Amount"]) if cs.get("ForecastedSpend") else None
    return float(b["BudgetLimit"]["Amount"]), act, fc


def _ce_pages(**kw):
    token = None
    while True:
        if token:
            kw["NextPageToken"] = token
        r = client("ce").get_cost_and_usage(**kw)
        yield r
        token = r.get("NextPageToken")
        if not token:
            return


def refresh_ce(state, now, force=False):
    """Cost Explorer costs $0.01 a request: daily totals every CE_INTERVAL_H, usage types once a day."""
    ce = state.setdefault("ce", {})
    last = ce.get("at")
    if force or not last or ce.get("month") != core.month_key(now) or \
            (now - core.as_utc(last)).total_seconds() >= CE_INTERVAL_H * 3600:
        start = min(core.month_start(now).date(), (now - timedelta(days=16)).date())
        daily = {}
        for r in _ce_pages(TimePeriod={"Start": start.isoformat(), "End": (now + timedelta(days=1)).date().isoformat()},
                           Granularity="DAILY", Metrics=["UnblendedCost"]):
            for t in r["ResultsByTime"]:
                daily[t["TimePeriod"]["Start"]] = round(float(t["Total"]["UnblendedCost"]["Amount"]), 4)
        ms = core.month_start(now).date().isoformat()
        ce.update({"at": now.isoformat(), "month": core.month_key(now), "daily": daily,
                   "mtd": round(sum(v for d, v in daily.items() if d >= ms), 4)})
    if force or ce.get("usage_day") != now.date().isoformat():
        usage = {}
        for r in _ce_pages(TimePeriod={"Start": (now - timedelta(days=8)).date().isoformat(), "End": now.date().isoformat()},
                           Granularity="DAILY", Metrics=["UnblendedCost"],
                           GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}]):
            for t in r["ResultsByTime"]:
                day = usage.setdefault(t["TimePeriod"]["Start"], {})
                for g in t.get("Groups", []):
                    v = float(g["Metrics"]["UnblendedCost"]["Amount"])
                    if v and any(p in g["Keys"][0].lower() for p in core.PERSISTENT_USAGE_PATTERNS):
                        day[g["Keys"][0]] = round(v, 5)
        ce.update({"usage_day": now.date().isoformat(), "persistent_by_day": usage})
    return ce


def lambda_costs(ctx, hours):
    """USD/day per function implied by the last `hours` of CloudWatch Duration/Invocations (~$0.02 a call)."""
    fns = lv.list_functions(ctx)
    end = now_utc().replace(second=0, microsecond=0)
    start = end - timedelta(hours=hours)
    period = int(hours * 3600)
    queries = []
    for i, f in enumerate(fns):
        for tag, metric in (("d", "Duration"), ("n", "Invocations")):
            queries.append({"Id": "%s%d" % (tag, i), "ReturnData": True, "MetricStat": {
                "Metric": {"Namespace": "AWS/Lambda", "MetricName": metric,
                           "Dimensions": [{"Name": "FunctionName", "Value": f["name"]}]},
                "Period": period, "Stat": "Sum"}})
    vals = {}
    for i in range(0, len(queries), 500):
        token = None
        while True:
            kw = {"MetricDataQueries": queries[i:i + 500], "StartTime": start, "EndTime": end}
            if token:
                kw["NextToken"] = token
            r = client("cloudwatch").get_metric_data(**kw)
            for res in r["MetricDataResults"]:
                vals[res["Id"]] = vals.get(res["Id"], 0.0) + sum(res.get("Values") or [])
            token = r.get("NextToken")
            if not token:
                break
    return {f["name"]: core.lambda_cost_per_day(vals.get("d%d" % i, 0.0), vals.get("n%d" % i, 0.0),
                                                f["mem"], f["arch"], hours)
            for i, f in enumerate(fns)}


# ----------------------------------------------------------------- actions --
def discover_stage(ctx, stage, cfg, include_lambda=True):
    entries = []
    if include_lambda and stage == "FREEZE":
        names = [f["name"] for f in lv.list_functions(ctx)]
    elif include_lambda:
        try:
            costs = lambda_costs(ctx, 72)
            exempt = {n for n in costs if ctx.exempt(n)}
            names = core.rank_brake_targets(costs, exempt, cfg, ctx.control.get("brake_always") or [])
        except Exception as e:  # noqa: BLE001 - never let ranking block the rest of the brake
            ctx.log("brake ranking unavailable (%s) - always-list only" % lv.err_code(e))
            names = [n for n in (ctx.control.get("brake_always") or []) if not ctx.exempt(n)]
        ctx.log("brake picks %d Lambdas (top: %s)" % (len(names), ", ".join(names[:5])))
    else:
        names = []
    if names:
        entries += lv.lambda_discover(ctx, stage, names)
    for name, (disc, _, _, _) in lv.LEVERS.items():
        try:
            entries += disc(ctx, stage)
        except Exception as e:  # noqa: BLE001 - one service must never block the others
            ctx.log("lever %s discover failed: %s" % (name, lv.err_code(e)))
    return entries


def apply_entries(ctx, entries, enforce, now):
    if not enforce:
        for e in entries:
            e["status"] = "planned"
        return {"planned": len(entries)}

    def do(e):
        if ctx.time_left() < 45:
            e["status"] = "deferred"
            return e
        try:
            lv.apply_fn(e["lever"])(ctx, e)
            e.update(status="applied", applied_at=now.isoformat())
            e.pop("error", None)
            e.pop("error_msg", None)
        except Exception as ex:  # noqa: BLE001
            e.update(status="failed", error=lv.err_code(ex), error_msg=lv.err_msg(ex), failed_at=now.isoformat())
        return e

    fast = [e for e in entries if e["lever"] in ("lambda", "esm")]
    slow = [e for e in entries if e["lever"] not in ("lambda", "esm")]
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        done = list(ex.map(do, fast))
    done += [do(e) for e in slow]
    counts = {}
    for e in done:
        counts[e["status"]] = counts.get(e["status"], 0) + 1
        if e["status"] in ("applied", "failed"):
            ctx.ledger[e["key"]] = e
    return counts


def enforce_drift(ctx, stage, now):
    """Re-apply ledger entries something undid (RDS auto-start, a reset concurrency) and retry failures."""
    todo = []
    for e in list(ctx.ledger.values()):
        if e.get("stage") not in ("BRAKE", "FREEZE", "FUSE") or e.get("restorable") is False:
            continue
        if core.LEVEL.get(e.get("stage"), 2) > core.LEVEL.get(stage, 0) and e.get("stage") != "FUSE":
            continue
        if e["lever"] == "lambda" and ctx.exempt(e["id"]):
            continue
        todo.append(e)

    def needs_redo(e):
        if ctx.time_left() < 60:
            return None
        try:
            if e.get("status") == "failed":
                return e
            if e["lever"] == "lambda":
                cur = lv.reserved_of(ctx, e["id"])
                if cur == 0:
                    return None
                # Someone set this function's concurrency during the hold (an ops script, another lane).
                # The hold wins now; their value becomes what the 1st-of-month restore puts back.
                e["before"] = {"reserved": cur}
                e["drift_seen_at"] = now.isoformat()
                return e
            check = lv.is_applied_fn(e["lever"])
            return e if (check and not check(ctx, e)) else None
        except Exception as ex:  # noqa: BLE001
            if lv.err_code(ex) in ("ResourceNotFoundException", "ValidationException"):
                e["status"] = "gone"
            return None

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        redo = [e for e in ex.map(needs_redo, todo) if e is not None]
    out = apply_entries(ctx, redo, True, now) if redo else {}
    for e in redo:
        if e["status"] == "applied":
            e["status"] = "reapplied"
    return out


def restore_entries(ctx, predicate, now):
    order = {name: i for i, name in enumerate(lv.RESTORE_ORDER)}
    picked = sorted((e for e in ctx.ledger.values() if predicate(e)), key=lambda e: order.get(e["lever"], 99))

    def undo(e):
        if e.get("restorable") is False or e.get("status") == "gone":
            e["status"] = "released"
            return e
        try:
            e["status"] = lv.restore_fn(e["lever"])(ctx, e)
            e["restored_at"] = now.isoformat()
        except Exception as ex:  # noqa: BLE001
            if lv.err_code(ex) in ("ResourceNotFoundException",):
                e["status"] = "skipped_gone"
            else:
                e.update(status="restore_failed", error=lv.err_code(ex), error_msg=lv.err_msg(ex))
        return e

    lam = [e for e in picked if e["lever"] == "lambda"]
    rest = [e for e in picked if e["lever"] != "lambda"]
    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        done = list(ex.map(undo, lam))
    done += [undo(e) for e in rest]
    counts = {}
    for e in done:
        counts[e["status"]] = counts.get(e["status"], 0) + 1
        if e["status"] != "restore_failed":
            ctx.ledger.pop(e["key"], None)
    return counts, done


def recent_invocations(ctx, names, minutes=20):
    """Invocation count per function over the last `minutes` - a burst that already ended is not a runaway."""
    end = now_utc().replace(second=0, microsecond=0)
    q = [{"Id": "i%d" % i, "ReturnData": True, "MetricStat": {
        "Metric": {"Namespace": "AWS/Lambda", "MetricName": "Invocations",
                   "Dimensions": [{"Name": "FunctionName", "Value": fn}]},
        "Period": minutes * 60, "Stat": "Sum"}} for i, fn in enumerate(names)]
    vals = {}
    for i in range(0, len(q), 500):
        r = client("cloudwatch").get_metric_data(MetricDataQueries=q[i:i + 500],
                                                 StartTime=end - timedelta(minutes=minutes), EndTime=end)
        for res in r["MetricDataResults"]:
            vals[res["Id"]] = sum(res.get("Values") or [])
    return {fn: vals.get("i%d" % i, 0.0) for i, fn in enumerate(names)}


def check_fuse(ctx, state, cfg, baseline, now, enforce, force=False):
    """Runaway fuse: independent of the monthly cap, throttles single functions burning far above normal.

    Two strikes (v1.0.1): a function must be hot (>= runaway_fn_usd_day over the LAST HOUR, still invoking
    in the last 20 minutes, fleet far above normal) at two consecutive checks 0.8-3 h apart.  A legitimate
    batch spike or a storm that already ended never trips it; a real runaway is held within ~2 hours."""
    fz = state.setdefault("fuse", {})
    alarms = {}
    try:
        for a in client("cloudwatch").describe_alarms(AlarmNames=list(GUARD_ALARMS)).get("MetricAlarms", []):
            alarms[a["AlarmName"]] = a["StateValue"]
    except Exception as e:  # noqa: BLE001
        ctx.log("guard alarms unreadable: %s" % lv.err_code(e))
    firing = sorted(n for n, s in alarms.items() if s == "ALARM")
    last = fz.get("checked_at")
    age_h = (now - core.as_utc(last)).total_seconds() / 3600.0 if last else 1e9
    interval = float(ctx.control.get("fuse_interval_h") or 6)
    suspects = fz.get("suspects") or {}
    if not (force or ((firing or suspects) and age_h >= 0.8) or age_h >= interval):
        return {"checked": False, "firing": firing, "suspects": sorted(suspects)}
    costs = lambda_costs(ctx, 1)
    exempt = {n for n in costs if ctx.exempt(n)}
    hot = core.runaway_functions(costs, baseline, exempt, cfg)
    if hot:
        active = recent_invocations(ctx, hot)
        hot = [fn for fn in hot if active.get(fn, 0) > 0]
    # strike two only counts against a strike one 0.8-3 h old
    confirmed = [fn for fn in hot if fn in suspects
                 and 0.8 <= (now - core.as_utc(suspects[fn])).total_seconds() / 3600.0 <= 3.0]
    fz["suspects"] = {fn: (suspects[fn] if fn in suspects and fn not in confirmed and
                           (now - core.as_utc(suspects[fn])).total_seconds() / 3600.0 <= 3.0 else now.isoformat())
                      for fn in hot if fn not in confirmed}
    top = sorted(costs.items(), key=lambda kv: -kv[1])[:8]
    fz.update({"checked_at": now.isoformat(), "firing": firing, "fleet_usd_day": round(sum(costs.values()), 2),
               "top": [(k, round(v, 2)) for k, v in top]})
    run = confirmed
    new = [fn for fn in run if ("lambda:" + fn) not in ctx.ledger]
    if not enforce:
        seen = fz.get("monitor_notified") or {}
        if seen.get("day") == now.date().isoformat() and set(new) <= set(seen.get("fns") or []):
            return {"checked": True, "firing": firing, "runaway": run, "new": new, "notified": False}
        fz["monitor_notified"] = {"day": now.date().isoformat(), "fns": sorted(set(new) | set(seen.get("fns") or []))}
    if not new:
        return {"checked": True, "firing": firing, "runaway": run, "new": [], "suspects": sorted(fz["suspects"])}
    entries = lv.lambda_discover(ctx, "FUSE", new)
    counts = apply_entries(ctx, entries, enforce, now)
    lines = ["Runaway fuse %s %d function(s) burning far above normal:" % ("throttled" if enforce else "would throttle", len(new))]
    lines += ["  %s  ~$%.0f/day" % (fn, costs[fn]) for fn in new]
    lines += ["Fleet Lambda rate ~$%.0f/day vs normal ~$%.0f/day. Guard alarms: %s" % (sum(costs.values()), baseline, ", ".join(firing) or "none"),
              "Reversal: invoke justhodl-spend-governor {\"action\":\"restore\",\"include_fuse\":true} after the cause is fixed."]
    notify("JustHodl runaway fuse: %s" % ", ".join(new[:3]), lines, "enforce" if enforce else "monitor")
    record_event("fuse", {"functions": new, "costs": {fn: costs[fn] for fn in new}, "counts": counts, "enforce": enforce}, now)
    return {"checked": True, "firing": firing, "runaway": run, "new": new, "counts": counts}


def sns_alarm(event):
    """The CloudWatch alarm notification inside an SNS record, or None (AWS Budgets sends plain text)."""
    try:
        msg = json.loads(event["Records"][0]["Sns"].get("Message") or "")
    except (KeyError, IndexError, TypeError, ValueError):
        return None
    return msg if isinstance(msg, dict) and msg.get("AlarmName") else None


def lambda_cost_buckets(ctx, names, minutes=15, period=300, lag_min=2):
    """USD/day per function for each `period` bucket of the last `minutes` (ending lag_min ago: Lambda metrics
    land 1-3 minutes late, and a half-filled newest bucket must not hide a runaway).  Missing bucket = 0."""
    fns = {f["name"]: f for f in lv.list_functions(ctx)}
    end = now_utc().replace(second=0, microsecond=0) - timedelta(minutes=lag_min)
    start = end - timedelta(minutes=minutes)
    q = []
    for i, fn in enumerate(names):
        for tag, metric in (("d", "Duration"), ("n", "Invocations")):
            q.append({"Id": "%s%d" % (tag, i), "ReturnData": True, "MetricStat": {
                "Metric": {"Namespace": "AWS/Lambda", "MetricName": metric,
                           "Dimensions": [{"Name": "FunctionName", "Value": fn}]},
                "Period": period, "Stat": "Sum"}})
    raw = {}
    for i in range(0, len(q), 500):
        r = client("cloudwatch").get_metric_data(MetricDataQueries=q[i:i + 500], StartTime=start, EndTime=end)
        for res in r["MetricDataResults"]:
            raw[res["Id"]] = {str(t): v for t, v in zip(res.get("Timestamps") or [], res.get("Values") or [])}
    n_buckets = max(1, int(minutes * 60 // period))
    out = {}
    for i, fn in enumerate(names):
        f = fns.get(fn) or {"mem": 128, "arch": "x86_64"}
        d, n = raw.get("d%d" % i, {}), raw.get("n%d" % i, {})
        rates = [core.lambda_cost_per_day(d.get(t, 0.0), n.get(t, 0.0), f["mem"], f["arch"], period / 3600.0)
                 for t in sorted(set(d) | set(n))][-n_buckets:]
        out[fn] = [0.0] * (n_buckets - len(rates)) + rates
    return out


def fast_fuse(ctx, state, cfg, now, enforce, alarm):
    """v1.1.0: stop a runaway within minutes of a sustained-runaway alarm instead of the hourly two-strike fuse.

    2026-10-12: justhodl-guard-lambda-concurrency went to ALARM at 09:18, three minutes into the loop, and stayed
    there 4.4 days - its notification reached only an unconfirmed e-mail and a dead Telegram bot, and nothing
    acted on it.  Now the alarm itself is the trigger."""
    ff = state.setdefault("fast_fuse", {})
    last = ff.get("at")
    if last and (now - core.as_utc(last)).total_seconds() < FAST_FUSE_MIN_GAP_S:
        return {"checked": False, "reason": "fast fuse ran %ds ago" % (now - core.as_utc(last)).total_seconds()}
    name = alarm.get("AlarmName")
    costs = lambda_costs(ctx, 0.25)
    exempt = {n for n in costs if ctx.exempt(n)}
    total = sum(costs.values())
    floor = cfg["runaway_fn_usd_day"]
    hot = [fn for fn, usd in sorted(costs.items(), key=lambda kv: -kv[1])
           if fn not in exempt and usd >= floor and usd >= cfg["fast_fuse_min_share"] * total][:10]
    buckets = lambda_cost_buckets(ctx, hot) if hot else {}
    picks = core.fast_fuse_targets(costs, buckets, exempt, cfg)
    new = [fn for fn in picks if ("lambda:" + fn) not in ctx.ledger]
    top = [(k, round(v, 2)) for k, v in sorted(costs.items(), key=lambda kv: -kv[1])[:8]]
    ff.update({"at": now.isoformat(), "alarm": name, "fleet_usd_day": round(total, 2), "hot": hot,
               "picks": picks, "new": new, "top": top,
               "buckets": {fn: [round(x, 1) for x in buckets.get(fn, [])] for fn in hot}})
    if not new:
        record_event("fast-fuse-check", {"alarm": name, "fleet_usd_day": round(total, 2), "hot": hot, "picks": picks,
                                         "top": top, "enforce": enforce}, now)
        return {"checked": True, "alarm": name, "fleet_usd_day": round(total, 2), "hot": hot, "picks": picks,
                "new": []}
    entries = lv.lambda_discover(ctx, "FUSE", new)
    counts = apply_entries(ctx, entries, enforce, now)
    lines = ["Alarm %s: the fleet has been far above normal for 15 minutes." % name,
             "Fast fuse %s %d function(s) burning in every 5-minute bucket of the window:" % (
                 "throttled" if enforce else "would throttle", len(new))]
    lines += ["  %s  ~$%.0f/day  buckets %s" % (fn, costs[fn], [round(x) for x in buckets.get(fn, [])]) for fn in new]
    lines += ["Fleet Lambda rate ~$%.0f/day. Nothing else was touched." % total,
              "Reversal after the cause is fixed: invoke justhodl-spend-governor "
              "{\"action\":\"restore\",\"include_fuse\":true,\"only_ids\":[\"<function>\"]}."]
    notify("JustHodl runaway STOPPED: %s" % ", ".join(new[:3]), lines, "enforce" if enforce else "monitor")
    record_event("fast-fuse", {"alarm": name, "functions": new, "costs": {fn: costs[fn] for fn in new},
                               "buckets": {fn: buckets.get(fn) for fn in new}, "counts": counts,
                               "enforce": enforce}, now)
    return {"checked": True, "alarm": name, "fleet_usd_day": round(total, 2), "picks": picks, "new": new,
            "counts": counts}


def alarm_trigger(event, context, alarm):
    now = now_utc()
    name, alarm_state = alarm.get("AlarmName"), alarm.get("NewStateValue")
    if name not in RUNAWAY_ALARMS or alarm_state != "ALARM":
        return {"trigger": "alarm", "alarm": name, "state": alarm_state, "action": "none"}
    control = load_control()
    state = s3_get_json("state.json", None) or new_state(now)
    ctx = Ctx(control, state.setdefault("ledger", {}), context)
    cfg = core.merged_cfg(control.get("thresholds"))
    res = fast_fuse(ctx, state, cfg, now, control.get("mode") == "enforce", alarm)
    state.update({"version": core.VERSION, "updated_at": now.isoformat()})
    s3_put_json("state.json", state)
    res.update({"trigger": "alarm", "alarm": name, "logs": ctx.logs[-10:]})
    return res


def month_rollover(ctx, state, now, mode):
    old = state.get("month")
    record_event("month-close", {"month": old, "stage": state.get("stage"), "ledger_size": len(ctx.ledger),
                                 "alerts": state.get("alerts")}, now)
    counts, _ = restore_entries(ctx, lambda e: e.get("stage") in ("BRAKE", "FREEZE"), now)
    held = [e["id"] for e in ctx.ledger.values() if e.get("stage") == "FUSE"]
    prev_stage = state.get("stage")
    state.update({"month": core.month_key(now), "stage": "NORMAL", "stage_since": now.isoformat(), "cap_at_stage": None,
                  "alerts": {}, "suppress_until": None, "observations": []})
    if prev_stage != "NORMAL" or counts:
        notify("JustHodl spend governor: new month, %s lifted" % (prev_stage or "NORMAL"),
               ["Month %s closed at stage %s. Restored: %s." % (old, prev_stage, json.dumps(counts)),
                "Still held by the runaway fuse: %s" % (", ".join(held) or "nothing")], mode)
    return counts


def stage_message(stage, reason, th, counts, ctx, state):
    lines = ["%s: %s" % (stage, reason),
             "Cap $%.0f | MTD $%.2f | brake line $%.2f | freeze line $%.2f | %.1f days left" % (
                 th["cap"], th["mtd"], th["brake_at"], th["freeze_at"], th["days_left"]),
             "Actions: %s" % json.dumps(counts),
             "Auto-restores on the 1st (UTC). To release now: raise the AWS Budget amount, or invoke "
             "justhodl-spend-governor {\"action\":\"restore\",\"suppress_hours\":24}."]
    by_lever = {}
    for e in ctx.ledger.values():
        by_lever[e["lever"]] = by_lever.get(e["lever"], 0) + 1
    lines.append("Held now: %s" % json.dumps(by_lever))
    return lines


def tick(event, context, trigger):
    now = now_utc()
    control = load_control()
    mode = "enforce" if control.get("mode") == "enforce" else "monitor"
    enforce = mode == "enforce"
    cfg = core.merged_cfg(control.get("thresholds"))
    state = s3_get_json("state.json", None) or new_state(now)
    ctx = Ctx(control, state.setdefault("ledger", {}), context)
    out = {"trigger": trigger, "mode": mode, "version": core.VERSION}

    if state.get("month") != core.month_key(now):
        out["rollover"] = month_rollover(ctx, state, now, mode)

    limit, b_actual, b_forecast = read_budget()
    try:
        ce = refresh_ce(state, now)
    except Exception as e:  # noqa: BLE001 - Budgets alone is enough to enforce
        ctx.log("cost explorer unavailable: %s" % lv.err_code(e))
        ce = state.get("ce") or {}
    sources = {"budget_actual": b_actual, "ce_mtd": ce.get("mtd") if ce.get("month") == core.month_key(now) else None}
    mtd = max([v for v in sources.values() if v is not None] or [0.0])
    cap, cap_src = core.effective_cap(limit, control, now)
    baseline, b_src = core.baseline_from_daily(ce.get("daily") or {}, now.date(), cfg)
    residual, r_src = core.residual_from_usage(ce.get("persistent_by_day") or {}, now.date(), cfg)
    th = core.thresholds(cap, mtd, now, baseline, residual, cfg)
    th.update({"cap_source": cap_src, "baseline_source": b_src, "residual_source": r_src,
               "budget_limit": limit, "budget_forecast": b_forecast, "sources": sources})
    obs = state.setdefault("observations", [])
    obs.append([now.isoformat(), round(mtd, 2)])
    del obs[:-72]

    try:
        if state.get("stage") == "FREEZE":
            out["fuse"] = {"checked": False, "reason": "frozen - nothing can run"}  # saves the metric reads
        else:
            out["fuse"] = check_fuse(ctx, state, cfg, baseline, now, enforce)
    except Exception as e:  # noqa: BLE001 - the cap path must run even if metrics are unreadable
        ctx.log("fuse check failed: %s" % lv.err_code(e))
        out["fuse"] = {"error": lv.err_code(e)}

    sup = state.get("suppress_until")
    suppressed = bool(sup) and core.as_utc(sup) > now
    cur = state.get("stage", "NORMAL")
    nxt, reason = core.decide(cur, mtd, th, state.get("cap_at_stage"), cfg, suppressed)
    out.update({"stage_before": cur, "decision": nxt, "reason": reason, "thresholds": th})

    if core.LEVEL[nxt] > core.LEVEL[cur]:
        entries = discover_stage(ctx, nxt, cfg)
        counts = apply_entries(ctx, entries, enforce, now)
        out["applied"] = counts
        if enforce:
            state.update({"stage": nxt, "stage_since": now.isoformat(), "cap_at_stage": cap})
        prev_plan = state.get("plan") or {}
        fresh = enforce or prev_plan.get("stage") != nxt or prev_plan.get("day") != now.date().isoformat()
        if not enforce:
            state["plan"] = {"stage": nxt, "at": now.isoformat(), "day": now.date().isoformat(), "counts": counts,
                             "sample": [e["key"] for e in entries[:40]]}
        if fresh:
            notify("JustHodl spend governor: %s (MTD $%.0f of $%.0f)" % (nxt, mtd, cap),
                   stage_message(nxt, reason, th, counts, ctx, state), mode)
            record_event("escalate-" + nxt.lower(), {"reason": reason, "thresholds": th, "counts": counts, "mode": mode}, now)
    elif core.LEVEL[nxt] < core.LEVEL[cur]:
        drop = ("FREEZE",) if nxt == "BRAKE" else ("BRAKE", "FREEZE")
        counts, _ = restore_entries(ctx, lambda e: e.get("stage") in drop, now)
        state.update({"stage": nxt, "stage_since": now.isoformat(), "cap_at_stage": cap if nxt != "NORMAL" else None})
        out["restored"] = counts
        notify("JustHodl spend governor: %s -> %s" % (cur, nxt), stage_message(nxt, reason, th, counts, ctx, state), mode)
        record_event("deescalate-" + nxt.lower(), {"reason": reason, "thresholds": th, "counts": counts}, now)
    elif cur in ("BRAKE", "FREEZE") and enforce:
        new = apply_entries(ctx, discover_stage(ctx, cur, cfg, include_lambda=(cur == "FREEZE")), True, now)
        out["drift"] = {"new": new, "reapplied": enforce_drift(ctx, cur, now)}
    elif cur == "NORMAL":
        leftovers = [e for e in ctx.ledger.values() if e.get("stage") in ("BRAKE", "FREEZE")]
        if leftovers:  # a restore that failed earlier: keep trying
            out["restore_retry"], _ = restore_entries(ctx, lambda e: e.get("stage") in ("BRAKE", "FREEZE"), now)

    today = now.date().isoformat()
    alerts = state.setdefault("alerts", {})
    if state.get("stage") == "NORMAL" and th["projected_eom"] > cap and alerts.get("projection_day") != today:
        alerts["projection_day"] = today
        notify("JustHodl spend: on pace for $%.0f (cap $%.0f)" % (th["projected_eom"], cap),
               ["Month-to-date $%.2f; at the normal ~$%.0f/day the month lands near $%.0f." % (mtd, baseline, th["projected_eom"]),
                "BRAKE (shed the heaviest load) at $%.2f, FREEZE at $%.2f." % (th["brake_at"], th["freeze_at"])], mode)
    if th["cap_at_risk_even_frozen"] and alerts.get("risk_day") != today:
        alerts["risk_day"] = today
        notify("JustHodl spend: cap $%.0f at risk even if frozen" % cap,
               ["Residual spend (storage, site traffic, alarms) ~$%.2f/day x %.1f days would land the month at $%.0f." % (
                   residual, th["days_left"], th["frozen_eom"])], mode)

    state.update({"version": core.VERSION, "updated_at": now.isoformat(), "last": {
        "mtd": round(mtd, 2), "decision": nxt, "reason": reason, "thresholds": th, "mode": mode, "logs": ctx.logs[-30:]}})
    released = [e for e in state.get("released", [])][-300:]
    state["released"] = released
    s3_put_json("state.json", state)
    put_metrics(mtd, state.get("stage"))
    out["stage"] = state.get("stage")
    out["ledger_size"] = len(ctx.ledger)
    out["logs"] = ctx.logs[-20:]
    return out


def action_status(event, context):
    now = now_utc()
    control = load_control()
    cfg = core.merged_cfg(control.get("thresholds"))
    state = s3_get_json("state.json", None) or new_state(now)
    limit, b_actual, b_forecast = read_budget()
    ce = state.get("ce") or {}
    if event.get("refresh"):
        ce = refresh_ce(state, now, force=True)
    mtd = max([v for v in (b_actual, ce.get("mtd") if ce.get("month") == core.month_key(now) else None) if v is not None] or [0.0])
    cap, cap_src = core.effective_cap(limit, control, now)
    baseline, _ = core.baseline_from_daily(ce.get("daily") or {}, now.date(), cfg)
    residual, _ = core.residual_from_usage(ce.get("persistent_by_day") or {}, now.date(), cfg)
    th = core.thresholds(cap, mtd, now, baseline, residual, cfg)
    by_stage = {}
    for e in (state.get("ledger") or {}).values():
        k = "%s/%s" % (e.get("stage"), e.get("lever"))
        by_stage[k] = by_stage.get(k, 0) + 1
    return {"version": core.VERSION, "mode": control.get("mode"), "stage": state.get("stage"),
            "stage_since": state.get("stage_since"), "cap": cap, "cap_source": cap_src, "budget_limit": limit,
            "budget_actual": b_actual, "budget_forecast": b_forecast, "thresholds": th, "held": by_stage,
            "suppress_until": state.get("suppress_until"), "fuse": state.get("fuse"), "last": state.get("last")}


def action_plan(event, context):
    stage = event.get("stage", "FREEZE")
    if stage not in ("BRAKE", "FREEZE"):
        return {"error": "stage must be BRAKE or FREEZE"}
    control = load_control()
    state = s3_get_json("state.json", None) or new_state(now_utc())
    ctx = Ctx(control, dict(state.get("ledger") or {}), context)
    entries = discover_stage(ctx, stage, core.merged_cfg(control.get("thresholds")))
    by_lever = {}
    for e in entries:
        by_lever.setdefault(e["lever"], []).append(e["id"])
    exempt = sorted(f["name"] for f in lv.list_functions(ctx) if ctx.exempt(f["name"]))
    return {"stage": stage, "would_act_on": {k: len(v) for k, v in by_lever.items()},
            "sample": {k: v[:25] for k, v in by_lever.items()}, "exempt": exempt, "logs": ctx.logs}


def action_force(event, context):
    stage = event.get("stage")
    if stage not in ("BRAKE", "FREEZE") or event.get("confirm") != stage:
        return {"error": "force needs stage BRAKE|FREEZE and confirm equal to the stage"}
    now = now_utc()
    control = load_control()
    state = s3_get_json("state.json", None) or new_state(now)
    ctx = Ctx(control, state.setdefault("ledger", {}), context)
    cfg = core.merged_cfg(control.get("thresholds"))
    counts = apply_entries(ctx, discover_stage(ctx, stage, cfg), True, now)
    limit, _, _ = read_budget()
    cap, _ = core.effective_cap(limit, control, now)
    if core.LEVEL[stage] > core.LEVEL.get(state.get("stage"), 0):
        state.update({"stage": stage, "stage_since": now.isoformat(), "cap_at_stage": cap})
    state["suppress_until"] = None
    s3_put_json("state.json", state)
    record_event("force-" + stage.lower(), {"reason": event.get("reason"), "counts": counts}, now)
    notify("JustHodl spend governor: operator %s" % stage, ["Forced %s: %s. Reason: %s" % (
        stage, json.dumps(counts), event.get("reason") or "-")], "enforce")
    return {"stage": state["stage"], "applied": counts, "logs": ctx.logs}


def action_restore(event, context):
    now = now_utc()
    control = load_control()
    state = s3_get_json("state.json", None) or new_state(now)
    ctx = Ctx(control, state.setdefault("ledger", {}), context)
    include_fuse = bool(event.get("include_fuse"))
    stages = ("BRAKE", "FREEZE", "FUSE") if include_fuse else ("BRAKE", "FREEZE")
    only = set(event.get("only_ids") or [])
    counts, done = restore_entries(ctx, lambda e: e.get("stage") in stages and (not only or e["id"] in only), now)
    hours = float(event.get("suppress_hours") or 0)
    still_held = any(e.get("stage") in ("BRAKE", "FREEZE") for e in ctx.ledger.values())
    if not still_held:
        state.update({"stage": "NORMAL", "stage_since": now.isoformat(), "cap_at_stage": None})
    if hours > 0:
        state["suppress_until"] = (now + timedelta(hours=hours)).isoformat()
    state.setdefault("released", []).extend(
        {"key": e["key"], "status": e["status"], "stage": e.get("stage"), "at": now.isoformat()} for e in done)
    s3_put_json("state.json", state)
    record_event("restore", {"counts": counts, "include_fuse": include_fuse, "suppress_hours": hours,
                             "reason": event.get("reason")}, now)
    notify("JustHodl spend governor: restored", ["Operator restore: %s; suppress %.1fh; fuse included: %s" % (
        json.dumps(counts), hours, include_fuse)], "enforce")
    return {"restored": counts, "remaining_in_ledger": len(ctx.ledger), "stage": state.get("stage"),
            "suppress_until": state.get("suppress_until")}


def action_selftest(event, context):
    """Live round trip of the Lambda lever on a canary function only."""
    fn = event.get("function") or CANARY_PREFIX
    if not fn.startswith(CANARY_PREFIX):
        return {"error": "selftest only runs on functions named %s*" % CANARY_PREFIX}
    ctx = Ctx(load_control(), {}, context)
    before = lv.reserved_of(ctx, fn)
    e = lv.entry("lambda", fn, "SELFTEST", {"reserved": before})
    lv.lambda_apply(ctx, e)
    applied = lv.reserved_of(ctx, fn)
    invoke = None
    try:
        r = client("lambda-probe").invoke(FunctionName=fn, InvocationType="RequestResponse", Payload=b"{}")
        invoke = {"status": r.get("StatusCode"), "error": r.get("FunctionError")}
    except ClientError as ex:
        invoke = {"error": lv.err_code(ex)}
    res = lv.lambda_restore(ctx, e)
    after = lv.reserved_of(ctx, fn)
    ok = applied == 0 and invoke.get("error") == "TooManyRequestsException" and res == "restored" and after == before
    return {"ok": ok, "function": fn, "before": before, "applied": applied, "invoke_while_throttled": invoke,
            "restore": res, "after": after}


def lambda_handler(event, context):
    event = event if isinstance(event, dict) else {}
    if event.get("Records") and str(event["Records"][0].get("EventSource", "")).lower() == "aws:sns":
        alarm = sns_alarm(event)
        if alarm:
            return alarm_trigger(event, context, alarm)
        return tick(event, context, "budget-sns")
    action = event.get("action", "tick")
    if action == "tick":
        return tick(event, context, "schedule" if event.get("source") != "ops" else "ops")
    if action == "status":
        return action_status(event, context)
    if action == "plan":
        return action_plan(event, context)
    if action == "force":
        return action_force(event, context)
    if action == "restore":
        return action_restore(event, context)
    if action == "selftest":
        return action_selftest(event, context)
    if action == "fast_fuse":  # manual / verification entry point: same path as a runaway alarm
        return alarm_trigger(event, context, {"AlarmName": event.get("alarm") or RUNAWAY_ALARMS[0],
                                              "NewStateValue": "ALARM", "manual": True})
    if action == "fuse_check":
        now = now_utc()
        control = load_control()
        state = s3_get_json("state.json", None) or new_state(now)
        ctx = Ctx(control, state.setdefault("ledger", {}), context)
        cfg = core.merged_cfg(control.get("thresholds"))
        baseline, _ = core.baseline_from_daily((state.get("ce") or {}).get("daily") or {}, now.date(), cfg)
        res = check_fuse(ctx, state, cfg, baseline, now, control.get("mode") == "enforce", force=True)
        s3_put_json("state.json", state)
        return res
    return {"error": "unknown action %s" % action}
