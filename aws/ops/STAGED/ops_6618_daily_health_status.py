"""ops 6618 - READ-ONLY daily health status (2026-10-23, the scheduled daily health run; successor of ops 6606).

Reads only; nothing is written to any bucket and no engine is invoked except the governor's read-only actions:

  1. justhodl-spend-governor {"action":"status"}: version, mode, stage, budget actual / forecast, the cap in force,
     held counts by stage/lever, CAP and ANOM lists, the fuse record, the storage record (last assessment, versioning,
     warn/trip days, S3 and per-service dollars by day).
  2. justhodl-spend-governor {"action":"caps"}: the meter's month-to-date top engines.
  3. Freshness TODAY: every ENABLED daily schedule whose trigger minute has passed today (45 min of grace) should have
     written its declared data/*.json outputs (engine-manifest.json) after that minute. HEAD each (up to 2 per engine).
     Engines the governor holds (CAP / FUSE / ANOM / operator hold) are listed separately and NOT re-invoked - the health
     run never releases a hold.
  4. Issue #125 (error storm 14:07 UTC Oct 22, justhodl-polygon-futures-curves): Invocations / Errors over the last 24 h
     from CloudWatch metrics, error lines from its log group and the declared outputs' ages (read-only) - the engine was
     paced and relaunched by ops 6614 at 16:35 UTC Oct 22, so this is the first full day after the fix.
  5. Governor guardrail self-check and the dead-man alarms (heartbeat / errors), read-only.

Weekly / monthly / rate() schedules are not assessed. Budget: <= 1,200 HEAD, two governor invokes, a few metric and
log reads, under 3 minutes.
"""
from datetime import datetime, timezone, timedelta
from pathlib import Path
import concurrent.futures as cf
import json
import re
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report  # noqa: E402
import pipeline_order as po  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=40)
GOV = "justhodl-spend-governor"
STORM_FN = "justhodl-polygon-futures-curves"      # issue #125 (was #122)
GRACE_MIN = 45
EARLY_MIN = 10          # an output written up to 10 min before its trigger minute still counts (clock skew / fan-out ticks)
DOW = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
CRON = re.compile(r"^cron\((\S+) (\S+) (\S+) (\S+) (\S+) (\S+)\)$")


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def invoke(lam, payload):
    res = lam.invoke(FunctionName=GOV, InvocationType="RequestResponse", Payload=json.dumps(payload).encode())
    body = json.loads(res["Payload"].read() or b"{}")
    if res.get("FunctionError"):
        raise SystemExit("%s invoke %s failed: %s" % (GOV, payload, str(body)[:400]))
    return body


def dow_set(spec):
    if spec in ("*", "?"):
        return None
    out = set()
    for part in spec.upper().split(","):
        if "-" in part:
            a, b = part.split("-", 1)
            if a in DOW and b in DOW:
                i, j = DOW.index(a), DOW.index(b)
                out.update(DOW[i:j + 1] if i <= j else DOW[i:] + DOW[:j + 1])
        elif part in DOW:
            out.add(part)
        elif part.isdigit() and 1 <= int(part) <= 7:   # EventBridge: 1 = SUN
            out.add(DOW[(int(part) + 5) % 7])
    return out


def due_today(expr, today):
    m = CRON.match(expr or "")
    if not m:
        return False, "rate"
    dom, dow = m.group(3), m.group(5)
    if dom not in ("*", "?"):
        return False, "monthly"
    days = dow_set(dow)
    if days is None:
        return True, "daily"
    return (DOW[today.weekday()] in days), "weekly"


def main():
    lam, s3, cw, logs = c("lambda"), c("s3"), c("cloudwatch"), c("logs")
    now = datetime.now(timezone.utc)
    with report("ops_6618_daily_health_status") as r:
        r.heading("ops 6618 - daily health status (read-only), %s" % now.strftime("%Y-%m-%d %H:%M UTC"))

        r.section("governor")
        st = invoke(lam, {"action": "status"})
        r.log("version %s, mode %s, stage %s since %s, suppress_until %s" % (
            st.get("version"), st.get("mode"), st.get("stage"), st.get("stage_since"), st.get("suppress_until")))
        r.log("cap in force $%s (%s); budget limit $%s, actual $%s, forecast $%s; per-engine cap $%s/month" % (
            st.get("cap"), st.get("cap_source"), st.get("budget_limit"), st.get("budget_actual"), st.get("budget_forecast"),
            st.get("engine_cap_usd_month")))
        r.log("thresholds: %s" % json.dumps(st.get("thresholds"), default=str))
        r.log("held by stage/lever: %s" % json.dumps(st.get("held")))
        r.log("CAP held: %s" % json.dumps(st.get("capped")))
        r.log("ANOM held: %s" % json.dumps(st.get("anomaly_held")))
        r.log("anomalies record: %s" % json.dumps(st.get("anomalies"), default=str)[:1500])
        r.log("fuse: %s" % json.dumps(st.get("fuse"), default=str)[:1200])
        r.log("last tick: %s" % json.dumps(st.get("last"), default=str)[:800])
        r.log("cadence: %s (fixed %s)" % (json.dumps(st.get("cadence"), default=str)[:600], st.get("cadence_fixed")))
        stor = st.get("storage") or {}
        r.log("storage: day %s, versioning %s, warn_days %s, trip_days %s" % (
            stor.get("day"), stor.get("versioning"), stor.get("warn_days"), stor.get("trip_days")))
        r.log("storage last assessment: %s" % json.dumps(stor.get("last"), default=str)[:1500])
        r.log("storage days (last 5): %s" % json.dumps((stor.get("days") or [])[-5:], default=str)[:1500])
        r.log("storage s3_usd (by day): %s" % json.dumps(stor.get("s3_usd"), default=str)[:600])
        r.log("service spend (by day): %s" % json.dumps(stor.get("svc_usd"), default=str)[:2500])
        r.log("meter record: %s" % json.dumps(st.get("meter"), default=str)[:800])

        r.section("meter - month-to-date top engines")
        caps = invoke(lam, {"action": "caps"})
        top = caps.get("month_to_date_top") or []
        for fn, usd in top[:15]:
            r.log("  $%8.3f  %s" % (float(usd), fn))
        r.log("engine cap overrides: %s; released this month: %s" % (json.dumps(caps.get("overrides")), json.dumps(caps.get("released"))))

        r.section("holds (from the ledger, read-only)")
        state = json.loads(s3.get_object(Bucket=PRIVATE, Key="spend-governor/state.json")["Body"].read())
        ledger = state.get("ledger") or {}
        held_fn = {}
        for key, e in ledger.items():
            stage = e.get("stage")
            if stage in ("CAP", "FUSE", "ANOM", "HOLD", "FREEZE", "BRAKE") and (e.get("lever") in (None, "lambda") or key.startswith("lambda:")):
                fn = e.get("id") or key.split(":", 1)[-1]
                held_fn[fn] = stage
        by = {}
        for fn, stage in held_fn.items():
            by.setdefault(stage, []).append(fn)
        for stage, fns in sorted(by.items()):
            r.log("%s (%d): %s" % (stage, len(fns), ", ".join(sorted(fns))))
        if not held_fn:
            r.log("no lambda held by the ledger")

        r.section("freshness today (declared outputs written after today's trigger minute)")
        man = json.loads((ROOT / "engine-manifest.json").read_text(encoding="utf-8"))
        keys = {e["engine"]: [k for k in e["keys"] if k.endswith(".json") and "{" not in k and "*" not in k] for e in man["engines"]}
        sched = json.loads((ROOT / "config/schedule-manifest.json").read_text(encoding="utf-8"))
        fan = json.loads((ROOT / "config/fanout-manifest.json").read_text(encoding="utf-8"))
        today = now.date()
        day0 = datetime(today.year, today.month, today.day, tzinfo=timezone.utc)
        due_cut = now - timedelta(minutes=GRACE_MIN)
        cut_min = due_cut.hour * 60 + due_cut.minute if due_cut.date() == today else -1
        minute, tick_min, skipped, no_minute = {}, {}, {"rate": 0, "monthly": 0, "weekly-not-today": 0, "no-single-minute": 0}, []
        for sec in ("rules", "schedules"):
            for e in sched[sec]:
                if e.get("state") != "ENABLED":
                    continue
                ok, kind = due_today(e.get("expr"), today)
                m = po.minute_of(e.get("expr"))
                if not ok:
                    skipped["weekly-not-today" if kind == "weekly" else kind] += 1
                    continue
                if m is None:                       # e.g. cron(0/5 9-16 ...) - the intraday lane; not a once-a-day slot
                    skipped["no-single-minute"] += 1
                    no_minute.append("%s %s" % (e.get("name"), e.get("expr")))
                    continue
                fns = sorted({po.fn_of(t.get("arn")) for t in e.get("targets") or [] if po.fn_of(t.get("arn"))})
                if "justhodl-scheduler" in fns:
                    for t in e["targets"]:
                        mm = re.search(r'"tick":\s*"([^"]+)"', t.get("input") or "")
                        if mm:
                            tick_min[mm.group(1)] = m
                    continue
                for fn in fns:
                    minute.setdefault(fn, []).append(m)
        for tick, members in fan["ticks"].items():
            if tick in tick_min:
                for fn in members:
                    minute.setdefault(fn, []).append(tick_min[tick])
        due = {fn: sorted(m for m in mins if 0 <= m <= cut_min) for fn, mins in minute.items()}
        due = {fn: mins for fn, mins in due.items() if mins}
        targets = [(fn, k) for fn in sorted(due) if keys.get(fn) for k in keys[fn][:2]]
        r.log("daily schedules assessed: %d engines; due today before %s UTC: %d (with declared json outputs: %d, files %d); skipped %s" % (
            len(minute), due_cut.strftime("%H:%M"), len(due), len({fn for fn, _ in targets}), len(targets), json.dumps(skipped)))
        if no_minute:
            r.log("schedules without a single daily minute (not assessed): %s" % "; ".join(no_minute[:10]))

        def head(t):
            fn, k = t
            try:
                h = s3.head_object(Bucket=LIVE, Key=k)
                return fn, k, h["LastModified"].astimezone(timezone.utc), None
            except Exception as ex:  # noqa: BLE001
                return fn, k, None, type(ex).__name__

        newest, errs = {}, {}
        with cf.ThreadPoolExecutor(max_workers=24) as ex:
            for fn, k, lm, err in ex.map(head, targets):
                if lm is None:
                    errs.setdefault(fn, []).append("%s:%s" % (k, err))
                elif fn not in newest or lm > newest[fn]:
                    newest[fn] = lm
        expect = {fn: day0 + timedelta(minutes=due[fn][0] - EARLY_MIN) for fn in due}
        fresh = sorted(fn for fn, lm in newest.items() if lm >= expect[fn])
        stale_held = sorted(fn for fn, lm in newest.items() if lm < expect[fn] and fn in held_fn)
        stale_free = sorted(fn for fn, lm in newest.items() if lm < expect[fn] and fn not in held_fn)
        missing_only = sorted(fn for fn in errs if fn not in newest)
        r.log("written after today's trigger minute: %d engines" % len(fresh))
        r.log("due but not written, held by the governor (%d; not re-invoked): %s" % (
            len(stale_held), ", ".join("%s[%s]" % (fn, held_fn[fn]) for fn in stale_held)))
        r.log("due but not written, NOT held (%d): %s" % (len(stale_free), ", ".join(
            "%s (last %s, due %s)" % (fn, newest[fn].strftime("%m-%d %H:%M"), ",".join(po.hhmm(m) for m in sorted(set(due[fn])))) for fn in stale_free)))
        r.log("declared outputs missing from the live bucket (%d engines): %s" % (len(missing_only), "; ".join(
            "%s %s" % (fn, errs[fn]) for fn in missing_only[:40])))
        ages = sorted(((now - lm).total_seconds() / 86400.0, fn) for fn, lm in newest.items() if lm < expect[fn])
        if ages:
            r.log("oldest among the stale: %s" % ", ".join("%s %.1fd" % (fn, d) for d, fn in ages[-12:][::-1]))
        wrote_24h = sorted(fn for fn, lm in newest.items() if lm < expect[fn] and lm >= now - timedelta(hours=26))
        r.log("stale today but written within the last 26 h (missed only today's slot so far): %s" % ", ".join(wrote_24h))

        r.section("issue #125 - %s (read-only)" % STORM_FN)
        start = now - timedelta(hours=24)
        tot = {}
        for metric in ("Invocations", "Errors", "Throttles"):
            pts = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=metric, Dimensions=[{"Name": "FunctionName", "Value": STORM_FN}],
                                           StartTime=start, EndTime=now, Period=3600, Statistics=["Sum"])["Datapoints"]
            tot[metric] = sum(p["Sum"] for p in pts)
            hot = sorted((p["Timestamp"].strftime("%m-%d %H:00"), int(p["Sum"])) for p in pts if p["Sum"] > 0)
            r.log("%s last 24 h: %d  by hour: %s" % (metric, tot[metric], json.dumps(hot[:30])))
        try:
            ev = logs.filter_log_events(logGroupName="/aws/lambda/%s" % STORM_FN, startTime=int((now - timedelta(hours=6)).timestamp() * 1000),
                                        filterPattern="?Traceback ?RuntimeError ?ERROR", limit=40)["events"]
            seen = 0
            for e in ev[-12:]:
                r.log("  log %s: %s" % (datetime.fromtimestamp(e["timestamp"] / 1000, timezone.utc).strftime("%H:%M:%S"), e["message"].strip()[:700]))
                seen += 1
            r.log("error log lines read (last 6 h): %d" % seen)
        except Exception as ex:  # noqa: BLE001
            r.log("log read skipped: %s" % type(ex).__name__)
        cfgf = lam.get_function_configuration(FunctionName=STORM_FN)
        r.log("function: version %s, last modified %s, state %s, reserved concurrency %s" % (
            cfgf.get("Version"), cfgf.get("LastModified"), cfgf.get("State"), json.dumps(cfgf.get("ReservedConcurrentExecutions"))))
        for k in ("data/polygon-futures-curves.json", "data/futures-curves.json", "data/polygon/futures-curves.json"):
            try:
                h = s3.head_object(Bucket=LIVE, Key=k)
                r.log("output %s last modified %s" % (k, h["LastModified"].astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")))
            except Exception:  # noqa: BLE001
                pass
        for k in keys.get(STORM_FN, [])[:4]:
            try:
                h = s3.head_object(Bucket=LIVE, Key=k)
                r.log("declared output %s last modified %s" % (k, h["LastModified"].astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")))
            except Exception as ex:  # noqa: BLE001
                r.log("declared output %s: %s" % (k, type(ex).__name__))

        r.section("governor guardrail self-check and dead-man alarms (read-only)")
        r.log("guardrail check (state.guardrails): %s" % json.dumps(state.get("guardrails"), default=str)[:1200])
        r.log("storage days (state, last 4): %s" % json.dumps(((state.get("storage") or {}).get("days") or [])[-4:], default=str)[:2000])
        alarm_states = {}
        try:
            names = ["justhodl-spend-governor-heartbeat", "justhodl-spend-governor-errors", "justhodl-runaway-concurrency",
                     "justhodl-runaway-invocations", "justhodl-runaway-duration", "justhodl-event-breaker-open",
                     "justhodl-event-breaker-unavailable", "justhodl-s3-live-bucket-size", "justhodl-llm-providers-down"]
            for a in cw.describe_alarms(AlarmNames=names)["MetricAlarms"]:
                alarm_states[a["AlarmName"]] = "%s%s (since %s)" % (a["StateValue"], "" if a.get("ActionsEnabled") else " [actions DISABLED]",
                                                                   a["StateUpdatedTimestamp"].strftime("%m-%d %H:%M"))
            for n in names:
                r.log("  %s: %s" % (n, alarm_states.get(n, "MISSING")))
        except Exception as ex:  # noqa: BLE001
            r.log("alarm read skipped: %s" % type(ex).__name__)
        not_ok = sorted(n for n, s in alarm_states.items() if not s.startswith("OK"))
        if not_ok:
            r.warn("alarms not OK: %s" % ", ".join(not_ok))

        r.kv(version=st.get("version"), stage=st.get("stage"), cap=st.get("cap"), budget_actual=st.get("budget_actual"),
             budget_forecast=st.get("budget_forecast"), capped=len(st.get("capped") or []), anomaly_held=len(st.get("anomaly_held") or []),
             held_lambdas=len(held_fn), due_engines=len(due), fresh=len(fresh), stale_held=len(stale_held), stale_free=len(stale_free),
             missing=len(missing_only), storage_versioning=stor.get("versioning"), storage_day=stor.get("day"),
             storm_invocations_24h=int(tot.get("Invocations", 0)), storm_errors_24h=int(tot.get("Errors", 0)),
             alarms_not_ok=len(not_ok))
        if stale_free or missing_only:
            r.warn("%d due engines have not written after today's trigger minute and are not held; %d have declared outputs missing" % (len(stale_free), len(missing_only)))
        r.ok("status read; nothing written, nothing invoked but the governor's read-only actions")
    sys.exit(0)


if __name__ == "__main__":
    main()
