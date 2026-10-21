"""ops 6590 - READ-ONLY daily health status (2026-10-21, the scheduled daily health run).

Three reads, nothing written to any bucket, no engine invoked except the governor's read-only actions:

  1. justhodl-spend-governor {"action":"status"}: version, mode, stage, budget actual / forecast, the cap in force,
     held counts by stage/lever, CAP and ANOM lists, the fuse record, the storage record (last assessment, versioning,
     warn/trip days, S3 and per-service dollars).
  2. justhodl-spend-governor {"action":"caps"}: the meter's month-to-date top engines (read-only listing).
  3. Freshness since the thaw (restore event 2026-10-21T00:29:25Z, issue #118): every ENABLED daily schedule whose
     trigger minute has passed since the thaw (plus 45 min of grace) should have written its declared data/*.json
     outputs (engine-manifest.json). HEAD each (up to 2 per engine); an engine whose newest declared output predates
     the thaw is "due since thaw, not written". Engines the governor still holds (CAP / FUSE / ANOM / operator hold)
     are listed separately and are NOT re-invoked - the health run never releases a hold.

Weekly / monthly / rate() schedules are not assessed (they were not necessarily due in the window).
Budget: <= 1,200 HEAD, two governor invokes, under 3 minutes.
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
THAW = datetime(2026, 10, 21, 0, 29, 25, tzinfo=timezone.utc)   # spend-governor/events/20261021T002925.519278Z-restore.json
GRACE_MIN = 45
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
    """'MON-FRI' / 'MON,WED,FRI' / '*' / '?' -> set of day names (None = every day)."""
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
        return False, "rate"                             # rate() - trigger minute unknown
    dom, dow = m.group(3), m.group(5)
    if dom not in ("*", "?"):
        return False, "monthly"
    days = dow_set(dow)
    if days is None:
        return True, "daily"
    return (DOW[today.weekday()] in days), "weekly"


def main():
    lam, s3 = c("lambda"), c("s3")
    now = datetime.now(timezone.utc)
    with report("ops_6590_daily_health_status") as r:
        r.heading("ops 6590 - daily health status (read-only), %s" % now.strftime("%Y-%m-%d %H:%M UTC"))

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
        r.log("storage last assessment: %s" % json.dumps(stor.get("last"), default=str)[:1200])
        r.log("storage s3_usd (by day): %s" % json.dumps(stor.get("s3_usd"), default=str)[:600])
        r.log("service spend (by day): %s" % json.dumps(stor.get("svc_usd"), default=str)[:1500])
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

        r.section("freshness since the thaw (%s)" % THAW.strftime("%Y-%m-%d %H:%M UTC"))
        man = json.loads((ROOT / "engine-manifest.json").read_text(encoding="utf-8"))
        keys = {e["engine"]: [k for k in e["keys"] if k.endswith(".json") and "{" not in k and "*" not in k] for e in man["engines"]}
        sched = json.loads((ROOT / "config/schedule-manifest.json").read_text(encoding="utf-8"))
        fan = json.loads((ROOT / "config/fanout-manifest.json").read_text(encoding="utf-8"))
        today = now.date()
        thaw_min = THAW.hour * 60 + THAW.minute
        due_cut = (now - timedelta(minutes=GRACE_MIN))
        cut_min = due_cut.hour * 60 + due_cut.minute if due_cut.date() == today else 24 * 60
        minute, tick_min, skipped = {}, {}, {"rate": 0, "monthly": 0, "weekly-not-today": 0}
        for sec in ("rules", "schedules"):
            for e in sched[sec]:
                if e.get("state") != "ENABLED":
                    continue
                ok, kind = due_today(e.get("expr"), today)
                m = po.minute_of(e.get("expr"))
                if not ok or m is None:
                    skipped["weekly-not-today" if kind == "weekly" else kind] += 1
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
        due = {fn: mins for fn, mins in minute.items() if any(thaw_min <= m <= cut_min for m in mins)}
        targets = [(fn, k) for fn in sorted(due) if keys.get(fn) for k in keys[fn][:2]]
        r.log("daily schedules assessed: %d engines; due since the thaw and before %s: %d (with declared json outputs: %d, files %d); skipped %s" % (
            len(minute), due_cut.strftime("%H:%M"), len(due), len({fn for fn, _ in targets}), len(targets), json.dumps(skipped)))

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
        fresh = sorted(fn for fn, lm in newest.items() if lm >= THAW)
        stale_held = sorted(fn for fn, lm in newest.items() if lm < THAW and fn in held_fn)
        stale_free = sorted(fn for fn, lm in newest.items() if lm < THAW and fn not in held_fn)
        missing_only = sorted(fn for fn in errs if fn not in newest)
        r.log("written since the thaw: %d engines" % len(fresh))
        r.log("due but not written, held by the governor (%d; not re-invoked): %s" % (
            len(stale_held), ", ".join("%s[%s]" % (fn, held_fn[fn]) for fn in stale_held)))
        r.log("due but not written, NOT held (%d): %s" % (len(stale_free), ", ".join(
            "%s (last %s, due %s)" % (fn, newest[fn].strftime("%m-%d %H:%M"), ",".join(po.hhmm(m) for m in sorted(set(due[fn])))) for fn in stale_free)))
        r.log("declared outputs missing from the live bucket (%d engines): %s" % (len(missing_only), "; ".join(
            "%s %s" % (fn, errs[fn]) for fn in missing_only[:40])))
        ages = sorted(((now - lm).total_seconds() / 86400.0, fn) for fn, lm in newest.items() if lm < THAW)
        if ages:
            r.log("oldest among the stale: %s" % ", ".join("%s %.1fd" % (fn, d) for d, fn in ages[-10:][::-1]))

        r.kv(version=st.get("version"), stage=st.get("stage"), cap=st.get("cap"), budget_actual=st.get("budget_actual"),
             budget_forecast=st.get("budget_forecast"), capped=len(st.get("capped") or []), anomaly_held=len(st.get("anomaly_held") or []),
             held_lambdas=len(held_fn), due_engines=len(due), fresh=len(fresh), stale_held=len(stale_held), stale_free=len(stale_free),
             missing=len(missing_only), storage_versioning=stor.get("versioning"), storage_day=stor.get("day"))
        if stale_free or missing_only:
            r.warn("%d due engines have not written since the thaw and are not held; %d have declared outputs missing" % (len(stale_free), len(missing_only)))
        r.ok("status read; nothing written, nothing invoked but the governor's read-only actions")
    sys.exit(0)


if __name__ == "__main__":
    main()
