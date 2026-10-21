"""ops 6586 - READ-ONLY: did the new timetable fire?  (the proof after ops 6585, run after 11:35 UTC)

For every retimed trigger in config/pipeline-order-plan.json whose new minute has passed today: did each of its
functions get invoked in the five minutes from the new time (CloudWatch Invocations, 60 s period), and NOT at the
old minute?  For the late tick: did justhodl-scheduler log `tick=daily-morn-late ok=3`, and did the three members run
after 11:15 UTC?  Also lists any retimed function whose Errors > 0 in its new window.  Nothing is changed.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)
CRON = re.compile(r"^cron\((\d{1,2}) (\d{1,2}) ")


def minute_of(expr, tz_name="UTC"):
    """UTC minute of a daily cron; a schedule in another zone (manifest "timezone") is shifted by that zone's offset today
    (2026-10-21 evening pass: three America/New_York schedules were judged on their cron digits as if UTC)."""
    m = CRON.match(expr)
    minute = int(m.group(2)) * 60 + int(m.group(1))
    if tz_name and tz_name != "UTC":
        try:
            from zoneinfo import ZoneInfo
            minute = (minute - int(datetime.now(timezone.utc).astimezone(ZoneInfo(tz_name)).utcoffset().total_seconds() // 60)) % 1440
        except Exception:  # noqa: BLE001
            pass
    return minute


def main():
    cw, logs = boto3.client("cloudwatch", region_name=REGION, config=CFG), boto3.client("logs", region_name=REGION, config=CFG)
    sys.path.insert(0, str(ROOT / "scripts"))
    import pipeline_order as po
    plans = [json.loads(p.read_text(encoding="utf-8")) for p in po.plan_paths(ROOT)]
    plans = [p for p in plans if p.get("applied")]
    latest = {}                                                   # the last applied plan's action wins per trigger
    for p in plans:
        for a in p["actions"]:
            latest[(a["kind"], a["name"])] = a
    plan = {"actions": list(latest.values()), "late_ticks": {}, "tick_moves": {}, "intraday_lane": None}
    for p in plans:
        plan["late_ticks"].update(p["late_ticks"])
        plan["tick_moves"].update(p["tick_moves"])
        if p.get("intraday_lane"):
            plan["intraday_lane"] = p["intraday_lane"]
    now = datetime.now(timezone.utc)
    day0 = now.replace(hour=0, minute=0, second=0, microsecond=0)
    with report("ops_6586_pipeline_order_first_light") as r:
        r.heading("ops 6586 - pipeline order, first light: did the retimed triggers fire at their new minutes?")
        # one get_metric_data per batch of functions: Invocations + Errors, 60 s period, today
        fns = sorted({fn for a in plan["actions"] for fn in a["functions"]} | {fn for fn in plan["tick_moves"]} | set((plan.get("intraday_lane") or {}).get("members", [])))
        series = {}
        queries = []
        for i, fn in enumerate(fns):
            for metric in ("Invocations", "Errors"):
                queries.append({"Id": "m%d%s" % (i, metric[0].lower()), "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": metric,
                                "Dimensions": [{"Name": "FunctionName", "Value": fn}]}, "Period": 60, "Stat": "Sum"}, "ReturnData": True})
        for start in range(0, len(queries), 400):
            batch = queries[start:start + 400]
            token = None
            while True:
                kw = {"MetricDataQueries": batch, "StartTime": day0, "EndTime": now, "ScanBy": "TimestampAscending"}
                if token:
                    kw["NextToken"] = token
                res = cw.get_metric_data(**kw)
                for mr in res.get("MetricDataResults", []):
                    series.setdefault(mr["Id"], []).extend(zip(mr.get("Timestamps", []), mr.get("Values", [])))
                token = res.get("NextToken")
                if not token:
                    break
        ids = {fn: "m%d" % i for i, fn in enumerate(fns)}

        def count(fn, metric, m0, m1):
            """Sum of a function's metric over today's minutes [m0, m1)."""
            return sum(v for ts, v in series.get(ids[fn] + metric[0].lower(), []) if m0 <= ts.hour * 60 + ts.minute < m1)

        due, fired_new, fired_old, errors, missing = 0, 0, 0, [], []
        for a in plan["actions"]:
            tz = a.get("timezone") or "UTC"
            new_m, old_m = minute_of(a["after"]["expr"], tz), minute_of(a["before"]["expr"], tz)
            if new_m + 6 > now.hour * 60 + now.minute:
                continue                                           # not due yet today
            dow = a["after"]["expr"].split(" ")[4]
            if dow not in ("?", "*") and now.strftime("%a").upper() not in dow and not (dow == "MON-FRI" and now.weekday() < 5):
                continue                                           # not today's day
            due += 1
            got_new = all(count(fn, "Invocations", new_m, new_m + 6) > 0 for fn in a["functions"])
            got_old = any(count(fn, "Invocations", old_m, old_m + 3) > 0 for fn in a["functions"]) if abs(new_m - old_m) > 6 else False
            fired_new += got_new
            fired_old += got_old
            if not got_new:
                missing.append((a["name"], a["after"]["expr"]))
            for fn in a["functions"]:
                e = count(fn, "Errors", new_m, new_m + 30)
                if e:
                    errors.append((fn, int(e)))
        r.log("retimed triggers due so far today: %d; fired at the new minute: %d; still fired at the old minute: %d" % (due, fired_new, fired_old))
        for name, expr in missing[:20]:
            r.warn("no invocation in the 6 min from its new time: %s %s" % (name, expr))
        for fn, e in errors[:20]:
            r.warn("errors after the retime: %s %d" % (fn, e))
        # the late tick
        lt_ok = {}
        for name, lt in plan["late_ticks"].items():
            m = lt["minute"]
            if m + 20 > now.hour * 60 + now.minute:
                r.log("late tick %s not due yet today (%s UTC)" % (name, lt["expr"]))
                continue
            members = {fn: count(fn, "Invocations", m, m + 20) for fn in lt["members"]}
            start_ms = int((day0 + timedelta(minutes=m - 2)).timestamp() * 1000)
            end_ms = int((day0 + timedelta(minutes=m + 20)).timestamp() * 1000)
            lines = []
            try:
                res = logs.filter_log_events(logGroupName="/aws/lambda/justhodl-scheduler", startTime=start_ms, endTime=end_ms,
                                             filterPattern='"tick=%s"' % name, limit=20)
                lines = [e["message"].strip()[:160] for e in res.get("events", [])]
            except Exception as ex:  # noqa: BLE001
                r.log("scheduler log read: %s" % type(ex).__name__)
            ok = all(v > 0 for v in members.values()) and any("ok=%d" % len(lt["members"]) in ln for ln in lines)
            lt_ok[name] = ok
            r.log("late tick %s: members invoked after %s: %s; scheduler log: %s" % (name, lt["expr"], json.dumps(members), lines[:3]))
        # the intraday lane: members invoked at least every 10 minutes through the session so far (New York time)
        lane_ok, lane_note = None, ""
        lane = plan.get("intraday_lane")
        if lane:
            from zoneinfo import ZoneInfo
            ny = now.astimezone(ZoneInfo("America/New_York"))
            if ny.weekday() < 5 and ny.hour >= 10:
                start = ny.replace(hour=9, minute=0, second=0, microsecond=0)
                end = min(ny, ny.replace(hour=17, minute=0, second=0, microsecond=0))
                m0 = start.astimezone(timezone.utc); m1 = end.astimezone(timezone.utc)
                sm0, sm1 = m0.hour * 60 + m0.minute, m1.hour * 60 + m1.minute
                expected = max(1, int((m1 - m0).total_seconds() // 300))
                got = {fn: count(fn, "Invocations", sm0, sm1) for fn in lane["members"]}
                errs = {fn: count(fn, "Errors", sm0, sm1) for fn in lane["members"]}
                lane_ok = all(v >= 0.8 * expected for v in got.values()) and all(v <= 0.1 * expected for v in errs.values())
                lane_note = "expected ~%d runs each since 09:00 NY; invocations %s; errors %s" % (expected, json.dumps(got), json.dumps({k: v for k, v in errs.items() if v}))
                r.log("intraday lane: %s -> %s" % (lane_note, "ok" if lane_ok else "NOT AS PLANNED"))
            else:
                r.log("intraday lane: not in a session window yet (%s NY)" % ny.strftime("%a %H:%M"))
        r.kv(due=due, fired_new=fired_new, fired_old=fired_old, errors=len(errors), late_ticks_ok=sum(1 for v in lt_ok.values() if v), late_ticks_checked=len(lt_ok),
             lane=("ok" if lane_ok else "not as planned") if lane_ok is not None else "n/a")
        if due and fired_new >= 0.9 * due and not fired_old and all(lt_ok.values()) and lane_ok is not False:
            r.ok("first light: %d/%d retimed triggers fired at their new minutes, none at the old, late tick(s) fired" % (fired_new, due))
        elif not due and not lt_ok:
            r.ok("nothing due yet today - rerun after the new minutes have passed")
        else:
            r.fail("timetable not confirmed: %d/%d fired at the new minute, %d at the old, late ticks %s, lane %s" % (fired_new, due, fired_old, lt_ok, lane_note or "n/a"))
            raise SystemExit(1)


if __name__ == "__main__":
    main()
