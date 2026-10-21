"""ops 6603 - first light (evening) follow-up: the three New York-zone schedules the pipeline planner moved with a UTC
reading of their crons go back to their original times; the clock bug is fixed in the planner (scripts/pipeline_order.py
tz_offset_min / utc_minute, 2026-10-21).

ops 6586 (23:36 UTC): 72 of 75 retimed triggers fired at their new minute; the three that did not are the only
ENABLED schedules whose ScheduleExpressionTimezone is America/New_York - justhodl-auction-desk-buyback (12:10 ->
13:51), justhodl-bond-warroom-after-auction (13:35 -> 13:56), justhodl-master-ranker-weekday-close (16:15 -> 17:05).
The planner thought those crons were UTC; in New York time each already fired after its writers, so no move was
needed.  Here: (1) read-only - the live timezone of the three, and their invocations today at the real UTC minutes
of the moved crons (17:51 / 17:56 / 21:05) so the day's run is accounted for; (2) restore the original expressions
(expression only; timezone, target, role, input, retry untouched; previous definitions backed up to the private
bucket); (3) deployed manifest := committed (exactly these three entries change, live-verified, ops 6594 pattern);
(4) read-only - why justhodl-polygon-futures-curves invoked 196 times on the 5-minute lane today with 98 errors (one
error + one retry per tick) - its last error lines.  Limits: nothing faster than daily; no caps, budgets, IAM, lifecycle
or archive changes.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import copy
import json
import re
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
from ops_report import report  # noqa: E402
from scheduler_payload import WRITABLE  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120)
RESTORE = {  # name: (original expression, the mistaken expression, the function)
    "justhodl-auction-desk-buyback": ("cron(10 12 ? * MON-FRI *)", "cron(51 13 ? * MON-FRI *)", "justhodl-auction-desk"),
    "justhodl-bond-warroom-after-auction": ("cron(35 13 ? * MON-FRI *)", "cron(56 13 ? * MON-FRI *)", "justhodl-bond-warroom"),
    "justhodl-master-ranker-weekday-close": ("cron(15 16 ? * MON-FRI *)", "cron(5 17 ? * MON-FRI *)", "justhodl-master-ranker"),
}
TZ = "America/New_York"
NY_OFFSET_MIN = 240  # EDT on 2026-10-21


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def entries(m):
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in m.get(sec, [])}


def cron_minute(expr):
    m = re.match(r"cron\((\d{1,2}) (\d{1,2}) ", expr)
    return int(m.group(2)) * 60 + int(m.group(1))


def invocations_at(cw, fn, utc_minute, day, width=6):
    start = day.replace(hour=utc_minute // 60, minute=utc_minute % 60, second=0, microsecond=0)
    res = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName="Invocations", Dimensions=[{"Name": "FunctionName", "Value": fn}],
                                   StartTime=start, EndTime=start + timedelta(minutes=width), Period=60, Statistics=["Sum"])
    return int(sum(p["Sum"] for p in res.get("Datapoints", [])))


def insights(logs, group, query, hours, limit=20):
    now = datetime.now(timezone.utc)
    try:
        qid = logs.start_query(logGroupName=group, startTime=int((now - timedelta(hours=hours)).timestamp()), endTime=int(now.timestamp()), queryString=query, limit=limit)["queryId"]
    except Exception as e:  # noqa: BLE001
        return "start:%s" % type(e).__name__, []
    for _ in range(40):
        time.sleep(2)
        r = logs.get_query_results(queryId=qid)
        if r["status"] in ("Complete", "Failed", "Cancelled", "Timeout"):
            return r["status"], [{f["field"]: f["value"] for f in row} for row in r.get("results", [])]
    return "Running", []


def main():
    sch, cw, s3, logs = c("scheduler"), c("cloudwatch"), c("s3"), c("logs")
    now = datetime.now(timezone.utc)
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    with report("ops_6603_ny_schedules_restore") as r:
        r.heading("ops 6603 - the three New York-zone schedules: today's run accounted for, original times restored, manifest declared")
        r.section("1. live state and today's firing (read-only)")
        live_before = {}
        for name, (orig, moved, fn) in RESTORE.items():
            s = sch.get_schedule(Name=name, GroupName="default")
            live_before[name] = s
            tz = s.get("ScheduleExpressionTimezone")
            utc_m = (cron_minute(s["ScheduleExpression"]) + NY_OFFSET_MIN) % 1440
            n = invocations_at(cw, fn, utc_m, today) if utc_m <= now.hour * 60 + now.minute else None
            r.log("%s: live %s tz %s %s target %s -> real UTC minute %02d:%02d today: %s invocations in 6 min" % (
                name, s["ScheduleExpression"], tz, s.get("State"), fn_of(s["Target"]["Arn"]), utc_m // 60, utc_m % 60, "not due yet" if n is None else n))
            if tz != TZ:
                r.fail("%s is not a %s schedule (%s) - nothing to restore here" % (name, TZ, tz))
                raise SystemExit(1)
            if s["ScheduleExpression"] != moved:
                r.warn("%s live expression %s is not the one the plan wrote (%s); restoring to %s anyway only if it matches the plan's 'after'" % (name, s["ScheduleExpression"], moved, orig))
        r.section("2. restore the original expressions")
        stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        s3.put_object(Bucket=PRIVATE, Key="backups/pipeline-order/%s/ny-schedules-before.json" % stamp, Body=json.dumps({k: {kk: vv for kk, vv in v.items() if kk in WRITABLE} for k, v in live_before.items()}, default=str).encode(), ServerSideEncryption="AES256")
        restored = 0
        for name, (orig, moved, fn) in RESTORE.items():
            s = live_before[name]
            if s["ScheduleExpression"] == orig:
                r.ok("%s already at %s" % (name, orig))
                restored += 1
                continue
            if s["ScheduleExpression"] != moved:
                r.warn("%s: skipped - live expression is neither the plan's nor the original" % name)
                continue
            kw = {k: copy.deepcopy(v) for k, v in s.items() if k in WRITABLE}
            kw["ScheduleExpression"] = orig
            sch.update_schedule(**kw)
            back = sch.get_schedule(Name=name, GroupName="default")
            ok = back.get("ScheduleExpression") == orig and back.get("ScheduleExpressionTimezone") == TZ and back.get("State") == s.get("State") and back["Target"]["Arn"] == s["Target"]["Arn"]
            r.kv(schedule=name, before=s["ScheduleExpression"], after=back.get("ScheduleExpression"), tz=back.get("ScheduleExpressionTimezone"), verified=ok)
            if not ok:
                r.fail("%s read-back differs" % name)
                raise SystemExit(1)
            r.ok("%s: %s -> %s (%s; target/role/input/retry unchanged)" % (name, s["ScheduleExpression"], orig, TZ))
            restored += 1
        r.section("3. deployed manifest := committed")
        repo_text = (ROOT / KEY).read_text(encoding="utf-8")
        repo = json.loads(repo_text)
        old_text = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        a, b = entries(json.loads(old_text)), entries(repo)
        removed = sorted(k for k in a if k not in b)
        added = sorted(k for k in b if k not in a)
        changed = sorted(k for k in a if k in b and {x: a[k][x] for x in ("expr", "state")} != {x: b[k][x] for x in ("expr", "state")})
        r.log("S3 %d entries, repo %d; added %s removed %d changed %s" % (len(a), len(b), [n for _, n in added], len(removed), [n for _, n in changed]))
        if removed or added or any(n not in RESTORE for _, n in changed):
            r.fail("the manifest difference is not just the three restored schedules")
            raise SystemExit(1)
        for kind, name in changed:
            e = b[(kind, name)]
            lv = sch.get_schedule(Name=name, GroupName="default")
            if not (lv.get("ScheduleExpression") == e["expr"] and lv.get("State") == e["state"] and lv.get("ScheduleExpressionTimezone") == e.get("timezone") and fn_of(lv["Target"]["Arn"]) == fn_of(e["targets"][0]["arn"])):
                r.fail("declared %s does not match live after the restore" % name)
                raise SystemExit(1)
        if changed:
            s3.put_object(Bucket=PRIVATE, Key="backups/schedule-manifest/%s/schedule-manifest.json" % stamp, Body=old_text.encode("utf-8"), ServerSideEncryption="AES256")
            s3.put_object(Bucket=LIVE, Key=KEY, Body=repo_text.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
            if s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8") != repo_text:
                r.fail("read-back differs")
                raise SystemExit(1)
            r.ok("manifest declared the restored times (backup backups/schedule-manifest/%s/)" % stamp)
        else:
            r.ok("manifest already equal")
        r.section("4. polygon-futures-curves on the lane (read-only)")
        fn = "justhodl-polygon-futures-curves"
        status, rows = insights(logs, "/aws/lambda/" + fn, 'fields @timestamp, @message | filter @message like /Traceback|Task timed out|\\[ERROR\\]|Error|Exception|429|403/ | sort @timestamp desc | limit 12', 4, limit=12)
        msgs = []
        for x in rows:
            m = x.get("@message", "").replace("\n", " ")[:300]
            if m and m not in msgs:
                msgs.append(m)
        r.log("%s errors (%s): %s" % (fn, status, json.dumps(msgs[:6])[:2400]))
        status2, rows2 = insights(logs, "/aws/lambda/" + fn, 'fields @message | filter @message like /REPORT/ | stats count() as n, avg(@duration) as ms, max(@duration) as maxms, max(@maxMemoryUsed) as mem by bin(1h) | sort @timestamp desc', 4, limit=6)
        r.log("  per hour (%s): %s" % (status2, json.dumps([(x.get("bin(1h)", "")[11:16], int(float(x.get("n", 0))), round(float(x.get("ms", 0) or 0)), round(float(x.get("maxms", 0) or 0)), x.get("mem")) for x in rows2])))
        try:
            lam = c("lambda")
            cf = lam.get_function_configuration(FunctionName=fn)
            ev = lam.get_function_event_invoke_config(FunctionName=fn) if hasattr(lam, "get_function_event_invoke_config") else {}
            r.log("  config: timeout %s s, memory %s MB; async retries %s" % (cf.get("Timeout"), cf.get("MemorySize"), ev.get("MaximumRetryAttempts", "default 2")))
        except Exception as e:  # noqa: BLE001
            r.log("  config: %s" % type(e).__name__)
        r.ok("%d/3 New York schedules at their original times; manifest in step; futures-curves errors read" % restored)


if __name__ == "__main__":
    main()
