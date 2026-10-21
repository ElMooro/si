"""ops 6600 - Polygon US append for the chart's bar bank (Khalid, 2026-10-21 11:52 ET: "do Polygon US append").

After deploy-lambdas ships justhodl-tv-bars v1.2 (mode=polygon-append): (1) create the EventBridge Scheduler schedule
justhodl-tv-bars-polygon-append - cron(52 22 ? * MON-FRI *) UTC, input {"mode":"polygon-append"}, the same role and
retry policy as the function's existing universe-refresh schedule, and declare it in the deployed manifest
(additions-only, live-verified, ops 6594 pattern); (2) run the append once now so the bank advances past 2026-10-19
today (before the close it appends through yesterday's session; tonight's run appends today's); (3) verify from the
bank itself: AAPL's document gains the sessions, stamped 09:30 America/New_York like every row before them, with the
per-bar label polygon-aggs:daily-adjusted, and the worker's /ohlc answer for AAPL ends on the new session.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import io
import json
import re
import sys
import time
import urllib.request

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
FN = "justhodl-tv-bars"
SCHED = "justhodl-tv-bars-polygon-append"
REF = "justhodl-tv-bars-universe-refresh"
EXPR = "cron(52 22 ? * MON-FRI *)"
IDX = "data/warm/tv-bars/universe/_index.json"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=600)


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def fn_of(arn):
    m = re.search(r":function:([^/:]+)", arn or "")
    return m.group(1) if m else arn


def entries(m):
    return {(e["kind"], e["name"]): e for sec in ("rules", "schedules") for e in m.get(sec, [])}


def bank(s3, key):
    raw = s3.get_object(Bucket=LIVE, Key=key)["Body"].read()
    with gzip.GzipFile(fileobj=io.BytesIO(raw)) as g:
        return json.loads(g.read())


def main():
    s3, lam, sch = c("s3"), c("lambda"), c("scheduler")
    with report("ops_6600_polygon_append_schedule") as r:
        r.heading("ops 6600 - tv-bars polygon-append: schedule, declaration, first run, bank verification")
        r.section("1. the deployed function carries v1.2")
        cf = lam.get_function_configuration(FunctionName=FN)
        r.log("%s modified %s, timeout %s, memory %s" % (FN, (cf.get("LastModified") or "")[:19], cf.get("Timeout"), cf.get("MemorySize")))
        src = (ROOT / "aws/lambdas/justhodl-tv-bars/source/lambda_function.py").read_text(encoding="utf-8")
        if "def polygon_append" not in src:
            r.fail("repo source has no polygon_append - wrong checkout")
            raise SystemExit(1)
        probe = lam.invoke(FunctionName=FN, Payload=json.dumps({"mode": "polygon-append", "symbols": ["__none__"]}).encode())
        body = json.loads(probe["Payload"].read() or b"{}")
        if probe.get("FunctionError") or body.get("mode") != "polygon-append":
            r.fail("deployed function does not answer mode=polygon-append yet: %s - run again after deploy-lambdas" % json.dumps(body)[:300])
            raise SystemExit(1)
        r.ok("deployed function answers mode=polygon-append (dry: %s)" % json.dumps(body)[:200])

        r.section("2. schedule")
        ref = sch.get_schedule(Name=REF, GroupName="default")
        role = ref["Target"]["RoleArn"]
        try:
            live = sch.get_schedule(Name=SCHED, GroupName="default")
            r.log("schedule exists: %s %s" % (live.get("ScheduleExpression"), live.get("State")))
        except sch.exceptions.ResourceNotFoundException:
            sch.create_schedule(Name=SCHED, GroupName="default", ScheduleExpression=EXPR, ScheduleExpressionTimezone="UTC", State="ENABLED",
                                FlexibleTimeWindow={"Mode": "OFF"}, Description="tv-bars v1.2: Polygon grouped-daily append of the US sessions TradingView refuses (ops 6600, 2026-10-21)",
                                Target={"Arn": cf["FunctionArn"], "RoleArn": role, "Input": json.dumps({"mode": "polygon-append"}, separators=(",", ":")),
                                        "RetryPolicy": ref["Target"].get("RetryPolicy") or {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600}})
            live = sch.get_schedule(Name=SCHED, GroupName="default")
            r.ok("created %s: %s %s target %s role %s" % (SCHED, live.get("ScheduleExpression"), live.get("State"), fn_of(live["Target"]["Arn"]), role.split("/")[-1]))
        if live.get("ScheduleExpression") != EXPR or live.get("State") != "ENABLED" or fn_of(live["Target"]["Arn"]) != FN:
            r.fail("schedule does not match the declaration")
            raise SystemExit(1)
        # declare: deployed manifest := committed, additions-only, live-verified
        repo_text = (ROOT / KEY).read_text(encoding="utf-8")
        repo = json.loads(repo_text)
        old_text = s3.get_object(Bucket=LIVE, Key=KEY)["Body"].read().decode("utf-8")
        a, b = entries(json.loads(old_text)), entries(repo)
        removed = sorted(k for k in a if k not in b)
        added = sorted(k for k in b if k not in a)
        changed = sorted(k for k in a if k in b and {x: a[k][x] for x in ("expr", "state")} != {x: b[k][x] for x in ("expr", "state")})
        if removed or changed:
            r.fail("not additions-only: removed %s changed %s" % (removed[:5], changed[:5]))
            raise SystemExit(1)
        for kind, name in added:
            e = b[(kind, name)]
            lv = sch.get_schedule(Name=name, GroupName=e.get("group") or "default")
            if not (lv.get("ScheduleExpression") == e["expr"] and lv.get("State") == e["state"] and fn_of(lv["Target"]["Arn"]) == fn_of(e["targets"][0]["arn"])):
                r.fail("declared %s differs from live" % name)
                raise SystemExit(1)
        if added:
            stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
            s3.put_object(Bucket=PRIVATE, Key="backups/schedule-manifest/%s/schedule-manifest.json" % stamp, Body=old_text.encode("utf-8"), ServerSideEncryption="AES256")
            s3.put_object(Bucket=LIVE, Key=KEY, Body=repo_text.encode("utf-8"), ContentType="application/json", CacheControl="no-cache")
            r.ok("manifest declared %s (backup backups/schedule-manifest/%s/)" % ([n for _, n in added], stamp))
        else:
            r.ok("manifest already equal")

        r.section("3. first append now")
        idx0 = json.loads(s3.get_object(Bucket=LIVE, Key=IDX)["Body"].read())
        aapl_key = (idx0.get("symbols") or {}).get("US:AAPL", {}).get("key") or (idx0.get("symbols") or {}).get("NASDAQ:AAPL", {}).get("key")
        before = bank(s3, aapl_key) if aapl_key else {}
        r.log("AAPL bank before: key %s n %s last %s source %s" % (aapl_key, before.get("n"), before.get("last_date"), before.get("source")))
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, Payload=json.dumps({"mode": "polygon-append"}).encode())
        out = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError"):
            r.fail("append failed: %s" % json.dumps(out)[:600])
            raise SystemExit(1)
        r.log("append (%.0fs): %s" % (time.time() - t0, json.dumps(out)[:1200]))
        r.kv(step="polygon-append first run", through=out.get("through"), calls=out.get("calls"), appended_symbols=out.get("appended_symbols"), appended_rows=out.get("appended_rows"), failed=out.get("failed"), skipped=out.get("skipped"), gap_older=out.get("gap_older_than_window"))
        after = bank(s3, aapl_key) if aapl_key else {}
        tail = after.get("bars", [])[-3:]
        labels = (after.get("bar_sources") or [])[-3:]
        r.log("AAPL bank after: n %s last %s source %s; last 3 bars %s labels %s" % (after.get("n"), after.get("last_date"), after.get("source"),
                                                                                    [(datetime.fromtimestamp(b[0], tz=timezone.utc).isoformat(), b[4]) for b in tail], labels))
        ok_ts = all(datetime.fromtimestamp(b[0], tz=timezone.utc).strftime("%H:%M") in ("13:30", "14:30") for b in tail)
        if after.get("last_date") == before.get("last_date") or not ok_ts:
            r.fail("AAPL bank did not advance cleanly (last %s -> %s, stamps ok %s)" % (before.get("last_date"), after.get("last_date"), ok_ts))
            raise SystemExit(1)
        idx1 = json.loads(s3.get_object(Bucket=LIVE, Key=IDX)["Body"].read())
        from collections import Counter
        lc = Counter((v.get("last") or "")[:10] for v in (idx1.get("symbols") or {}).values())
        r.log("index last-bar dates now: %s; polygon_append record: %s" % (json.dumps(lc.most_common(5)), json.dumps(idx1.get("polygon_append"))[:400]))

        r.section("4. the worker serves it")
        try:
            req = urllib.request.Request("https://justhodl-data-proxy.raafouis.workers.dev/ohlc?ticker=AAPL&span=day&mult=1&days=5&nocache=%d" % int(time.time()), headers={"User-Agent": "justhodl-ops/6600", "Cache-Control": "no-cache"})
            with urllib.request.urlopen(req, timeout=60) as resp2:
                doc = json.loads(resp2.read())
            bars = doc.get("bars") or []
            last = bars[-1] if bars else {}
            r.log("worker /ohlc AAPL: source %s, %d bars, last %s close %s" % (doc.get("source"), len(bars), datetime.fromtimestamp(last.get("time", 0), tz=timezone.utc).isoformat() if last else None, last.get("close")))
            if last and datetime.fromtimestamp(last["time"], tz=timezone.utc).strftime("%Y-%m-%d") != after.get("last_date"):
                r.warn("the edge still serves the previous bank (60 s cache on the symdir fetch) - re-check in a minute")
        except Exception as e:  # noqa: BLE001
            r.warn("worker check: %s" % type(e).__name__)
        r.ok("schedule %s live and declared; bank advanced to %s for %s names" % (SCHED, after.get("last_date"), out.get("appended_symbols")))


if __name__ == "__main__":
    main()
