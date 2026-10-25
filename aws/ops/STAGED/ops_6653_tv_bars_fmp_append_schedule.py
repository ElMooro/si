"""ops 6653 - tv-bars v1.3 mode=fmp-append: schedule, declaration, first run, bank verification (Khalid 2026-10-25:
"100% of the data pulled and used").

After deploy-lambdas ships justhodl-tv-bars v1.3: (1) create the EventBridge Scheduler schedule
justhodl-tv-bars-fmp-append - cron(58 23 * * ? *) UTC (after justhodl-fmp-walker 23:27, whose v1.2.0 daily price lane
refreshes the FMP EOD histories first), input {"mode":"fmp-append"}, the same role and retry policy as the function's
universe-refresh schedule, declared in the deployed manifest (additions-only, live-verified); (2) run the append once
now and record every verdict - the proof chain is in the output (fmp_symbol proven by list membership, continuity
check, sessions appended) and until tonight's walker run most names report fmp_history_not_banked, which is the honest
state; (3) read the index's fmp_append block and one advanced document's tail back from the bank.  Keys never printed.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import io
import json
import re
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
LIVE, PRIVATE = "justhodl-dashboard-live", "justhodl-ops-private-%s" % ACCOUNT
KEY = "config/schedule-manifest.json"
FN = "justhodl-tv-bars"
SCHED = "justhodl-tv-bars-fmp-append"
REF = "justhodl-tv-bars-universe-refresh"
EXPR = "cron(58 23 * * ? *)"
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
    with report("ops_6653_tv_bars_fmp_append_schedule") as r:
        r.heading("ops 6653 - tv-bars fmp-append: schedule, declaration, first run, bank verification")
        r.section("1. the deployed function carries v1.3")
        cf = lam.get_function_configuration(FunctionName=FN)
        r.log("%s modified %s, timeout %s, memory %s" % (FN, (cf.get("LastModified") or "")[:19], cf.get("Timeout"), cf.get("MemorySize")))
        src = (ROOT / "aws/lambdas/justhodl-tv-bars/source/lambda_function.py").read_text(encoding="utf-8")
        if "def fmp_append" not in src:
            r.fail("repo source has no fmp_append - wrong checkout")
            raise SystemExit(1)
        probe = lam.invoke(FunctionName=FN, Payload=json.dumps({"mode": "fmp-append", "symbols": ["__none__"]}).encode())
        body = json.loads(probe["Payload"].read() or b"{}")
        if probe.get("FunctionError") or body.get("mode") != "fmp-append":
            r.fail("deployed function does not answer mode=fmp-append yet: %s - run again after deploy-lambdas" % json.dumps(body)[:300])
            raise SystemExit(1)
        r.ok("deployed function answers mode=fmp-append (dry: %s)" % json.dumps(body)[:200])

        r.section("2. schedule")
        ref = sch.get_schedule(Name=REF, GroupName="default")
        role = ref["Target"]["RoleArn"]
        try:
            live = sch.get_schedule(Name=SCHED, GroupName="default")
            r.log("schedule exists: %s %s" % (live.get("ScheduleExpression"), live.get("State")))
        except sch.exceptions.ResourceNotFoundException:
            sch.create_schedule(Name=SCHED, GroupName="default", ScheduleExpression=EXPR, ScheduleExpressionTimezone="UTC", State="ENABLED",
                                FlexibleTimeWindow={"Mode": "OFF"}, Description="tv-bars v1.3: FMP-warehouse append of the non-tape chart names (indexes, forex, crypto, non-US) after the walker's daily price lane (ops 6653, 2026-10-25)",
                                Target={"Arn": cf["FunctionArn"], "RoleArn": role, "Input": json.dumps({"mode": "fmp-append"}, separators=(",", ":")),
                                        "RetryPolicy": ref["Target"].get("RetryPolicy") or {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600}})
            live = sch.get_schedule(Name=SCHED, GroupName="default")
            r.ok("created %s: %s %s target %s role %s" % (SCHED, live.get("ScheduleExpression"), live.get("State"), fn_of(live["Target"]["Arn"]), role.split("/")[-1]))
        if live.get("ScheduleExpression") != EXPR or live.get("State") != "ENABLED" or fn_of(live["Target"]["Arn"]) != FN:
            r.fail("schedule does not match the declaration")
            raise SystemExit(1)
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

        r.section("3. first append now (every verdict recorded)")
        t0 = time.time()
        resp = lam.invoke(FunctionName=FN, Payload=json.dumps({"mode": "fmp-append"}).encode())
        out = json.loads(resp["Payload"].read() or b"{}")
        if resp.get("FunctionError"):
            r.fail("append failed: %s" % json.dumps(out)[:600])
            raise SystemExit(1)
        r.log("append (%.0fs): through %s mapped %s appended_symbols %s appended_rows %s failed %s skipped %s unproven %s fmp_bank_not_ready %s" % (
            time.time() - t0, out.get("through"), out.get("mapped"), out.get("appended_symbols"), out.get("appended_rows"), out.get("failed"), out.get("skipped"), out.get("unproven"), out.get("fmp_bank_not_ready")))
        for sym, v in sorted((out.get("verdicts") or {}).items()):
            r.log("  %s: %s" % (sym, v))
        r.kv(step="fmp-append first run", through=out.get("through"), appended_symbols=out.get("appended_symbols"), appended_rows=out.get("appended_rows"), failed=out.get("failed"),
             unproven=out.get("unproven"), fmp_bank_not_ready=out.get("fmp_bank_not_ready"))
        idx = json.loads(s3.get_object(Bucket=LIVE, Key=IDX)["Body"].read())
        fa = idx.get("fmp_append") or {}
        r.log("index fmp_append: at %s through %s appended_symbols %s" % (fa.get("at"), fa.get("through"), fa.get("appended_symbols")))
        advanced = [s for s, v in (out.get("verdicts") or {}).items() if str(v).startswith("appended")]
        for sym in advanced[:3]:
            doc = bank(s3, idx["symbols"][sym]["key"])
            tail = doc.get("bars", [])[-3:]
            labels = (doc.get("bar_sources") or [])[-3:]
            r.log("%s bank after: n %s last %s source %s; last 3 bars %s labels %s" % (sym, doc.get("n"), doc.get("last_date"), doc.get("source"),
                                                                                       [(datetime.fromtimestamp(b[0], tz=timezone.utc).isoformat(), b[4]) for b in tail], labels))
        if not advanced:
            r.warn("no name advanced on this run - expected before the walker's first v1.2.0 price lane (23:27 UTC); the schedule (23:58 UTC) takes it from here")
        r.ok("ops 6653 complete")


if __name__ == "__main__":
    main()
    sys.exit(0)
