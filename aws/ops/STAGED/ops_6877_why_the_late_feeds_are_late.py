"""ops 6877 - READ-ONLY: why the really-late feeds are late - each engine's own last words.

ops 6876 sorted the freshness monitor's list: 115 feeds are written by engines that DO run every day and yet are old
(90 engines; 28 of them erred in the last 3 days).  Two age clusters stand out - about 417 hours (2026-11-03) and about
500 hours (2026-10-31).  This reads, for every engine behind a really-late feed, the last 36 hours of its log: how
many runs ended in an error, and the lines that say why (provider refusals such as FMP's bandwidth limit, timeouts,
out-of-memory, exceptions, "preserve previous publication").  It then groups the engines by cause.

Nothing is written but this report.  Log lines are masked (addresses, credential-looking values, long tokens).
"""
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
import concurrent.futures as cf
import importlib.util
import json
import re
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
sys.path.insert(0, str(ROOT / "scripts"))
_spec = importlib.util.spec_from_file_location("ops6876", str(Path(__file__).resolve().parent / "ops_6876_walkers_and_real_staleness.py"))
W = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(W)
LIVE, REGION = "justhodl-dashboard-live", "us-east-1"
CAUSES = [("FMP bandwidth limit", re.compile(r"bandwidth limit", re.I)),
          ("FMP refused (429 / limit)", re.compile(r"(financialmodelingprep|fmp)[^\n]{0,160}(429|limit reach|too many)|429[^\n]{0,120}(financialmodelingprep|fmp)", re.I)),
          ("provider 429 (other)", re.compile(r"\b429\b|too many requests|rate.?limit", re.I)),
          ("provider 401 / 403", re.compile(r"\b(401|403)\b|unauthorized|forbidden|not.?authorized|not entitled", re.I)),
          ("timed out", re.compile(r"task timed out|timed out after|timeout", re.I)),
          ("out of memory", re.compile(r"outofmemory|out of memory|memoryerror|Runtime exited with error: signal: killed", re.I)),
          ("kept the previous publication", re.compile(r"preserv|previous publication|kept the previous|keep previous|last.good|not published|publication guard", re.I)),
          ("an input file is missing or old", re.compile(r"nosuchkey|no such key|stale input|input .* (missing|stale|old)|upstream .* (missing|stale)|no selected universe|missing input", re.I)),
          ("exception", re.compile(r"traceback|\[error\]|error:|exception|raise ", re.I))]


def cause_of(lines):
    """The first cause in priority order that any line shows, and the line."""
    for name, rx in CAUSES:
        for l in lines:
            if rx.search(l):
                return name, l
    return ("no reason in the log", lines[-1] if lines else "")


def selftest():
    assert cause_of(["x", "FMP 429 'Bandwidth Limit Reach' at ..."])[0] == "FMP bandwidth limit"
    assert cause_of(["HTTP 429 from https://financialmodelingprep.com/stable/x"])[0] == "FMP refused (429 / limit)"
    assert cause_of(["polygon answered 429"])[0] == "provider 429 (other)" and cause_of(["Task timed out after 300.00 seconds"])[0] == "timed out"
    assert cause_of(["Runtime exited with error: signal: killed Runtime.OutOfMemory"])[0] == "out of memory"
    assert cause_of(["publication guard: kept the previous file"])[0] == "kept the previous publication" and cause_of(["all good"]) == ("no reason in the log", "all good")
    assert cause_of(["Traceback (most recent call last):", "KeyError: 'a'"])[0] == "exception" and cause_of([])[0] == "no reason in the log"


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    import cadence_apply as ca

    selftest()
    cfg = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=60, max_pool_connections=64)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    with report("ops_6877_why_the_late_feeds_are_late") as r:
        r.heading("ops 6877 - why the really-late feeds are late (read-only)")
        mon = json.loads(s3.get_object(Bucket=LIVE, Key="data/_freshness-monitor.json")["Body"].read())
        em = json.loads((ROOT / "engine-manifest.json").read_text(encoding="utf-8"))
        owner = defaultdict(list)
        for e in em.get("engines", []):
            for k in e.get("keys") or []:
                owner[k].append(e["engine"])
        live = set()
        for page in lam.get_paginator("list_functions").paginate():
            live |= {f["FunctionName"] for f in page["Functions"]}
        late = defaultdict(list)                     # engine -> [(age, key)]
        for x in mon.get("results") or []:
            if not isinstance(x, dict) or x.get("status") not in ("SOURCE_STALE", "STALE"):
                continue
            for o in owner.get(x.get("key"), []):
                if o in live:
                    late[o].append((x.get("age_h") or 0, x.get("key")))
        r.log("monitor report of %s; %d engines own a feed whose data is past its limit" % (mon.get("generated_at"), len(late)))

        def study(fn):
            def m(name):
                res = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName=name, Dimensions=[{"Name": "FunctionName", "Value": fn}],
                                               StartTime=now - timedelta(days=3), EndTime=now, Period=86400, Statistics=["Sum"])
                return int(sum(d["Sum"] for d in res.get("Datapoints", [])))
            runs, errs = m("Invocations"), m("Errors")
            lines = []
            try:
                ev = logs.filter_log_events(logGroupName="/aws/lambda/" + fn, startTime=int((now - timedelta(hours=36)).timestamp() * 1000),
                                            filterPattern='?429 ?Bandwidth ?"timed out" ?Traceback ?ERROR ?Error ?preserv ?previous ?OutOfMemory ?killed ?403 ?401 ?NoSuchKey ?stale ?guard', limit=60).get("events", [])
                lines = [W.mask(e["message"].strip(), 260) for e in ev]
            except Exception as e:  # noqa: BLE001
                lines = ["(log not read: %s)" % type(e).__name__]
            return fn, runs, errs, lines
        with cf.ThreadPoolExecutor(max_workers=8) as ex:
            got = list(ex.map(study, sorted(late)))
        groups = defaultdict(list)
        for fn, runs, errs, lines in got:
            if runs == 0:
                continue
            c, line = cause_of(lines)
            oldest = max(a for a, _ in late[fn])
            groups[c].append((fn, runs, errs, oldest, len(late[fn]), line))
        r.section("Engines that ran in the last 3 days and own a late feed, by what their log says")
        order = [n for n, _ in CAUSES] + ["no reason in the log"]
        for c in order:
            rows = sorted(groups.get(c, []), key=lambda t: -t[3])
            if not rows:
                continue
            r.log("%s: %d engines, %d late feeds; %d of the engines end runs in an error" % (c, len(rows), sum(x[4] for x in rows), sum(1 for x in rows if x[2])))
            for fn, runs, errs, oldest, n, line in rows[:40]:
                r.log("    %-40s runs %3d errors %3d | oldest feed %6.0f h (%s) | %s" % (fn, runs, errs, oldest, (now - timedelta(hours=oldest)).strftime("%m-%d"), line[:170]))
        days = Counter((now - timedelta(hours=a)).strftime("%m-%d") for rows in groups.values() for *_x, a, _n, _l in rows if a > 26)
        r.log("the day each engine's oldest late feed stopped moving: %s" % ", ".join("%s x%d" % kv for kv in sorted(days.items())[-30:]))
        fmp = len(groups.get("FMP bandwidth limit", [])) + len(groups.get("FMP refused (429 / limit)", []))
        r.kv(engines=sum(len(v) for v in groups.values()), fmp_blocked=fmp, timed_out=len(groups.get("timed out", [])), out_of_memory=len(groups.get("out of memory", [])),
             other_429=len(groups.get("provider 429 (other)", [])), exceptions=len(groups.get("exception", [])), kept_previous=len(groups.get("kept the previous publication", [])),
             no_reason=len(groups.get("no reason in the log", [])))
        r.ok("read-only; nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--selftest":
        selftest()
        print("selftest ok")
    else:
        main()
