"""ops 6710 - READ-ONLY: the engines held under the governor's manual-release stage CAP, and what each costs a month.

Khalid (2026-11-02 11:02 ET): "list me all the engines that are frozen because of the $50 cap and give me their monthly
estimates".  ops 6709 read-back: five functions sit at reserved concurrency 0 under ledger stage CAP -
justhodl-event-coordinator and justhodl-miss-calibrator (metered past $50 in October) and macro-financial-intelligence,
ofrapi, treasury-api (held 2026-10-20 under config/retire-plan-2026-10-20.json, recorded in the same manual-release
stage).  For each: the governor's ledger entry (why, when, metered USD), configuration and every trigger that would
fire it once released, and its real Lambda cost by day from CloudWatch (Duration + Invocations, the governor's own
formula: list prices, no free tier) from 2026-07-01 - monthly totals, the October storm, and a normal-run window.
For the coordinator also the engines it routes to (ROUTES), with their cost while it routed vs while it was held.
Nothing is changed.  No key involved.
"""
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from statistics import median
import ast
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
PRIVATE = "justhodl-ops-private-%s" % ACCOUNT
FNS = ["justhodl-event-coordinator", "justhodl-miss-calibrator", "macro-financial-intelligence", "ofrapi", "treasury-api"]
START = datetime(2026, 7, 1, tzinfo=timezone.utc)
NORMAL = (date(2026, 9, 1), date(2026, 10, 11))      # before the Oct 12-16 loop
HELD = (date(2026, 10, 21), date(2026, 10, 31))      # coordinator held, fleet running
CFG = Config(retries={"max_attempts": 10, "mode": "adaptive"})


def usd(dur_ms, inv, mem, arch):
    price = 0.0000133334 if arch == "arm64" else 0.0000166667
    return (dur_ms / 1000.0) * (mem / 1024.0) * price + inv * 2e-7


def daily(cw, names, end):
    q, ids = [], {}
    for i, n in enumerate(names):
        for tag, m in (("d", "Duration"), ("n", "Invocations"), ("e", "Errors"), ("t", "Throttles")):
            ids["%s%d" % (tag, i)] = (n, tag)
            q.append({"Id": "%s%d" % (tag, i), "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": m,
                      "Dimensions": [{"Name": "FunctionName", "Value": n}]}, "Period": 86400, "Stat": "Sum"}})
    out = {n: {} for n in names}
    for i in range(0, len(q), 400):
        tok = None
        while True:
            kw = {"MetricDataQueries": q[i:i + 400], "StartTime": START, "EndTime": end}
            if tok:
                kw["NextToken"] = tok
            r = cw.get_metric_data(**kw)
            for res in r["MetricDataResults"]:
                n, tag = ids[res["Id"]]
                for t, v in zip(res["Timestamps"], res["Values"]):
                    out[n].setdefault(t.date(), {}).setdefault(tag, 0.0)
                    out[n][t.date()][tag] += v
            tok = r.get("NextToken")
            if not tok:
                break
    return out


def routes():
    src = (ROOT / "aws" / "lambdas" / "justhodl-event-coordinator" / "source" / "lambda_function.py").read_text()
    for node in ast.parse(src).body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "ROUTES" for t in node.targets):
            return ast.literal_eval(node.value)
    return {}


def main():
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
    ev = boto3.client("events", region_name=REGION, config=CFG)
    sch = boto3.client("scheduler", region_name=REGION, config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    now = datetime.now(timezone.utc)
    with report("ops_6710_capped_engines_cost") as r:
        r.heading("ops 6710 - the CAP-held engines: why, triggers, and their monthly Lambda cost (read-only)")
        st = json.loads(s3.get_object(Bucket=PRIVATE, Key="spend-governor/state.json")["Body"].read())
        ledger = {e.get("id"): e for e in (st.get("ledger") or {}).values()}
        r.section("A. governor ledger")
        r.log("engine cap state: %s" % json.dumps({k: v for k, v in (st.get("caps") or st.get("engine_caps") or {}).items()})[:1500])
        for fn in FNS:
            e = ledger.get(fn)
            r.log("%s: %s" % (fn, json.dumps(e, default=str)[:900] if e else "NOT in the ledger"))
        for k in sorted(st):
            if k not in ("ledger", "ce", "released", "storage", "fuse", "cadence", "anomalies", "last", "meter"):
                r.log("state.%s = %s" % (k, json.dumps(st[k], default=str)[:300]))

        r.section("B. configuration and every trigger that would fire it once released")
        sched_targets = {}
        for page in sch.get_paginator("list_schedules").paginate():
            for s in page.get("Schedules", []):
                sched_targets.setdefault(((s.get("Target") or {}).get("Arn") or "").split(":")[-1], []).append(
                    "%s/%s [%s]" % (s.get("GroupName"), s["Name"], s.get("State")))
        cfgs = {}
        for fn in FNS:
            c = lam.get_function_configuration(FunctionName=fn)
            arch = (c.get("Architectures") or ["x86_64"])[0]
            cfgs[fn] = (c["MemorySize"], arch)
            rc = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
            rules = []
            for name in ev.list_rule_names_by_target(TargetArn=c["FunctionArn"]).get("RuleNames", []):
                d = ev.describe_rule(Name=name)
                rules.append("%s [%s] %s" % (name, d.get("State"), d.get("ScheduleExpression") or "pattern " + (d.get("EventPattern") or "")[:120]))
            esm = [m.get("EventSourceArn") for m in lam.list_event_source_mappings(FunctionName=fn).get("EventSourceMappings", [])]
            try:
                url = lam.get_function_url_config(FunctionName=fn).get("FunctionUrl")
            except Exception:  # noqa: BLE001
                url = None
            try:
                pol = json.loads(lam.get_policy(FunctionName=fn)["Policy"])
                princ = sorted({json.dumps(s.get("Principal")) + ("<-" + str((s.get("Condition") or {}).get("ArnLike", {}).get("AWS:SourceArn", "")).split(":")[-1]
                                if s.get("Condition") else "") for s in pol.get("Statement", [])})
            except Exception:  # noqa: BLE001
                princ = []
            r.log("%s: %s MB %s, timeout %s s, runtime %s, modified %s, reserved concurrency %s | %s" % (
                fn, c["MemorySize"], arch, c["Timeout"], c.get("Runtime") or c.get("PackageType"), c["LastModified"][:16], rc, (c.get("Description") or "")[:160]))
            r.log("   rules %s | schedules %s | event sources %s | function URL %s | invoke grants %s" % (
                rules or "none", sched_targets.get(fn) or "none", esm or "none", url, princ[:8]))

        r.section("C. Lambda cost by month and by day (CloudWatch, governor formula)")
        data = daily(cw, FNS, now)
        for fn in FNS:
            mem, arch = cfgs[fn]
            days = {d: usd(v.get("d", 0), v.get("n", 0), mem, arch) for d, v in data[fn].items()}
            by_month = {}
            for d, v in data[fn].items():
                m = by_month.setdefault(d.strftime("%Y-%m"), {"usd": 0.0, "inv": 0, "err": 0, "thr": 0, "days": 0})
                m["usd"] += days[d]
                m["inv"] += int(v.get("n", 0))
                m["err"] += int(v.get("e", 0))
                m["thr"] += int(v.get("t", 0))
                m["days"] += bool(v.get("n", 0))
            r.log("%s (%s MB %s): %s" % (fn, mem, arch, " | ".join("%s $%.2f %d inv %d err %d throttled (%d active days)" % (
                k, v["usd"], v["inv"], v["err"], v["thr"], v["days"]) for k, v in sorted(by_month.items()))))
            win = [(d, days[d], data[fn][d].get("n", 0), data[fn][d].get("d", 0)) for d in sorted(days) if NORMAL[0] <= d <= NORMAL[1]]
            active = [x for x in win if x[2] > 0]
            if active:
                med = median(x[1] for x in active)
                mean = sum(x[1] for x in win) / max(1, (NORMAL[1] - NORMAL[0]).days + 1)
                avg_ms = sum(x[3] for x in active) / max(1, sum(x[2] for x in active))
                r.log("   normal window %s..%s: %d active days, median $%.4f/active day, mean $%.4f/day -> ~$%.2f/month; median %d inv/day, avg %.1f s/inv" % (
                    NORMAL[0], NORMAL[1], len(active), med, mean, mean * 30.4, median(x[2] for x in active), avg_ms / 1000.0))
            else:
                r.log("   normal window %s..%s: no invocations" % NORMAL)
            octd = [(d, days[d], int(data[fn][d].get("n", 0)), int(data[fn][d].get("e", 0))) for d in sorted(days) if d >= date(2026, 10, 1)]
            r.log("   daily since Oct 1: %s" % ", ".join("%s $%.2f/%d inv/%d err" % (d.strftime("%m-%d"), u, n, e) for d, u, n, e in octd if n or u))

        r.section("D. what the coordinator drives: its routed targets, cost while it routed vs while it was held")
        rt = routes()
        targets = sorted({t for v in rt.values() for t in (v.get("invoke") or [])})
        r.log("routes %d, invoke targets %d: %s" % (len(rt), len(targets), targets))
        if targets:
            cfgt = {}
            for t in targets:
                try:
                    c = lam.get_function_configuration(FunctionName=t)
                    cfgt[t] = (c["MemorySize"], (c.get("Architectures") or ["x86_64"])[0])
                except Exception as e:  # noqa: BLE001
                    r.log("   %s: %s" % (t, type(e).__name__))
            dt = daily(cw, list(cfgt), now)
            tot_n, tot_h = 0.0, 0.0
            for t in cfgt:
                mem, arch = cfgt[t]

                def per_day(lo, hi):
                    span = (hi - lo).days + 1
                    rows = [v for d, v in dt[t].items() if lo <= d <= hi]
                    return (sum(usd(v.get("d", 0), v.get("n", 0), mem, arch) for v in rows) / span, sum(v.get("n", 0) for v in rows) / span)
                (un, nn), (uh, nh) = per_day(*NORMAL), per_day(*HELD)
                tot_n += un
                tot_h += uh
                r.log("   %s (%s MB): routed era $%.3f/day %.1f inv/day | held era $%.3f/day %.1f inv/day" % (t, mem, un, nn, uh, nh))
            r.log("   targets together: $%.2f/day while routing vs $%.2f/day while held -> coordinator-driven ~$%.2f/month" % (tot_n, tot_h, max(0.0, tot_n - tot_h) * 30.4))
        r.ok("ops 6710 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
