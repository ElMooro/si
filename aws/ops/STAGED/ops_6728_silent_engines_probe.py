"""ops 6728 - READ-ONLY probe of the 18 engines ops 6727 (daily health run 2026-11-04) found with every declared output
written before the Nov 1-2 freeze although two daily slots have passed since the Nov 2 15:44 UTC restore.

For each engine, from the account (never from the repo's config.json, which the Oct 20 cadence plan retimed or disabled):
  1. the function: state, last-modified, reserved concurrency (0 = held), architecture, timeout;
  2. its live triggers: classic EventBridge rules that target it (state + schedule expression) and EventBridge Scheduler
     schedules whose target is the function (state + expression) - the schedule is what decides whether a slot was
     actually missed (a weekly engine at 120 h on a Wednesday is on time);
  3. CloudWatch Invocations / Errors / Throttles / Duration per day over the last 8 days (Oct 27 - Nov 4): ran-and-wrote,
     ran-and-failed, throttled, or never started;
  4. the last 3 days of its log group filtered for Traceback / ERROR / Task timed out, last 3 lines.
Then a verdict per engine: ON_TIME (schedule not yet due), NEVER_STARTED (no invocations since the restore with an
enabled daily trigger), FAILING (invocations with errors), THROTTLED, RAN_NO_WRITE (invocations, no errors, no output),
HELD (reserved concurrency 0), or NO_TRIGGER (nothing in the account invokes it). Invokes nothing; changes nothing.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
RESTORE = datetime(2026, 11, 2, 15, 44, tzinfo=timezone.utc)
# engine -> declared outputs ops 6727 found stale (ages at 11:19 UTC)
ENGINES = {
    "justhodl-base-rates": ["data/base-rates.json"],
    "justhodl-beaters-grader": ["data/beaters-learned-weights.json"],
    "justhodl-crypto-emergence": ["data/crypto-emergence.json"],
    "justhodl-earnings-blackout": ["data/earnings-blackout.json", "data/history/blackout.json"],
    "justhodl-earnings-quality": ["data/earnings-quality.json"],
    "justhodl-ecb-derived": ["data/ecb-derived.json", "data/ecb-hist/esi.json"],
    "justhodl-eps-revision-velocity": ["data/eps-revision-velocity.json"],
    "justhodl-forward-orders": ["data/forward-orders.json"],
    "justhodl-liquidity-profile": ["data/liquidity-profile.json"],
    "justhodl-momentum-breakout": ["data/momentum-breakout.json"],
    "justhodl-naaim": ["data/naaim.json"],
    "justhodl-portwatch": ["data/portwatch.json"],
    "justhodl-revenue-acceleration": ["data/revenue-acceleration.json"],
    "justhodl-russell-recon-frontrun": ["data/russell-recon-frontrun.json"],
    "justhodl-sellside-views": ["data/sellside-views.json"],
    "justhodl-spx-beaters": ["data/spx-beaters.json"],
    "justhodl-systemic-stress": ["data/systemic-stress.json"],
    "justhodl-volatility-squeeze-hunter": ["data/volatility-squeeze.json"],
}
# the manifest attributes some of these outputs to a differently named producer (ops 6727's cadence read)
ALIASES = {"justhodl-momentum-breakout": "justhodl-momentum-leaders", "justhodl-naaim": "justhodl-put-call-extreme",
           "justhodl-eps-revision-velocity": "justhodl-nobrainer-rationale"}


def verdict(exists, reserved, triggers_enabled, inv_since, err_since, thr_since, out_age_h, due_daily):
    """Pure classification for the selftest. inv/err/thr are sums since the restore; due_daily says an enabled
    trigger fires at least daily (so two slots have passed)."""
    if not exists:
        return "NO_FUNCTION"
    if reserved == 0:
        return "HELD"
    if thr_since > 0 and inv_since == 0:
        return "THROTTLED"
    if inv_since > 0 and err_since >= inv_since:
        return "FAILING"
    if inv_since > 0 and err_since > 0:
        return "PARTLY_FAILING"
    if inv_since > 0 and out_age_h is not None and out_age_h > (datetime.now(timezone.utc) - RESTORE).total_seconds() / 3600:
        return "RAN_NO_WRITE"
    if inv_since > 0:
        return "RAN"
    if not triggers_enabled:
        return "NO_TRIGGER"
    if due_daily:
        return "NEVER_STARTED"
    return "ON_TIME"


def selftest():
    assert verdict(False, None, 0, 0, 0, 0, 100, True) == "NO_FUNCTION"
    assert verdict(True, 0, 1, 0, 0, 0, 100, True) == "HELD"
    assert verdict(True, None, 1, 0, 0, 3, 100, True) == "THROTTLED"
    assert verdict(True, None, 1, 2, 2, 0, 100, True) == "FAILING"
    assert verdict(True, None, 1, 4, 1, 0, 100, True) == "PARTLY_FAILING"
    assert verdict(True, None, 1, 2, 0, 0, 100, True) == "RAN_NO_WRITE"
    assert verdict(True, None, 1, 2, 0, 0, 1, True) == "RAN"
    assert verdict(True, None, 0, 0, 0, 0, 100, False) == "NO_TRIGGER"
    assert verdict(True, None, 1, 0, 0, 0, 100, True) == "NEVER_STARTED"
    assert verdict(True, None, 1, 0, 0, 0, 120, False) == "ON_TIME"
    print("selftest ok")


def daily_metrics(cw, names, start, end):
    q, ids = [], {}
    for i, n in enumerate(names):
        for tag, m in (("n", "Invocations"), ("e", "Errors"), ("t", "Throttles"), ("d", "Duration")):
            ids["%s%d" % (tag, i)] = (n, tag)
            q.append({"Id": "%s%d" % (tag, i), "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": m,
                      "Dimensions": [{"Name": "FunctionName", "Value": n}]}, "Period": 86400, "Stat": "Sum"}})
    out = {n: {} for n in names}
    for i in range(0, len(q), 400):
        tok = None
        while True:
            kw = {"MetricDataQueries": q[i:i + 400], "StartTime": start, "EndTime": end}
            if tok:
                kw["NextToken"] = tok
            res = cw.get_metric_data(**kw)
            for r_ in res["MetricDataResults"]:
                n, tag = ids[r_["Id"]]
                for t, v in zip(r_["Timestamps"], r_["Values"]):
                    out[n].setdefault(t.date().isoformat(), {}).setdefault(tag, 0.0)
                    out[n][t.date().isoformat()][tag] += v
            tok = res.get("NextToken")
            if not tok:
                break
    return out


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report

    selftest()
    cfg = Config(retries={"max_attempts": 8, "mode": "adaptive"}, connect_timeout=10, read_timeout=60)
    lam = boto3.client("lambda", region_name=REGION, config=cfg)
    s3 = boto3.client("s3", region_name=REGION, config=cfg)
    cw = boto3.client("cloudwatch", region_name=REGION, config=cfg)
    logs = boto3.client("logs", region_name=REGION, config=cfg)
    ev = boto3.client("events", region_name=REGION, config=cfg)
    sch = boto3.client("scheduler", region_name=REGION, config=cfg)
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=8)
    facts = {}
    with report("ops_6728_silent_engines_probe") as r:
        r.heading("ops 6728 - why 18 engines have not written since before the Nov 1-2 freeze (triggers, invocations, errors; read-only)")

        r.section("0. every Scheduler schedule in the account, indexed by target function (one listing, reused)")
        by_target = {}
        try:
            tok = None
            n_sched = 0
            while True:
                kw = {"MaxResults": 100}
                if tok:
                    kw["NextToken"] = tok
                res = sch.list_schedules(**kw)
                for s in res.get("Schedules", []):
                    n_sched += 1
                    arn = (s.get("Target") or {}).get("Arn") or ""
                    fn = arn.split(":function:")[-1].split(":")[0] if ":function:" in arn else ""
                    by_target.setdefault(fn, []).append((s.get("Name"), s.get("State"), s.get("GroupName")))
                tok = res.get("NextToken")
                if not tok:
                    break
            r.log("%d schedules listed, %d distinct target functions" % (n_sched, len(by_target)))
        except Exception as exc:  # noqa: BLE001
            r.warn("list_schedules: %s: %s" % (type(exc).__name__, str(exc)[:160]))

        names = sorted(set(ENGINES) | set(ALIASES.values()))
        r.section("1. CloudWatch per-day Invocations / Errors / Throttles, %s .. %s" % (start.date(), now.date()))
        try:
            met = daily_metrics(cw, names, start, now)
        except Exception as exc:  # noqa: BLE001
            met = {n: {} for n in names}
            r.warn("get_metric_data: %s: %s" % (type(exc).__name__, str(exc)[:160]))

        r.section("2. per engine: function, triggers, metrics, last errors, verdict")
        verdicts = {}
        for eng, outs in sorted(ENGINES.items()):
            cands = [eng] + ([ALIASES[eng]] if eng in ALIASES else [])
            fn_name, fconf = None, None
            for c in cands:
                try:
                    fconf = lam.get_function_configuration(FunctionName=c)
                    fn_name = c
                    break
                except lam.exceptions.ResourceNotFoundException:
                    continue
                except Exception as exc:  # noqa: BLE001
                    r.warn("%s: get_function_configuration %s: %s" % (c, type(exc).__name__, str(exc)[:120]))
            ages = []
            for k in outs:
                try:
                    h = s3.head_object(Bucket=LIVE, Key=k)
                    ages.append((now - h["LastModified"]).total_seconds() / 3600)
                except Exception:  # noqa: BLE001
                    ages.append(None)
            out_age = max((a for a in ages if a is not None), default=None)
            if not fconf:
                r.warn("%s: no function named %s in the account; outputs %s" % (eng, " / ".join(cands), ["%.0fh" % a if a else None for a in ages]))
                verdicts[eng] = "NO_FUNCTION"
                continue
            reserved = None
            try:
                reserved = lam.get_function_concurrency(FunctionName=fn_name).get("ReservedConcurrentExecutions")
            except Exception:  # noqa: BLE001
                pass
            triggers, enabled, due_daily = [], 0, False
            try:
                for rule in ev.list_rule_names_by_target(TargetArn=fconf["FunctionArn"]).get("RuleNames", []):
                    d = ev.describe_rule(Name=rule)
                    expr = d.get("ScheduleExpression") or ("event-pattern" if d.get("EventPattern") else "?")
                    triggers.append("rule %s %s %s" % (rule, d.get("State"), expr))
                    if d.get("State") == "ENABLED":
                        enabled += 1
                        if expr.startswith("rate(") or (expr.startswith("cron(") and expr.split()[2] == "*" and expr.split()[4] in ("?", "*")):
                            due_daily = True
            except Exception as exc:  # noqa: BLE001
                triggers.append("rules: %s" % str(exc)[:80])
            for sname, sstate, group in by_target.get(fn_name, []):
                try:
                    d = sch.get_schedule(Name=sname, GroupName=group or "default")
                    expr = d.get("ScheduleExpression", "?")
                except Exception as exc:  # noqa: BLE001
                    expr = "?(%s)" % str(exc)[:40]
                triggers.append("sched %s %s %s" % (sname, sstate, expr))
                if sstate == "ENABLED":
                    enabled += 1
                    parts = expr.split()
                    if expr.startswith("rate(") or (expr.startswith("cron(") and len(parts) >= 5 and parts[2] == "*" and parts[4] in ("?", "*")):
                        due_daily = True
            # event-source mappings (SQS/Kinesis) count as triggers too
            try:
                esm = lam.list_event_source_mappings(FunctionName=fn_name).get("EventSourceMappings", [])
                for m in esm:
                    triggers.append("esm %s %s" % (m.get("EventSourceArn", "?").split(":")[-1][:40], m.get("State")))
                    if m.get("State") == "Enabled":
                        enabled += 1
            except Exception:  # noqa: BLE001
                pass
            days = met.get(fn_name) or {}
            inv_since = sum(v.get("n", 0) for d_, v in days.items() if d_ >= "2026-11-02")
            err_since = sum(v.get("e", 0) for d_, v in days.items() if d_ >= "2026-11-02")
            thr_since = sum(v.get("t", 0) for d_, v in days.items() if d_ >= "2026-11-02")
            series = ", ".join("%s n%d e%d t%d" % (d_[5:], v.get("n", 0), v.get("e", 0), v.get("t", 0)) for d_, v in sorted(days.items()))
            last_err = []
            try:
                fl = logs.filter_log_events(logGroupName="/aws/lambda/" + fn_name, startTime=int((now - timedelta(days=3)).timestamp() * 1000),
                                            filterPattern='?Traceback ?ERROR ?"Task timed out" ?"Runtime exited"', limit=60)
                last_err = [e["message"].strip()[:220] for e in fl.get("events", [])][-3:]
            except logs.exceptions.ResourceNotFoundException:
                last_err = ["(no log group)"]
            except Exception as exc:  # noqa: BLE001
                last_err = ["logs: %s" % str(exc)[:80]]
            v = verdict(True, reserved, enabled, inv_since, err_since, thr_since, out_age, due_daily)
            verdicts[eng] = v
            r.log("%s%s -> %s | state %s updated %s | reserved %s | outputs %s" % (
                eng.replace("justhodl-", ""), "" if fn_name == eng else " (=%s)" % fn_name, v, fconf.get("State"),
                str(fconf.get("LastModified"))[:16], reserved, ", ".join("%s %s" % (k.replace("data/", ""), "%.0fh" % a if a is not None else "MISSING") for k, a in zip(outs, ages))))
            r.log("    triggers (%d enabled, daily-or-faster %s): %s" % (enabled, due_daily, "; ".join(triggers) or "NONE"))
            r.log("    metrics: %s" % (series or "no datapoints in 8 days"))
            if last_err:
                for line in last_err:
                    r.log("    log: %s" % line)
        r.section("3. verdicts")
        groups = {}
        for eng, v in verdicts.items():
            groups.setdefault(v, []).append(eng.replace("justhodl-", ""))
        for v, lst in sorted(groups.items()):
            r.log("%s (%d): %s" % (v, len(lst), ", ".join(sorted(lst))))
            facts[v] = ", ".join(sorted(lst))
        broken = sum(len(lst) for v, lst in groups.items() if v in ("FAILING", "PARTLY_FAILING", "NEVER_STARTED", "THROTTLED", "RAN_NO_WRITE", "NO_FUNCTION"))
        facts["broken_count"] = broken
        if broken:
            r.warn("%d engines need a fix (FAILING / NEVER_STARTED / THROTTLED / RAN_NO_WRITE / NO_FUNCTION) - nothing was changed here" % broken)
        r.kv(**{k: str(v) for k, v in facts.items()})
        r.ok("read-only; nothing invoked, nothing changed")
    sys.exit(0)


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        selftest()
    else:
        main()
