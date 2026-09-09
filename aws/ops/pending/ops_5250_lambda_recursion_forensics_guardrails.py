"""
ops_5250 — LAMBDA RECURSION FORENSICS + PERMANENT COST/RUNAWAY GUARDRAILS

Trigger: AWS Health mail Mon 2026-09-07 06:41 EDT (10:41 UTC) —
"[Action Required] AWS Lambda recursive loop detected and auto-remediated
in your account [857687956942]". Khalid: "fix this ... i dont want the
same mistake again" (the August bill was $484).

What AWS actually did (docs, verified 2026-09-09): Lambda tracks a Lineage
counter in the X-Ray trace header across SQS/SNS/S3/Lambda-Invoke hops.
When the SAME originating event reaches a function for the 17th time it
DROPS that invocation (RecursiveInvocationException / DLQ), emits the
CloudWatch metric RecursiveInvocationsDropped, and mails a Health event.
"Auto-remediated" = the 17th hop was dropped. It does NOT throttle the
function; we verify reserved concurrency anyway.

This op is READ-ONLY on every engine. The only mutations are guardrails
that touch no engine: one small notifier Lambda, SNS wiring, and six
CloudWatch alarms that fire within minutes instead of a Cost Anomaly mail
days later.

  A. WHO tripped — RecursiveInvocationsDropped per function, per 5-min
     datapoint (14d), and the account aggregate.
  B. WHAT the chain looks like — static census of every engine that
     self-invokes with InvocationType=Event (walk vs one-shot fan-out),
     its own depth cap vs AWS's 16, and its live triggers.
  C. Per tripped function: config, async config (DLQ/destination),
     reserved concurrency, recursion config, hourly Invocations/Errors/
     Throttles/Duration since Sep 1, and the log lines around each drop.
  D. Fleet posture — Cost Explorer daily by service Sep 1→today, account
     hourly invocations p95/max (alarm thresholds), top functions by
     invocations and GB-s over 7d.
  E. Loop surfaces — S3 bucket notifications and aws.s3 EventBridge rules
     targeting Lambdas (the S3→Lambda→S3 pattern).
  F. GUARDRAILS — justhodl-guardrail-notify (SNS→Telegram+S3 ledger),
     SNS justhodl-fleet-alerts subscriptions, alarms:
       recursion-dropped (Metrics Insights, any function), recursion-
       dropped-account, invocations-1h, errors-1h, concurrency,
       s3-bytes-growth, s3-objects-growth.

Report: aws/ops/reports/latest/5250_lambda_recursion_forensics_guardrails.md
"""
import io
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _lambda_deploy_helpers import build_zip, create_or_update_lambda  # noqa: E402

REGION = "us-east-1"
ACCT = "857687956942"
BUCKET = "justhodl-dashboard-live"
TOPIC_ARN = "arn:aws:sns:%s:%s:justhodl-fleet-alerts" % (REGION, ACCT)
NOTIFY_FN = "justhodl-guardrail-notify"
OWNER_EMAIL = "raafouis@gmail.com"
CFG = Config(retries={"max_attempts": 8, "mode": "adaptive"}, read_timeout=90)

cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
sch = boto3.client("scheduler", region_name=REGION, config=CFG)
evb = boto3.client("events", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
sns = boto3.client("sns", region_name=REGION, config=CFG)
ce = boto3.client("ce", region_name="us-east-1", config=CFG)

NOW = datetime.now(timezone.utc)
D14 = NOW - timedelta(days=14)
SEP1 = datetime(2026, 9, 1, tzinfo=timezone.utc)
ROOT = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
OUT = {"ops": 5250, "ts": NOW.isoformat()}
GB_S = 0.0000166667
REQ = 0.20 / 1e6


def d(n):
    return (NOW - timedelta(days=n)).strftime("%Y-%m-%d")


def p95(vals):
    if not vals:
        return 0.0
    v = sorted(vals)
    return v[min(len(v) - 1, int(round(0.95 * (len(v) - 1))))]


def series(metric, dims, stat, period, start, end=None, ns="AWS/Lambda"):
    """One metric -> list of (ts, value) ascending."""
    r = cw.get_metric_data(
        MetricDataQueries=[{"Id": "m", "MetricStat": {
            "Metric": {"Namespace": ns, "MetricName": metric, "Dimensions": dims},
            "Period": period, "Stat": stat}, "ReturnData": True}],
        StartTime=start, EndTime=end or NOW, ScanBy="TimestampAscending")
    res = r["MetricDataResults"][0]
    return list(zip(res["Timestamps"], res["Values"]))


with report("5250_lambda_recursion_forensics_guardrails") as rep:
    rep.heading("ops 5250 — Lambda recursion forensics + cost/runaway guardrails")
    fails = []

    # ================================================================ A
    rep.section("A. WHO tripped AWS's recursion breaker (RecursiveInvocationsDropped, 14d)")
    tripped = {}
    try:
        names = set()
        for page in cw.get_paginator("list_metrics").paginate(
                Namespace="AWS/Lambda", MetricName="RecursiveInvocationsDropped"):
            for m in page["Metrics"]:
                for dm in m.get("Dimensions", []):
                    if dm["Name"] == "FunctionName":
                        names.add(dm["Value"])
        rep.log("functions that have EVER published the metric (≤2w window): %d" % len(names))
        for fn in sorted(names):
            pts = [(t, v) for t, v in series("RecursiveInvocationsDropped",
                                             [{"Name": "FunctionName", "Value": fn}],
                                             "Sum", 300, D14) if v > 0]
            if not pts:
                continue
            tot = int(sum(v for _, v in pts))
            tripped[fn] = {"dropped": tot,
                           "moments": [(t.isoformat(), int(v)) for t, v in pts]}
            rep.fail("  TRIPPED %-44s dropped=%d  first=%s  last=%s"
                     % (fn, tot, pts[0][0].strftime("%m-%d %H:%M"),
                        pts[-1][0].strftime("%m-%d %H:%M")))
            for t, v in pts[:12]:
                rep.log("      %s UTC  +%d" % (t.strftime("%Y-%m-%d %H:%M"), int(v)))
            rep.kv(section="tripped", function=fn, dropped=tot,
                   first=pts[0][0].isoformat(), last=pts[-1][0].isoformat())
        if not tripped:
            rep.warn("no per-function RecursiveInvocationsDropped datapoints in 14d")
        acct = [(t, v) for t, v in series("RecursiveInvocationsDropped", [], "Sum", 86400, D14) if v > 0]
        rep.log("account-level aggregate (no dimension) daily: %s"
                % (", ".join("%s=%d" % (t.strftime("%m-%d"), int(v)) for t, v in acct) or "none"))
        OUT["account_aggregate_present"] = bool(acct)
    except Exception as e:
        fails.append("A: %s" % str(e)[:200])
        rep.fail("recursion probe failed: %s" % str(e)[:200])
    OUT["tripped"] = tripped

    # Health API (needs Business+ support; report either way)
    try:
        health = boto3.client("health", region_name="us-east-1", config=CFG)
        ev = health.describe_events(filter={"services": ["LAMBDA"], "startTimes": [
            {"from": D14}]}, maxResults=20).get("events", [])
        for e in ev:
            rep.log("  HEALTH %s %s %s" % (e.get("startTime"), e.get("eventTypeCode"), e.get("statusCode")))
            try:
                ents = health.describe_affected_entities(filter={"eventArns": [e["arn"]]}).get("entities", [])
                for en in ents[:20]:
                    rep.log("     affected: %s" % en.get("entityValue"))
            except Exception as e2:
                rep.log("     entities: %s" % str(e2)[:100])
        if not ev:
            rep.log("Health API: no Lambda events returned")
    except Exception as e:
        rep.log("Health API unavailable (%s) — metric evidence above is authoritative" % str(e)[:90])

    # ================================================================ B
    rep.section("B. Chain census — every engine that Event-invokes ITSELF (static, this checkout)")
    # live trigger map: function name -> [(kind, name, expr, state)]
    trig = {}
    try:
        for page in sch.get_paginator("list_schedules").paginate():
            for s_ in page["Schedules"]:
                try:
                    g = sch.get_schedule(Name=s_["Name"], GroupName=s_.get("GroupName", "default"))
                except Exception:
                    continue
                fn = (g.get("Target", {}).get("Arn", "") or "").split(":")[-1]
                trig.setdefault(fn, []).append(("scheduler", s_["Name"], g.get("ScheduleExpression"), g.get("State")))
    except Exception as e:
        rep.warn("scheduler scan: %s" % str(e)[:120])
    try:
        for page in evb.get_paginator("list_rules").paginate():
            for r in page["Rules"]:
                try:
                    tg = evb.list_targets_by_rule(Rule=r["Name"])
                except Exception:
                    continue
                for t in tg.get("Targets", []):
                    fn = t.get("Arn", "").split(":")[-1]
                    trig.setdefault(fn, []).append(("rule", r["Name"], r.get("ScheduleExpression") or r.get("EventPattern", "")[:40], r.get("State")))
    except Exception as e:
        rep.warn("rule scan: %s" % str(e)[:120])
    OUT["trigger_map_size"] = len(trig)

    census = []
    self_pat = re.compile(r"(context\.function_name|AWS_LAMBDA_FUNCTION_NAME|FunctionName=SELF|FunctionName=FN_NAME)")
    cap_pat = re.compile(r"(CHAIN_DEPTH_MAX|CHAIN_MAX|MAX_HOPS|MAX_DEPTH)\s*=\s*(?:int\([^)]*\"(\d+)\"\)|(\d+))")
    cap_use = re.compile(r"(depth|hop|_depth|hops)\s*<\s*(\d+)")
    for src in sorted((ROOT / "aws" / "lambdas").glob("*/source/*.py")):
        try:
            code = src.read_text(errors="ignore")
        except Exception:
            continue
        if "InvocationType=\"Event\"" not in code and "InvocationType='Event'" not in code:
            continue
        if not self_pat.search(code) and ("FunctionName=\"%s\"" % src.parent.parent.name) not in code:
            continue
        fn = src.parent.parent.name
        caps = [int(a or b) for _, a, b in cap_pat.findall(code)] + [int(n) for _, n in cap_use.findall(code)]
        cap = max(caps) if caps else None
        # walk (chains a cursor/depth) vs fan-out (shards, no continuation)
        walk = bool(re.search(r"(cursor|depth|hop|chain)", code, re.I)) and not (
            fn in ("justhodl-boj-full", "justhodl-sdmx-walker", "justhodl-tv-bars", "justhodl-symdir", "justhodl-census-us"))
        kind = "WALK" if walk else "FANOUT/ONE-SHOT"
        live = trig.get(fn, [])
        try:
            rc = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
        except Exception:
            rc = "?"
        risk = ("BREAKS@16" if kind == "WALK" and (cap is None or cap > 15) else
                "capped<16" if kind == "WALK" else "n/a")
        rec = {"fn": fn, "kind": kind, "own_cap": cap, "risk": risk,
               "reserved_concurrency": rc,
               "triggers": ["%s:%s %s [%s]" % t for t in live],
               "tripped_14d": tripped.get(fn, {}).get("dropped", 0)}
        census.append(rec)
        (rep.warn if risk == "BREAKS@16" else rep.log)(
            "  %-34s %-16s cap=%-5s %-10s rc=%-4s trips=%-3s %s"
            % (fn[:34], kind, cap, risk, rc, rec["tripped_14d"],
               "; ".join(rec["triggers"])[:70] or "NO TRIGGER"))
        rep.kv(section="census", **{k: (v if not isinstance(v, list) else "; ".join(v)) for k, v in rec.items()})
    OUT["census"] = census
    n_break = sum(1 for c in census if c["risk"] == "BREAKS@16")
    rep.log("engines self-chaining a WALK with no cap or a cap above AWS's 16: %d" % n_break)

    # ================================================================ C
    rep.section("C. Each tripped function — config, async config, triggers, hourly metrics, logs at the drop")
    detail = {}
    for fn, info in tripped.items():
        rep.log("── %s" % fn)
        drec = {}
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            drec["config"] = {k: c.get(k) for k in ("MemorySize", "Timeout", "LastModified", "Runtime", "Role")}
            rep.log("   mem=%sMB timeout=%ss modified=%s" % (c.get("MemorySize"), c.get("Timeout"), c.get("LastModified")))
        except Exception as e:
            rep.warn("   config: %s" % str(e)[:100])
        try:
            rc = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
            drec["reserved_concurrency"] = rc
            rep.log("   reserved concurrency: %s %s" % (rc, "(THROTTLED — engine is dead until raised)" if rc == 0 else ""))
        except Exception as e:
            rep.warn("   concurrency: %s" % str(e)[:100])
        try:
            rl = lam.get_function_recursion_config(FunctionName=fn).get("RecursiveLoop")
            drec["recursive_loop"] = rl
            rep.log("   recursion config: %s" % rl)
        except Exception as e:
            rep.log("   recursion config: %s" % str(e)[:80])
        try:
            ic = lam.get_function_event_invoke_config(FunctionName=fn)
            drec["async"] = {"MaximumRetryAttempts": ic.get("MaximumRetryAttempts"),
                             "MaximumEventAgeInSeconds": ic.get("MaximumEventAgeInSeconds"),
                             "DestinationConfig": ic.get("DestinationConfig")}
            rep.log("   async config: retries=%s max_age=%s dest=%s" % (ic.get("MaximumRetryAttempts"), ic.get("MaximumEventAgeInSeconds"), json.dumps(ic.get("DestinationConfig"))[:120]))
        except Exception as e:
            rep.log("   async config: default (retries=2, no destination) — %s" % str(e)[:60])
        drec["triggers"] = trig.get(fn, [])
        for t in drec["triggers"]:
            rep.log("   trigger %s:%s %s [%s]" % t)
        if not drec["triggers"]:
            rep.log("   trigger: NONE of its own (fan-out member or manual)")
        # hourly metrics since Sep 1
        try:
            dims = [{"Name": "FunctionName", "Value": fn}]
            inv = series("Invocations", dims, "Sum", 3600, SEP1)
            err = dict(series("Errors", dims, "Sum", 3600, SEP1))
            thr = dict(series("Throttles", dims, "Sum", 3600, SEP1))
            dur = dict(series("Duration", dims, "Sum", 3600, SEP1))
            mem = (drec.get("config") or {}).get("MemorySize") or 1024
            tot_inv = sum(v for _, v in inv)
            tot_gbs = sum(dur.values()) / 1000.0 * mem / 1024.0
            usd = tot_gbs * GB_S + tot_inv * REQ
            rep.log("   since Sep 1: invocations=%d errors=%d throttles=%d GB-s=%.0f ≈ $%.2f"
                    % (tot_inv, sum(err.values()), sum(thr.values()), tot_gbs, usd))
            drec["since_sep1"] = {"invocations": int(tot_inv), "errors": int(sum(err.values())),
                                  "throttles": int(sum(thr.values())), "gb_s": round(tot_gbs), "usd": round(usd, 2)}
            hot = sorted(inv, key=lambda x: -x[1])[:6]
            for t, v in hot:
                rep.log("     busiest hour %s UTC  inv=%d err=%d thr=%d"
                        % (t.strftime("%m-%d %H:00"), int(v), int(err.get(t, 0)), int(thr.get(t, 0))))
        except Exception as e:
            rep.warn("   metrics: %s" % str(e)[:120])
        # logs around each drop moment (first 3 moments)
        lines = []
        try:
            for ts_iso, _ in info["moments"][:3]:
                ts = datetime.fromisoformat(ts_iso)
                r = logs.filter_log_events(
                    logGroupName="/aws/lambda/" + fn,
                    startTime=int((ts - timedelta(minutes=20)).timestamp() * 1000),
                    endTime=int((ts + timedelta(minutes=10)).timestamp() * 1000),
                    filterPattern="?chain ?depth ?hop ?cursor ?parked ?Recursive ?recursion ?fanout ?shard",
                    limit=25)
                for ev_ in r.get("events", []):
                    line = ev_["message"].strip()[:170]
                    lines.append(line)
                    rep.log("     log %s | %s" % (datetime.fromtimestamp(ev_["timestamp"] / 1000, tz=timezone.utc).strftime("%H:%M:%S"), line))
        except Exception as e:
            rep.log("   logs: %s" % str(e)[:100])
        drec["log_sample"] = lines[:60]
        detail[fn] = drec
    OUT["detail"] = detail

    # ================================================================ D
    rep.section("D. Fleet posture — Cost Explorer daily (Sep 1→today) and Lambda burn")
    try:
        r = ce.get_cost_and_usage(TimePeriod={"Start": "2026-09-01", "End": d(0)},
                                  Granularity="DAILY", Metrics=["UnblendedCost"],
                                  GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}])
        daily = {}
        for res in r["ResultsByTime"]:
            day = res["TimePeriod"]["Start"]
            row = {}
            for g in res["Groups"]:
                v = float(g["Metrics"]["UnblendedCost"]["Amount"])
                if v > 0.005:
                    row[g["Keys"][0]] = v
            daily[day] = row
        rep.log("%-10s %8s %8s %8s %8s  %s" % ("day", "TOTAL", "Lambda", "S3", "CW", "next-biggest"))
        for day in sorted(daily):
            row = daily[day]
            tot = sum(row.values())
            lam_ = sum(v for k, v in row.items() if "Lambda" in k)
            s3_ = sum(v for k, v in row.items() if "Simple Storage" in k)
            cw_ = sum(v for k, v in row.items() if "CloudWatch" in k)
            rest = sorted(((k, v) for k, v in row.items() if not any(x in k for x in ("Lambda", "Simple Storage", "CloudWatch"))), key=lambda x: -x[1])[:2]
            rep.log("%-10s %8.2f %8.2f %8.2f %8.2f  %s" % (day, tot, lam_, s3_, cw_, " ".join("%s=%.2f" % (k.split()[-1][:14], v) for k, v in rest)))
            rep.kv(section="ce_daily", day=day, total=round(tot, 2), lambda_=round(lam_, 2), s3=round(s3_, 2), cloudwatch=round(cw_, 2))
        days = sorted(daily)
        if len(days) >= 4:
            last3 = [sum(daily[x].values()) for x in days[-4:-1]]  # exclude today (partial)
            rr = sum(last3) / 3.0
            rep.log("run-rate (last 3 full days): $%.2f/day ≈ $%.0f/month" % (rr, rr * 30))
            OUT["run_rate_usd_day"] = round(rr, 2)
        OUT["ce_daily_total"] = {k: round(sum(v.values()), 2) for k, v in daily.items()}
    except Exception as e:
        rep.fail("Cost Explorer: %s" % str(e)[:160])

    # account-level hourly invocations/errors/concurrency → alarm thresholds
    thr_inv = 20000
    thr_err = 300
    thr_conc = 150
    try:
        inv_h = [v for _, v in series("Invocations", [], "Sum", 3600, NOW - timedelta(days=9))]
        err_h = [v for _, v in series("Errors", [], "Sum", 3600, NOW - timedelta(days=9))]
        con_m = [v for _, v in series("ConcurrentExecutions", [], "Maximum", 300, NOW - timedelta(days=9))]
        pi, mi = p95(inv_h), (max(inv_h) if inv_h else 0)
        pe, me = p95(err_h), (max(err_h) if err_h else 0)
        pc, mc = p95(con_m), (max(con_m) if con_m else 0)
        thr_inv = int(max(2.5 * pi, 20000))
        thr_err = int(max(3 * pe, 300))
        thr_conc = int(max(3 * pc, 150))
        rep.log("account hourly Invocations  9d: p95=%d max=%d  → alarm threshold %d/h" % (pi, mi, thr_inv))
        rep.log("account hourly Errors       9d: p95=%d max=%d  → alarm threshold %d/h" % (pe, me, thr_err))
        rep.log("account ConcurrentExecutions 9d: p95=%d max=%d → alarm threshold %d" % (pc, mc, thr_conc))
        rep.log("daily invocations (last 9d): %s" % ", ".join("%d" % int(sum(inv_h[i:i + 24])) for i in range(0, len(inv_h), 24)))
        OUT["thresholds"] = {"invocations_1h": thr_inv, "errors_1h": thr_err, "concurrency": thr_conc,
                             "obs": {"inv_p95": pi, "inv_max": mi, "err_p95": pe, "err_max": me, "conc_p95": pc, "conc_max": mc}}
    except Exception as e:
        rep.warn("account metrics: %s" % str(e)[:120])

    # top functions by invocations and GB-s (7d)
    try:
        fns = {}
        for page in lam.get_paginator("list_functions").paginate():
            for f in page["Functions"]:
                fns[f["FunctionName"]] = f.get("MemorySize", 128)
        names = sorted(fns)
        stats = {}
        START7 = NOW - timedelta(days=7)
        for i in range(0, len(names), 240):
            chunk = names[i:i + 240]
            q = []
            for j, fn in enumerate(chunk):
                for mt, tag in (("Invocations", "i"), ("Duration", "d")):
                    q.append({"Id": "%s%d" % (tag, j), "MetricStat": {"Metric": {
                        "Namespace": "AWS/Lambda", "MetricName": mt,
                        "Dimensions": [{"Name": "FunctionName", "Value": fn}]},
                        "Period": 604800, "Stat": "Sum"}, "ReturnData": True})
            res = cw.get_metric_data(MetricDataQueries=q, StartTime=START7, EndTime=NOW)
            vals = {x["Id"]: (sum(x["Values"]) if x["Values"] else 0.0) for x in res["MetricDataResults"]}
            for j, fn in enumerate(chunk):
                inv = vals.get("i%d" % j, 0.0)
                dur = vals.get("d%d" % j, 0.0)
                if inv <= 0:
                    continue
                gbs = dur / 1000.0 * fns[fn] / 1024.0
                stats[fn] = {"inv": int(inv), "gb_s": round(gbs), "usd_7d": round(gbs * GB_S + inv * REQ, 2)}
        lam_total = sum(v["usd_7d"] for v in stats.values())
        rep.log("fleet: %d functions, %d active in 7d, Lambda compute 7d ≈ $%.2f (≈ $%.0f/mo)"
                % (len(fns), len(stats), lam_total, lam_total / 7 * 30))
        rep.log("TOP by invocations (7d):")
        for fn, v in sorted(stats.items(), key=lambda x: -x[1]["inv"])[:15]:
            rep.log("   %-44s inv=%-8d GB-s=%-8d $%.2f" % (fn[:44], v["inv"], v["gb_s"], v["usd_7d"]))
            rep.kv(section="top_inv", function=fn, **v)
        rep.log("TOP by $ (7d):")
        for fn, v in sorted(stats.items(), key=lambda x: -x[1]["usd_7d"])[:15]:
            rep.log("   %-44s inv=%-8d GB-s=%-8d $%.2f" % (fn[:44], v["inv"], v["gb_s"], v["usd_7d"]))
            rep.kv(section="top_usd", function=fn, **v)
        OUT["lambda_usd_7d"] = round(lam_total, 2)
        OUT["fleet_size"] = len(fns)
    except Exception as e:
        rep.warn("fleet burn: %s" % str(e)[:120])

    # ================================================================ E
    rep.section("E. S3 loop surfaces (S3 → Lambda → same bucket)")
    try:
        nc = s3.get_bucket_notification_configuration(Bucket=BUCKET)
        lcs = nc.get("LambdaFunctionConfigurations", [])
        for lc in lcs:
            fr = " ".join("%s=%s" % (x.get("Name"), x.get("Value")) for x in lc.get("Filter", {}).get("Key", {}).get("FilterRules", []))
            rep.warn("  S3 notification → %s events=%s filter=[%s]" % (lc.get("LambdaFunctionArn", "").split(":")[-1], ",".join(lc.get("Events", [])), fr))
        rep.log("  EventBridge notifications on bucket: %s" % ("ENABLED" if nc.get("EventBridgeConfiguration") is not None else "off"))
        if not lcs:
            rep.ok("  no direct S3→Lambda notifications on %s" % BUCKET)
        OUT["s3_lambda_notifications"] = len(lcs)
    except Exception as e:
        rep.warn("  bucket notification read: %s" % str(e)[:120])
    try:
        s3rules = []
        for page in evb.get_paginator("list_rules").paginate():
            for r in page["Rules"]:
                ep = r.get("EventPattern") or ""
                if "aws.s3" in ep and r.get("State") == "ENABLED":
                    tg = evb.list_targets_by_rule(Rule=r["Name"]).get("Targets", [])
                    s3rules.append((r["Name"], [t.get("Arn", "").split(":")[-1] for t in tg]))
        for name, tgts in s3rules:
            rep.warn("  aws.s3 rule %s → %s" % (name, ", ".join(tgts)))
        if not s3rules:
            rep.ok("  no ENABLED aws.s3 EventBridge rules on the default bus")
        OUT["s3_rules"] = s3rules
    except Exception as e:
        rep.warn("  rule read: %s" % str(e)[:120])

    # ================================================================ F
    rep.section("F. GUARDRAILS — notifier, SNS wiring, alarms")
    notify_arn = None
    try:
        cfgj = json.loads((ROOT / "aws" / "lambdas" / NOTIFY_FN / "config.json").read_text())
        create_or_update_lambda(report=rep, function_name=NOTIFY_FN,
                                zip_bytes=build_zip(ROOT / "aws" / "lambdas" / NOTIFY_FN / "source"),
                                env_vars=dict(cfgj.get("env") or {}), timeout=int(cfgj["timeout"]),
                                memory=int(cfgj["memory"]), description=cfgj.get("description", "")[:250],
                                reserved_concurrency=None, create_function_url=False, ephemeral_storage=None)
        for _ in range(30):
            c = lam.get_function_configuration(FunctionName=NOTIFY_FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in (None, "Successful"):
                break
            time.sleep(4)
        notify_arn = c["FunctionArn"]
        rep.ok("  %s %s" % (NOTIFY_FN, notify_arn))
    except Exception as e:
        fails.append("notifier deploy: %s" % str(e)[:160])
        rep.fail("  notifier deploy: %s" % str(e)[:160])

    # SNS topic + subscriptions
    try:
        try:
            sns.get_topic_attributes(TopicArn=TOPIC_ARN)
            rep.log("  topic exists: %s" % TOPIC_ARN)
        except Exception:
            sns.create_topic(Name="justhodl-fleet-alerts")
            rep.ok("  topic created: %s" % TOPIC_ARN)
        subs = sns.list_subscriptions_by_topic(TopicArn=TOPIC_ARN).get("Subscriptions", [])
        for sbn in subs:
            rep.log("   sub %-8s %-60s %s" % (sbn.get("Protocol"), sbn.get("Endpoint", "")[:60], sbn.get("SubscriptionArn", "")[-24:]))
        has_lambda = any(sbn.get("Protocol") == "lambda" and NOTIFY_FN in (sbn.get("Endpoint") or "") for sbn in subs)
        has_email = any(sbn.get("Protocol") == "email" for sbn in subs)
        if notify_arn and not has_lambda:
            try:
                lam.add_permission(FunctionName=NOTIFY_FN, StatementId="sns-fleet-alerts-5250",
                                   Action="lambda:InvokeFunction", Principal="sns.amazonaws.com", SourceArn=TOPIC_ARN)
            except Exception as e:
                if "ResourceConflictException" not in str(e):
                    raise
            sns.subscribe(TopicArn=TOPIC_ARN, Protocol="lambda", Endpoint=notify_arn)
            rep.ok("  subscribed %s to the topic (Telegram + S3 ledger)" % NOTIFY_FN)
        if not has_email:
            sns.subscribe(TopicArn=TOPIC_ARN, Protocol="email", Endpoint=OWNER_EMAIL)
            rep.warn("  email subscription requested for %s — PENDING until the confirmation link is clicked" % OWNER_EMAIL)
            OUT["email_pending"] = OWNER_EMAIL
        OUT["subscriptions"] = [(s_.get("Protocol"), s_.get("Endpoint")) for s_ in subs]
    except Exception as e:
        fails.append("sns wiring: %s" % str(e)[:160])
        rep.fail("  sns wiring: %s" % str(e)[:160])

    # alarms
    alarms = []

    def alarm(name, desc, **kw):
        try:
            cw.put_metric_alarm(AlarmName=name, AlarmDescription=desc[:1000], ActionsEnabled=True,
                                AlarmActions=[TOPIC_ARN], OKActions=[TOPIC_ARN],
                                TreatMissingData="notBreaching", **kw)
            st = cw.describe_alarms(AlarmNames=[name])["MetricAlarms"][0]["StateValue"]
            alarms.append((name, st))
            rep.ok("  alarm %-42s state=%s" % (name, st))
        except Exception as e:
            fails.append("alarm %s: %s" % (name, str(e)[:140]))
            rep.fail("  alarm %s: %s" % (name, str(e)[:140]))

    alarm("justhodl-guard-lambda-recursion-dropped",
          "ops 5250: ANY function hit AWS's 16-hop recursion breaker (Metrics Insights over all FunctionName). "
          "Runbook: aws/ops/reports/latest/5250_*.md section B names the chain engines.",
          Metrics=[{"Id": "q1", "Expression": 'SELECT SUM(RecursiveInvocationsDropped) FROM SCHEMA("AWS/Lambda", FunctionName)',
                    "Period": 300, "ReturnData": True}],
          EvaluationPeriods=1, DatapointsToAlarm=1, Threshold=0, ComparisonOperator="GreaterThanThreshold")
    alarm("justhodl-guard-lambda-recursion-dropped-account",
          "ops 5250: account aggregate RecursiveInvocationsDropped > 0 (belt for the Metrics Insights alarm).",
          Namespace="AWS/Lambda", MetricName="RecursiveInvocationsDropped", Statistic="Sum", Period=300,
          EvaluationPeriods=1, DatapointsToAlarm=1, Threshold=0, ComparisonOperator="GreaterThanThreshold")
    alarm("justhodl-guard-lambda-invocations-1h",
          "ops 5250: account Lambda invocations in one hour above %d (2.5x the 9-day p95). A runaway fan-out or loop." % thr_inv,
          Namespace="AWS/Lambda", MetricName="Invocations", Statistic="Sum", Period=3600,
          EvaluationPeriods=1, DatapointsToAlarm=1, Threshold=thr_inv, ComparisonOperator="GreaterThanThreshold")
    alarm("justhodl-guard-lambda-errors-1h",
          "ops 5250: account Lambda errors in one hour above %d (3x the 9-day p95). A failing retry storm bills like a loop." % thr_err,
          Namespace="AWS/Lambda", MetricName="Errors", Statistic="Sum", Period=3600,
          EvaluationPeriods=1, DatapointsToAlarm=1, Threshold=thr_err, ComparisonOperator="GreaterThanThreshold")
    alarm("justhodl-guard-lambda-concurrency",
          "ops 5250: account ConcurrentExecutions above %d for 3 of 5 minutes (account limit 1000)." % thr_conc,
          Namespace="AWS/Lambda", MetricName="ConcurrentExecutions", Statistic="Maximum", Period=60,
          EvaluationPeriods=5, DatapointsToAlarm=3, Threshold=thr_conc, ComparisonOperator="GreaterThanThreshold")
    alarm("justhodl-guard-s3-bytes-growth-1d",
          "ops 5250: justhodl-dashboard-live grew more than 80 GB in one day (the Aug rewrite churn was ~112 GB/day of dead versions).",
          Metrics=[{"Id": "b", "MetricStat": {"Metric": {"Namespace": "AWS/S3", "MetricName": "BucketSizeBytes",
                    "Dimensions": [{"Name": "BucketName", "Value": BUCKET}, {"Name": "StorageType", "Value": "StandardStorage"}]},
                    "Period": 86400, "Stat": "Average"}, "ReturnData": False},
                   {"Id": "g", "Expression": "DIFF(b)", "Label": "daily growth bytes", "ReturnData": True}],
          EvaluationPeriods=1, DatapointsToAlarm=1, Threshold=80e9, ComparisonOperator="GreaterThanThreshold")
    alarm("justhodl-guard-s3-objects-growth-1d",
          "ops 5250: justhodl-dashboard-live object count grew by more than 2,000,000 in one day (churn = versions piling up).",
          Metrics=[{"Id": "n", "MetricStat": {"Metric": {"Namespace": "AWS/S3", "MetricName": "NumberOfObjects",
                    "Dimensions": [{"Name": "BucketName", "Value": BUCKET}, {"Name": "StorageType", "Value": "AllStorageTypes"}]},
                    "Period": 86400, "Stat": "Average"}, "ReturnData": False},
                   {"Id": "g", "Expression": "DIFF(n)", "Label": "daily object growth", "ReturnData": True}],
          EvaluationPeriods=1, DatapointsToAlarm=1, Threshold=2000000, ComparisonOperator="GreaterThanThreshold")
    OUT["alarms"] = alarms

    # prove the delivery leg end-to-end
    if notify_arn:
        try:
            r = lam.invoke(FunctionName=NOTIFY_FN, InvocationType="RequestResponse",
                           Payload=json.dumps({"test": True}).encode())
            body = json.loads(r["Payload"].read() or b"{}")
            (rep.ok if body.get("telegram_ok") else rep.warn)("  delivery test: %s" % json.dumps(body)[:200])
            OUT["delivery_test"] = body
        except Exception as e:
            rep.warn("  delivery test: %s" % str(e)[:140])

    # ================================================================ G
    rep.section("VERDICT")
    rep.log("tripped functions (14d): %s" % (", ".join("%s=%d" % (k, v["dropped"]) for k, v in tripped.items()) or "none"))
    rep.log("walk engines that break at AWS's 16: %d (section B, risk=BREAKS@16) — fix wave = ops 5251" % n_break)
    rep.log("alarms armed: %d/7; notifier: %s" % (len(alarms), notify_arn or "FAILED"))
    if fails:
        for f in fails:
            rep.fail("  ! %s" % f)
    OUT["fails"] = fails
    rp = ROOT / "aws" / "ops" / "reports" / "5250_lambda_recursion_forensics_guardrails.json"
    rp.write_text(json.dumps(OUT, indent=1, default=str), encoding="utf-8")
    rep.ok("wrote %s" % rp.name)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails[:3]))
