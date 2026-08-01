"""
ops_4227 — AWS COST FORENSICS. READ-ONLY. Nothing is mutated.

AWS Budgets forecast for Aug-2026 = $328.62 against a $150 budget, and
Lambda emitted a recursive-loop auto-remediation notice on the account.
This op answers four questions with evidence, not theory:

  A. WHERE the money goes  — Cost Explorer daily by SERVICE (45d) and by
     USAGE_TYPE (14d). Names the inflection date and the top usage types.
  B. WHICH function loops   — CloudWatch RecursiveInvocationsDropped by
     FunctionName. AWS publishes this exactly when it breaks a loop, so
     it names the culprit directly rather than by inference.
  C. WHICH functions burn   — Invocations + Duration(sum) per function for
     the fleet over 14d, converted to real GB-seconds and dollars using
     each function's own MemorySize. Ranked.
  D. WHY it loops           — S3 bucket notification configs (the classic
     Lambda->S3->Lambda cycle), event source mappings, EventBridge rules
     firing at sub-5-minute rates, and CloudWatch Logs storage/retention.

Output: aws/ops/reports/latest/4227_cost_forensics.md + reports/4227_cost_forensics.json
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

from ops_report import report

REGION = "us-east-1"
CFG = Config(retries={"max_attempts": 6, "mode": "adaptive"}, read_timeout=70)

ce = boto3.client("ce", region_name="us-east-1", config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
evb = boto3.client("events", region_name=REGION, config=CFG)
sch = boto3.client("scheduler", region_name=REGION, config=CFG)

NOW = datetime.now(timezone.utc)
GB_S_PRICE = 0.0000166667
REQ_PRICE = 0.20 / 1_000_000

OUT = {"ops": 4227, "ts": NOW.isoformat()}


def d(n):
    return (NOW - timedelta(days=n)).strftime("%Y-%m-%d")


with report("4227_cost_forensics") as rep:
    rep.heading("ops 4227 — AWS cost forensics (read-only)")

    # ---------------------------------------------------------------- A
    rep.section("A. Cost Explorer — daily cost by service (45d)")
    svc_daily, svc_total, ce_ok = {}, {}, True
    try:
        pg = ce.get_cost_and_usage(
            TimePeriod={"Start": d(45), "End": d(0)},
            Granularity="DAILY",
            Metrics=["UnblendedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
        )
        for res in pg["ResultsByTime"]:
            day = res["TimePeriod"]["Start"]
            for g in res["Groups"]:
                s = g["Keys"][0]
                v = float(g["Metrics"]["UnblendedCost"]["Amount"])
                if v <= 0:
                    continue
                svc_daily.setdefault(day, {})[s] = round(v, 4)
                svc_total[s] = svc_total.get(s, 0) + v
    except Exception as e:
        ce_ok = False
        rep.fail("Cost Explorer denied/failed: %s" % str(e)[:200])

    if ce_ok:
        top = sorted(svc_total.items(), key=lambda x: -x[1])[:14]
        rep.log("TOP SERVICES over 45d (total $):")
        for s, v in top:
            rep.log("   %-46s $%8.2f" % (s[:46], v))
            rep.kv(section="service_45d", service=s, usd=round(v, 2))
        # daily totals -> find the inflection
        rep.log("")
        rep.log("DAILY TOTALS (all services):")
        days = sorted(svc_daily)
        for day in days:
            tot = sum(svc_daily[day].values())
            bar = "#" * int(min(tot, 40) * 1.2)
            top3 = sorted(svc_daily[day].items(), key=lambda x: -x[1])[:3]
            rep.log("   %s  $%7.2f %-48s %s" % (
                day, tot, bar,
                " ".join("%s=%.2f" % (k.split()[-1][:12], v) for k, v in top3)))
        OUT["service_totals_45d"] = {k: round(v, 2) for k, v in top}
        OUT["daily_totals"] = {dd: round(sum(svc_daily[dd].values()), 2)
                               for dd in days}

    rep.section("A2. Cost Explorer — top USAGE_TYPE (14d)")
    try:
        r2 = ce.get_cost_and_usage(
            TimePeriod={"Start": d(14), "End": d(0)},
            Granularity="MONTHLY",
            Metrics=["UnblendedCost", "UsageQuantity"],
            GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}],
        )
        ut = {}
        for res in r2["ResultsByTime"]:
            for g in res["Groups"]:
                k = g["Keys"][0]
                ut[k] = ut.get(k, 0) + float(
                    g["Metrics"]["UnblendedCost"]["Amount"])
        for k, v in sorted(ut.items(), key=lambda x: -x[1])[:20]:
            rep.log("   %-52s $%8.2f" % (k[:52], v))
            rep.kv(section="usage_type_14d", usage_type=k, usd=round(v, 2))
        OUT["usage_types_14d"] = {k: round(v, 2)
                                  for k, v in sorted(ut.items(),
                                                     key=lambda x: -x[1])[:20]}
    except Exception as e:
        rep.fail("usage-type breakdown failed: %s" % str(e)[:160])

    # ---------------------------------------------------------------- B
    rep.section("B. RECURSIVE LOOP — which function did AWS break?")
    rec = {}
    try:
        paginator = cw.get_paginator("list_metrics")
        names = set()
        for page in paginator.paginate(
                Namespace="AWS/Lambda",
                MetricName="RecursiveInvocationsDropped"):
            for m in page["Metrics"]:
                for dm in m["Dimensions"]:
                    if dm["Name"] == "FunctionName":
                        names.add(dm["Value"])
        rep.log("functions with a RecursiveInvocationsDropped metric: %d"
                % len(names))
        for fn in sorted(names):
            r = cw.get_metric_statistics(
                Namespace="AWS/Lambda",
                MetricName="RecursiveInvocationsDropped",
                Dimensions=[{"Name": "FunctionName", "Value": fn}],
                StartTime=NOW - timedelta(days=14), EndTime=NOW,
                Period=86400, Statistics=["Sum"])
            tot = sum(p["Sum"] for p in r["Datapoints"])
            if tot > 0:
                rec[fn] = tot
                rep.fail("  ROGUE  %-46s dropped=%d" % (fn, tot))
                rep.kv(section="recursive", function=fn, dropped=int(tot))
        if not rec:
            rep.warn("no RecursiveInvocationsDropped datapoints in 14d "
                     "(metric only publishes at the moment of the break)")
    except Exception as e:
        rep.fail("recursion probe failed: %s" % str(e)[:200])
    OUT["recursive"] = {k: int(v) for k, v in rec.items()}

    # ---------------------------------------------------------------- C
    rep.section("C. Fleet burn — invocations + GB-seconds per function (14d)")
    fns = {}
    p = lam.get_paginator("list_functions")
    for page in p.paginate():
        for f in page["Functions"]:
            fns[f["FunctionName"]] = {
                "mem": f.get("MemorySize", 128),
                "timeout": f.get("Timeout", 3),
                "runtime": f.get("Runtime", ""),
            }
    rep.log("fleet size: %d functions" % len(fns))

    names = sorted(fns)
    stats = {}
    START = NOW - timedelta(days=14)
    for i in range(0, len(names), 160):
        chunk = names[i:i + 160]
        q = []
        for j, fn in enumerate(chunk):
            for mt, st, tag in (("Invocations", "Sum", "inv"),
                                ("Duration", "Sum", "dur"),
                                ("Errors", "Sum", "err")):
                q.append({
                    "Id": "m%s_%d" % (tag, j),
                    "MetricStat": {
                        "Metric": {"Namespace": "AWS/Lambda",
                                   "MetricName": mt,
                                   "Dimensions": [{"Name": "FunctionName",
                                                   "Value": fn}]},
                        "Period": 1209600, "Stat": st},
                    "ReturnData": True,
                })
        res = cw.get_metric_data(MetricDataQueries=q, StartTime=START,
                                 EndTime=NOW, ScanBy="TimestampDescending")
        vals = {r["Id"]: (sum(r["Values"]) if r["Values"] else 0.0)
                for r in res["MetricDataResults"]}
        for j, fn in enumerate(chunk):
            inv = vals.get("minv_%d" % j, 0.0)
            dur = vals.get("mdur_%d" % j, 0.0)
            err = vals.get("merr_%d" % j, 0.0)
            if inv <= 0 and dur <= 0:
                continue
            mem = fns[fn]["mem"]
            gbs = (dur / 1000.0) * (mem / 1024.0)
            usd = gbs * GB_S_PRICE + inv * REQ_PRICE
            stats[fn] = {"inv": int(inv), "dur_s": round(dur / 1000.0, 1),
                         "gb_s": round(gbs, 1), "usd_14d": round(usd, 2),
                         "mem": mem, "err": int(err),
                         "avg_ms": round(dur / inv, 1) if inv else 0.0}

    ranked = sorted(stats.items(), key=lambda x: -x[1]["usd_14d"])
    lam_total = sum(v["usd_14d"] for v in stats.values())
    inv_total = sum(v["inv"] for v in stats.values())
    rep.log("LAMBDA COMPUTE 14d = $%.2f across %s invocations "
            "(active functions: %d)" % (lam_total, f"{inv_total:,}",
                                        len(stats)))
    rep.log("")
    rep.log("%-46s %10s %9s %8s %7s %8s" % (
        "FUNCTION", "INVOKES", "GB-SEC", "USD14d", "MEM", "AVG_MS"))
    for fn, v in ranked[:35]:
        rep.log("%-46s %10s %9.0f %8.2f %7d %8.0f" % (
            fn[:46], f"{v['inv']:,}", v["gb_s"], v["usd_14d"], v["mem"],
            v["avg_ms"]))
        rep.kv(section="burn", function=fn, invokes=v["inv"],
               gb_sec=v["gb_s"], usd_14d=v["usd_14d"], mem=v["mem"],
               avg_ms=v["avg_ms"], errors=v["err"])
    OUT["lambda_total_usd_14d"] = round(lam_total, 2)
    OUT["lambda_invocations_14d"] = inv_total
    OUT["burn_top"] = {k: v for k, v in ranked[:40]}

    # anomaly: functions whose invocation count is wildly above fleet norm
    rep.log("")
    rep.log("INVOCATION OUTLIERS (>20,000 in 14d):")
    for fn, v in sorted(stats.items(), key=lambda x: -x[1]["inv"])[:20]:
        if v["inv"] > 20000:
            rep.warn("  %-46s %s invokes (%.0f/day)"
                     % (fn[:46], f"{v['inv']:,}", v["inv"] / 14.0))

    # ---------------------------------------------------------------- D
    rep.section("D1. S3 -> Lambda notification wiring (loop surface)")
    loops = []
    try:
        for b in s3.list_buckets()["Buckets"]:
            bn = b["Name"]
            try:
                nc = s3.get_bucket_notification_configuration(Bucket=bn)
            except Exception:
                continue
            for lc in nc.get("LambdaFunctionConfigurations", []):
                arn = lc.get("LambdaFunctionArn", "")
                fn = arn.split(":")[-1]
                pfx = ""
                for r in lc.get("Filter", {}).get("Key", {}).get(
                        "FilterRules", []):
                    pfx += "%s=%s " % (r.get("Name"), r.get("Value"))
                rep.warn("  S3 %s -> %s events=%s filter=[%s]"
                         % (bn, fn, ",".join(lc.get("Events", [])), pfx.strip()))
                loops.append({"bucket": bn, "fn": fn,
                              "events": lc.get("Events", []),
                              "filter": pfx.strip()})
                rep.kv(section="s3_trigger", bucket=bn, function=fn,
                       events=",".join(lc.get("Events", [])),
                       filter=pfx.strip() or "NONE")
            for q in nc.get("QueueConfigurations", []):
                rep.warn("  S3 %s -> SQS %s events=%s"
                         % (bn, q.get("QueueArn", "").split(":")[-1],
                            ",".join(q.get("Events", []))))
                loops.append({"bucket": bn,
                              "sqs": q.get("QueueArn", "").split(":")[-1],
                              "events": q.get("Events", [])})
        if not loops:
            rep.ok("no S3 event notifications wired to Lambda/SQS anywhere")
    except Exception as e:
        rep.fail("s3 notification scan: %s" % str(e)[:160])
    OUT["s3_triggers"] = loops

    rep.section("D2. Event source mappings (SQS/DDB-stream/Kinesis pumps)")
    esm = []
    try:
        pe = lam.get_paginator("list_event_source_mappings")
        for page in pe.paginate():
            for m in page["EventSourceMappings"]:
                if m.get("State") in ("Enabled", "Creating", "Updating"):
                    row = {"src": m.get("EventSourceArn", "")[-60:],
                           "fn": m.get("FunctionArn", "").split(":")[-1],
                           "state": m.get("State"),
                           "batch": m.get("BatchSize")}
                    esm.append(row)
                    rep.warn("  ESM %-40s -> %-40s batch=%s"
                             % (row["src"][-40:], row["fn"][:40], row["batch"]))
                    rep.kv(section="esm", **row)
        if not esm:
            rep.ok("no enabled event source mappings")
    except Exception as e:
        rep.fail("esm scan: %s" % str(e)[:160])
    OUT["event_source_mappings"] = esm

    rep.section("D3. High-frequency schedules (rate < 5 min)")
    hot = []
    try:
        pr = evb.get_paginator("list_rules")
        nrules = 0
        for page in pr.paginate():
            for r in page["Rules"]:
                nrules += 1
                expr = r.get("ScheduleExpression", "")
                if not expr:
                    continue
                bad = False
                low = expr.lower()
                if "rate(1 minute" in low or "rate(2 minute" in low \
                        or "rate(3 minute" in low or "rate(4 minute" in low:
                    bad = True
                if low.startswith("cron(") and low[5:7] in ("*/", "0/"):
                    bad = True
                if bad and r.get("State") == "ENABLED":
                    tg = evb.list_targets_by_rule(Rule=r["Name"])
                    for t in tg.get("Targets", []):
                        fn = t.get("Arn", "").split(":")[-1]
                        hot.append({"rule": r["Name"], "expr": expr, "fn": fn})
                        rep.warn("  HOT %-38s %-22s -> %s"
                                 % (r["Name"][:38], expr, fn))
                        rep.kv(section="hot_schedule", rule=r["Name"],
                               expr=expr, function=fn)
        rep.log("total EventBridge rules: %d" % nrules)
        OUT["eventbridge_rules"] = nrules
    except Exception as e:
        rep.fail("eventbridge scan: %s" % str(e)[:160])
    try:
        ns = 0
        ps = sch.get_paginator("list_schedules")
        for page in ps.paginate():
            for s_ in page["Schedules"]:
                ns += 1
                e_ = s_.get("ScheduleExpression", "") or ""
                if "rate(1 minute" in e_ or "rate(2 minute" in e_:
                    hot.append({"rule": "scheduler:" + s_["Name"],
                                "expr": e_, "fn": "?"})
                    rep.warn("  HOT scheduler %-30s %s" % (s_["Name"][:30], e_))
        rep.log("total EventBridge Scheduler schedules: %d" % ns)
        OUT["scheduler_schedules"] = ns
    except Exception as e:
        rep.warn("scheduler scan: %s" % str(e)[:120])
    OUT["hot_schedules"] = hot

    rep.section("D4. CloudWatch Logs — storage + retention (silent cost)")
    lg = []
    try:
        pl = logs.get_paginator("describe_log_groups")
        for page in pl.paginate():
            for g in page["logGroups"]:
                lg.append({"name": g["logGroupName"],
                           "gb": round(g.get("storedBytes", 0) / 1e9, 3),
                           "ret": g.get("retentionInDays")})
        tot_gb = sum(x["gb"] for x in lg)
        never = [x for x in lg if x["ret"] is None]
        rep.log("log groups: %d   stored: %.1f GB   never-expiring: %d "
                "(%.1f GB)" % (len(lg), tot_gb, len(never),
                               sum(x["gb"] for x in never)))
        rep.log("storage cost ~$%.2f/mo at $0.03/GB" % (tot_gb * 0.03))
        for x in sorted(lg, key=lambda z: -z["gb"])[:15]:
            rep.log("   %-62s %7.2f GB  ret=%s"
                    % (x["name"][-62:], x["gb"], x["ret"]))
            rep.kv(section="logs", group=x["name"], gb=x["gb"],
                   retention=x["ret"])
        OUT["logs"] = {"groups": len(lg), "stored_gb": round(tot_gb, 1),
                       "never_expiring": len(never),
                       "never_expiring_gb": round(
                           sum(x["gb"] for x in never), 1)}
    except Exception as e:
        rep.fail("logs scan: %s" % str(e)[:160])

    rep.section("D5. Functions with unbounded config (900s timeout, big mem)")
    heavy = [(f, v) for f, v in fns.items()
             if v["timeout"] >= 600 or v["mem"] >= 3008]
    rep.log("functions with timeout>=600s or mem>=3008MB: %d" % len(heavy))
    for f, v in sorted(heavy, key=lambda x: -(x[1]["timeout"] * x[1]["mem"]))[:20]:
        st = stats.get(f, {})
        rep.log("   %-46s t=%4ds mem=%5d inv14d=%s usd=%.2f"
                % (f[:46], v["timeout"], v["mem"],
                   f"{st.get('inv', 0):,}", st.get("usd_14d", 0)))
        rep.kv(section="heavy", function=f, timeout=v["timeout"],
               mem=v["mem"], inv_14d=st.get("inv", 0),
               usd_14d=st.get("usd_14d", 0))

    # ---------------------------------------------------------------- write
    rp = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) \
        / "aws" / "ops" / "reports" / "4227_cost_forensics.json"
    rp.write_text(json.dumps(OUT, indent=1, default=str), encoding="utf-8")
    rep.ok("wrote %s" % rp.name)
    rep.log("OPS 4227 COMPLETE — read-only, nothing mutated.")
