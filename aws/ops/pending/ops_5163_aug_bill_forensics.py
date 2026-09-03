"""ops_5163 -- August-2026 invoice forensics ($484.26). READ-ONLY.

Khalid's questions, in his words: "why so much? which lambdas are causing
this? make sure after the download is done cost comes down to where it is
supposed to be."

This op answers them from ground truth, not memory:

  A. INVOICE RECONCILIATION -- Cost Explorer MONTHLY Aug-01..Sep-01 by
     RECORD_TYPE (usage vs tax vs credit) and by SERVICE. Must land on the
     invoice figure or the rest of the report is not trustworthy.
  B. USAGE TYPES -- top 40 August usage types with quantities. This is
     where "S3 requests" vs "S3 storage" vs "Lambda GB-s" vs "CloudWatch"
     separate.
  C. DAILY CURVE -- Aug-01..yesterday by service. Names the anomaly window
     (the series-extractor version churn, ops 5024-5028) and prices the
     excess above the Aug-01..08 baseline. Then prices the post-fix days
     and projects September from its own MTD run-rate.
  D. S3 -- last 10 days by usage type and by operation, so the request
     tiers and storage byte-hours can be seen returning to baseline.
  E. LAMBDA -- every function's Invocations + Duration for August and for
     September MTD, converted to GB-seconds and dollars with each
     function's own MemorySize. Ranked. Reconciled against the CE Lambda
     line so the ranking is known to be complete.
  F. S3 WRITER ATTRIBUTION -- the server access logs armed by ops 5024
     (requester session == Lambda function name). Samples the most recent
     hours, aggregates requests by function x operation class, prices
     them at list rates and extrapolates to a month. This is the only
     evidence that names WHO is hitting S3 right now.
  G. STORAGE STATE -- live-bucket size/objects now vs the 2.59TB/9.69M
     seen on Aug-26, lifecycle purge rule still armed, versioning status.
  H. STANDBY LINES -- any non-platform service still billing in
     September (EC2/ELB/OpenSearch/SageMaker/AppRunner/IPv4).
  I. VERDICT -- one table: August total, anomaly excess, Lambda share,
     September projection, and what is still above the landing zone.

Nothing is mutated. Report: aws/ops/reports/latest/ops_5163_aug_bill_forensics.md
"""
import gzip
import json
import re
import sys
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
LIVE_BUCKET = "justhodl-dashboard-live"
LOG_BUCKET = "justhodl-s3-access-logs-%s" % ACCOUNT
CFG = Config(retries={"max_attempts": 8, "mode": "adaptive"}, read_timeout=120)

ce = boto3.client("ce", region_name="us-east-1", config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)

NOW = datetime.now(timezone.utc)
TODAY = NOW.date()
AUG1, SEP1 = date(2026, 8, 1), date(2026, 9, 1)
ANOM_START, ANOM_END = date(2026, 8, 9), date(2026, 8, 29)  # ops 5024-5028

# list prices, us-east-1
LAMBDA_GBS = 0.0000166667
LAMBDA_REQ = 0.20 / 1e6
S3_TIER1 = 0.005 / 1000     # PUT COPY POST LIST
S3_TIER2 = 0.0004 / 1000    # GET SELECT and all others
S3_EGRESS_GB = 0.09

OUT = {"ops": 5163, "ts": NOW.isoformat(), "invoice_reported": 484.26}
FAILS = []


def money(x):
    return "$%.2f" % x


def ce_query(start, end, gran, group_key, metrics=("UnblendedCost",),
             filt=None):
    """Return {period_start: {group: {metric: float}}} across all pages."""
    out = defaultdict(lambda: defaultdict(dict))
    kw = dict(TimePeriod={"Start": start.isoformat(), "End": end.isoformat()},
              Granularity=gran, Metrics=list(metrics),
              GroupBy=[{"Type": "DIMENSION", "Key": group_key}])
    if filt:
        kw["Filter"] = filt
    token = None
    while True:
        if token:
            kw["NextPageToken"] = token
        r = ce.get_cost_and_usage(**kw)
        for res in r["ResultsByTime"]:
            p = res["TimePeriod"]["Start"]
            for g in res["Groups"]:
                k = g["Keys"][0]
                for m in metrics:
                    out[p][k][m] = float(g["Metrics"][m]["Amount"])
        token = r.get("NextPageToken")
        if not token:
            break
    return out


def svc_bucket(name):
    n = name.lower()
    if "simple storage" in n:
        return "S3"
    if "lambda" in n:
        return "Lambda"
    if "cloudwatch" in n:
        return "CloudWatch"
    if "dynamodb" in n:
        return "DynamoDB"
    if "eventbridge" in n or "cloudwatch events" in n:
        return "EventBridge"
    if "tax" in n:
        return "Tax"
    return "Other"


STANDBY_HINTS = ("elastic compute cloud", "ec2", "elastic load balancing",
                 "opensearch", "elasticsearch", "sagemaker", "app runner",
                 "relational database", "virtual private cloud",
                 "amazon elastic file", "elastic container", "lightsail")


with report("ops_5163_aug_bill_forensics") as r:
    r.heading("ops 5163 -- August-2026 invoice forensics ($484.26), read-only")
    r.log("account %s  now=%s  invoice window %s..%s (End exclusive)"
          % (ACCOUNT, NOW.strftime("%Y-%m-%d %H:%MZ"), AUG1, SEP1))

    # ================================================================ A
    r.section("A. Invoice reconciliation -- Cost Explorer, August by record type and service")
    aug_total = 0.0
    aug_by_svc = {}
    try:
        rt = ce_query(AUG1, SEP1, "MONTHLY", "RECORD_TYPE")
        for p, groups in rt.items():
            for k, m in groups.items():
                r.log("  record_type %-14s %s" % (k, money(m["UnblendedCost"])))
                r.kv(section="A_record_type", record_type=k,
                     usd=round(m["UnblendedCost"], 2))
        bs = ce_query(AUG1, SEP1, "MONTHLY", "SERVICE")
        for p, groups in bs.items():
            for k, m in groups.items():
                aug_by_svc[k] = aug_by_svc.get(k, 0.0) + m["UnblendedCost"]
        aug_total = sum(aug_by_svc.values())
        r.log("")
        r.log("AUGUST BY SERVICE (unblended, all record types):")
        for k, v in sorted(aug_by_svc.items(), key=lambda x: -x[1]):
            if abs(v) < 0.005:
                continue
            r.log("   %-48s %10s  %5.1f%%" % (k[:48], money(v),
                                             100.0 * v / aug_total if aug_total else 0))
            r.kv(section="A_service", service=k, usd=round(v, 2))
        r.log("")
        delta = aug_total - 484.26
        if abs(delta) <= 15.0:
            r.ok("CE August total %s reconciles to the $484.26 invoice (delta %+.2f)"
                 % (money(aug_total), delta))
        else:
            r.warn("CE August total %s vs invoice $484.26 (delta %+.2f) -- "
                   "tax/credits/timing; treat shares as approximate"
                   % (money(aug_total), delta))
        OUT["aug_total_ce"] = round(aug_total, 2)
        OUT["aug_by_service"] = {k: round(v, 2) for k, v in aug_by_svc.items()}
    except Exception as e:
        FAILS.append("CE monthly: %s" % str(e)[:200])
        r.fail("Cost Explorer monthly query failed: %s" % str(e)[:200])

    # ================================================================ B
    r.section("B. August top usage types (cost + quantity)")
    aug_ut = {}
    try:
        ut = ce_query(AUG1, SEP1, "MONTHLY", "USAGE_TYPE",
                      metrics=("UnblendedCost", "UsageQuantity"))
        for p, groups in ut.items():
            for k, m in groups.items():
                aug_ut[k] = (aug_ut.get(k, (0.0, 0.0))[0] + m["UnblendedCost"],
                             aug_ut.get(k, (0.0, 0.0))[1] + m["UsageQuantity"])
        r.log("%-52s %10s %18s" % ("USAGE_TYPE", "USD", "QUANTITY"))
        for k, (v, q) in sorted(aug_ut.items(), key=lambda x: -x[1][0])[:40]:
            if v < 0.005:
                continue
            r.log("   %-49s %10s %18s" % (k[:49], money(v), "{:,.0f}".format(q)))
            r.kv(section="B_usage_type", usage_type=k, usd=round(v, 2),
                 qty=round(q, 1))
        OUT["aug_usage_types"] = {k: [round(v, 2), round(q, 1)]
                                  for k, (v, q) in sorted(aug_ut.items(),
                                                          key=lambda x: -x[1][0])[:60]}
    except Exception as e:
        FAILS.append("CE usage types: %s" % str(e)[:200])
        r.fail("usage-type query failed: %s" % str(e)[:200])

    # ================================================================ C
    r.section("C. Daily curve Aug-01..yesterday by service -- anomaly window priced")
    daily = {}          # day -> {bucket: usd}
    daily_svc = {}      # day -> {service: usd}
    baseline_s3 = baseline_all = 0.0
    anomaly_excess_s3 = anomaly_excess_all = 0.0
    sep_days = []
    try:
        d = ce_query(AUG1, TODAY, "DAILY", "SERVICE")
        for p in sorted(d):
            day = date.fromisoformat(p)
            row = defaultdict(float)
            srow = {}
            for k, m in d[p].items():
                v = m["UnblendedCost"]
                if svc_bucket(k) == "Tax":
                    continue
                row[svc_bucket(k)] += v
                srow[k] = v
            daily[day] = dict(row)
            daily_svc[day] = srow
        pre = [x for x in daily if AUG1 <= x < ANOM_START]
        if pre:
            baseline_s3 = sum(daily[x].get("S3", 0) for x in pre) / len(pre)
            baseline_all = sum(sum(daily[x].values()) for x in pre) / len(pre)
        r.log("baseline Aug-01..08: all-services %s/day, S3 %s/day"
              % (money(baseline_all), money(baseline_s3)))
        r.log("")
        r.log("%-10s %8s %8s %8s %8s %8s %8s  %s" % (
            "DAY", "TOTAL", "S3", "LAMBDA", "CW", "DDB", "OTHER", ""))
        for day in sorted(daily):
            row = daily[day]
            tot = sum(row.values())
            other = tot - sum(row.get(k, 0) for k in ("S3", "Lambda", "CloudWatch", "DynamoDB"))
            tag = ""
            if ANOM_START <= day <= ANOM_END:
                tag = "<< churn window"
                anomaly_excess_s3 += max(0.0, row.get("S3", 0) - baseline_s3)
                anomaly_excess_all += max(0.0, tot - baseline_all)
            elif day >= SEP1:
                tag = "SEP"
                sep_days.append(day)
            bar = "#" * int(min(tot, 60))
            r.log("%s %8.2f %8.2f %8.2f %8.2f %8.2f %8.2f  %s %s" % (
                day, tot, row.get("S3", 0), row.get("Lambda", 0),
                row.get("CloudWatch", 0), row.get("DynamoDB", 0), other, bar, tag))
            r.kv(section="C_daily", day=str(day), total=round(tot, 2),
                 s3=round(row.get("S3", 0), 2), lam=round(row.get("Lambda", 0), 2),
                 cw=round(row.get("CloudWatch", 0), 2), other=round(other, 2))
        post = [x for x in daily if ANOM_END < x < SEP1]
        post_avg = (sum(sum(daily[x].values()) for x in post) / len(post)) if post else 0.0
        sep_avg = (sum(sum(daily[x].values()) for x in sep_days) / len(sep_days)) if sep_days else 0.0
        r.log("")
        r.log("EXCESS above the Aug-01..08 baseline inside the churn window "
              "%s..%s: S3 %s, all services %s"
              % (ANOM_START, ANOM_END, money(anomaly_excess_s3), money(anomaly_excess_all)))
        r.log("post-fix Aug-30/31 average %s/day; September MTD average %s/day "
              "over %d complete day(s) -> %s/month projected"
              % (money(post_avg), money(sep_avg), len(sep_days), money(sep_avg * 30)))
        OUT["baseline_daily_all"] = round(baseline_all, 2)
        OUT["baseline_daily_s3"] = round(baseline_s3, 2)
        OUT["anomaly_excess_s3"] = round(anomaly_excess_s3, 2)
        OUT["anomaly_excess_all"] = round(anomaly_excess_all, 2)
        OUT["sep_mtd_daily_avg"] = round(sep_avg, 2)
        OUT["sep_projection"] = round(sep_avg * 30, 2)
        OUT["daily"] = {str(k): {kk: round(vv, 2) for kk, vv in v.items()}
                        for k, v in daily.items()}
    except Exception as e:
        FAILS.append("CE daily: %s" % str(e)[:200])
        r.fail("daily query failed: %s" % str(e)[:200])

    # ================================================================ D
    r.section("D. S3 last 10 days -- usage types and operations back to baseline?")
    try:
        s3f = {"Dimensions": {"Key": "SERVICE",
                              "Values": ["Amazon Simple Storage Service"]}}
        d10 = TODAY - timedelta(days=10)
        du = ce_query(d10, TODAY, "DAILY", "USAGE_TYPE",
                      metrics=("UnblendedCost", "UsageQuantity"), filt=s3f)
        keys = set()
        for p in du:
            keys.update(k for k, m in du[p].items() if m["UnblendedCost"] > 0.01)
        keys = sorted(keys, key=lambda k: -sum(du[p].get(k, {}).get("UnblendedCost", 0) for p in du))[:8]
        r.log("%-10s " % "DAY" + " ".join("%14s" % k.replace("USE1-", "")[-14:] for k in keys))
        for p in sorted(du):
            r.log("%-10s " % p + " ".join("%14.2f" % du[p].get(k, {}).get("UnblendedCost", 0) for k in keys))
            r.kv(section="D_s3_daily", day=p,
                 **{k.replace("USE1-", "")[-20:]: round(du[p].get(k, {}).get("UnblendedCost", 0), 2)
                    for k in keys})
        dop = ce_query(d10, TODAY, "DAILY", "OPERATION",
                       metrics=("UnblendedCost", "UsageQuantity"), filt=s3f)
        r.log("")
        r.log("S3 by OPERATION, last 10 days (cost / requests):")
        agg = defaultdict(lambda: [0.0, 0.0])
        for p in dop:
            for k, m in dop[p].items():
                agg[k][0] += m["UnblendedCost"]
                agg[k][1] += m["UsageQuantity"]
        for k, (v, q) in sorted(agg.items(), key=lambda x: -x[1][0])[:12]:
            r.log("   %-28s %9s  qty %16s" % (k[:28], money(v), "{:,.0f}".format(q)))
            r.kv(section="D_s3_ops10d", operation=k, usd=round(v, 2), qty=round(q))
        OUT["s3_ops_10d"] = {k: [round(v, 2), round(q)] for k, (v, q) in agg.items()}
    except Exception as e:
        r.warn("S3 10-day breakdown failed: %s" % str(e)[:160])

    # ================================================================ E
    r.section("E. Lambda burn per function -- August and September MTD")
    fns = {}
    try:
        for page in lam.get_paginator("list_functions").paginate():
            for f in page["Functions"]:
                fns[f["FunctionName"]] = {"mem": f.get("MemorySize", 128),
                                          "timeout": f.get("Timeout", 3)}
        r.log("fleet: %d functions" % len(fns))
    except Exception as e:
        FAILS.append("list_functions: %s" % str(e)[:160])
        r.fail("list_functions failed: %s" % str(e)[:160])

    def burn(start_dt, end_dt):
        period = 86400  # day-aligned points; summed below (31 x 480 metrics << 100,800 cap)
        names = sorted(fns)
        stats = {}
        for i in range(0, len(names), 160):
            chunk = names[i:i + 160]
            q = []
            for j, fn in enumerate(chunk):
                for mt, tag in (("Invocations", "inv"), ("Duration", "dur"),
                                ("Errors", "err")):
                    q.append({"Id": "m%s_%d" % (tag, j),
                              "MetricStat": {"Metric": {
                                  "Namespace": "AWS/Lambda", "MetricName": mt,
                                  "Dimensions": [{"Name": "FunctionName", "Value": fn}]},
                                  "Period": period, "Stat": "Sum"},
                              "ReturnData": True})
            token = None
            vals = {}
            while True:
                kw = dict(MetricDataQueries=q, StartTime=start_dt, EndTime=end_dt,
                          ScanBy="TimestampDescending")
                if token:
                    kw["NextToken"] = token
                res = cw.get_metric_data(**kw)
                for m in res["MetricDataResults"]:
                    vals[m["Id"]] = vals.get(m["Id"], 0.0) + (sum(m["Values"]) if m["Values"] else 0.0)
                token = res.get("NextToken")
                if not token:
                    break
            for j, fn in enumerate(chunk):
                inv = vals.get("minv_%d" % j, 0.0)
                dur = vals.get("mdur_%d" % j, 0.0)
                err = vals.get("merr_%d" % j, 0.0)
                if inv <= 0 and dur <= 0:
                    continue
                gbs = (dur / 1000.0) * (fns[fn]["mem"] / 1024.0)
                stats[fn] = {"inv": int(inv), "gb_s": gbs,
                             "usd": gbs * LAMBDA_GBS + inv * LAMBDA_REQ,
                             "mem": fns[fn]["mem"], "err": int(err),
                             "avg_s": (dur / inv / 1000.0) if inv else 0.0}
        return stats

    def show(stats, label, days, top=40):
        ranked = sorted(stats.items(), key=lambda x: -x[1]["usd"])
        tot = sum(v["usd"] for v in stats.values())
        tinv = sum(v["inv"] for v in stats.values())
        r.log("%s: computed Lambda cost %s across %s invocations, %d active functions"
              % (label, money(tot), "{:,}".format(tinv), len(stats)))
        r.log("%-46s %11s %10s %8s %6s %7s %6s" % (
            "FUNCTION", "INVOKES", "GB-SEC", "USD", "MEM", "AVG_S", "ERR"))
        for fn, v in ranked[:top]:
            r.log("%-46s %11s %10.0f %8.2f %6d %7.1f %6d" % (
                fn[:46], "{:,}".format(v["inv"]), v["gb_s"], v["usd"], v["mem"],
                v["avg_s"], v["err"]))
            r.kv(section="E_" + label.replace(" ", "_"), function=fn,
                 invokes=v["inv"], gb_s=round(v["gb_s"]), usd=round(v["usd"], 2),
                 mem=v["mem"], avg_s=round(v["avg_s"], 1), errors=v["err"])
        return tot, ranked

    aug_lambda_calc = sep_lambda_calc = 0.0
    aug_rank = sep_rank = []
    if fns:
        try:
            a_start = datetime(2026, 8, 1, tzinfo=timezone.utc)
            a_end = datetime(2026, 9, 1, tzinfo=timezone.utc)
            st_aug = burn(a_start, a_end)
            aug_lambda_calc, aug_rank = show(st_aug, "AUGUST", 31)
            ce_lambda = sum(v for k, v in aug_by_svc.items() if svc_bucket(k) == "Lambda")
            r.log("CE says Lambda service August = %s; computed compute+requests = %s "
                  "(difference = tiered/other Lambda usage types, see B)"
                  % (money(ce_lambda), money(aug_lambda_calc)))
            OUT["aug_lambda_ce"] = round(ce_lambda, 2)
            OUT["aug_lambda_calc"] = round(aug_lambda_calc, 2)
            OUT["aug_lambda_top"] = [
                {"fn": k, "inv": v["inv"], "gb_s": round(v["gb_s"]),
                 "usd": round(v["usd"], 2), "mem": v["mem"], "err": v["err"]}
                for k, v in aug_rank[:60]]
        except Exception as e:
            FAILS.append("lambda august burn: %s" % str(e)[:200])
            r.fail("August Lambda burn failed: %s" % str(e)[:200])
        try:
            s_start = datetime(2026, 9, 1, tzinfo=timezone.utc)
            st_sep = burn(s_start, NOW)
            span_days = max((NOW - s_start).total_seconds() / 86400.0, 0.01)
            r.log("")
            sep_lambda_calc, sep_rank = show(st_sep, "SEPTEMBER MTD", span_days, top=30)
            r.log("September MTD Lambda run-rate: %s/day -> %s/month"
                  % (money(sep_lambda_calc / span_days), money(sep_lambda_calc / span_days * 30)))
            OUT["sep_lambda_calc"] = round(sep_lambda_calc, 2)
            OUT["sep_lambda_daily"] = round(sep_lambda_calc / span_days, 2)
            OUT["sep_lambda_top"] = [
                {"fn": k, "inv": v["inv"], "gb_s": round(v["gb_s"]),
                 "usd": round(v["usd"], 2), "mem": v["mem"], "err": v["err"]}
                for k, v in sep_rank[:40]]
            # invocation storms right now
            r.log("")
            r.log("invocation outliers in September MTD (>5,000/day):")
            n_out = 0
            for fn, v in sorted(st_sep.items(), key=lambda x: -x[1]["inv"])[:25]:
                per_day = v["inv"] / span_days
                if per_day > 5000:
                    n_out += 1
                    r.warn("   %-46s %s/day  (%s invocations)"
                           % (fn[:46], "{:,.0f}".format(per_day), "{:,}".format(v["inv"])))
            if not n_out:
                r.ok("no function above 5,000 invocations/day in September")
        except Exception as e:
            FAILS.append("lambda sep burn: %s" % str(e)[:200])
            r.fail("September Lambda burn failed: %s" % str(e)[:200])

    # ================================================================ F
    r.section("F. S3 request attribution by requester -- server access logs (ops 5024)")
    LOG_RE = re.compile(
        r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" '
        r'(\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" "([^"]*)"')

    def requester_name(req):
        if req == "-":
            return "anonymous"
        if ":assumed-role/" in req:
            parts = req.split("/")
            return parts[-1] if len(parts) >= 3 else req[-40:]
        if ":user/" in req:
            return "iam-user:" + req.split("/")[-1]
        if req.startswith("svc:"):
            return req
        return req[-48:]

    def op_class(op):
        u = op.upper()
        if u.startswith("S3."):          # lifecycle/replication/inventory, not billed as requests
            return "lifecycle"
        if any(x in u for x in ("PUT", "POST", "COPY", "GET.BUCKET", "LIST", "MULTIPART")):
            return "tier1"
        return "tier2"

    try:
        # discover partition layout: walk the deepest / latest prefix
        prefix = "live/"
        for _depth in range(8):
            lr = s3.list_objects_v2(Bucket=LOG_BUCKET, Prefix=prefix, Delimiter="/")
            cps = [c["Prefix"] for c in lr.get("CommonPrefixes", [])]
            objs = lr.get("Contents", [])
            if objs and not cps:
                break
            if not cps:
                break
            prefix = sorted(cps)[-1]
        r.log("access-log latest partition: s3://%s/%s" % (LOG_BUCKET, prefix))
        # gather recent objects (last 3h) inside the latest partition (plus previous day if thin)
        cutoff = NOW - timedelta(hours=3)
        allobj, recent = [], []
        for page in s3.get_paginator("list_objects_v2").paginate(Bucket=LOG_BUCKET, Prefix=prefix):
            for o in page.get("Contents", []):
                allobj.append(o)
                if o["LastModified"] >= cutoff:
                    recent.append(o)
        recent.sort(key=lambda o: o["LastModified"], reverse=True)
        r.log("log objects under that partition: %d total, %d delivered in the last 3h"
              % (len(allobj), len(recent)))
        if len(recent) < 5:
            recent = sorted(allobj, key=lambda o: o["Key"])[-200:]
            recent.reverse()
            r.warn("thin last-3h sample -- falling back to the latest %d objects by key" % len(recent))
        MAX_FILES, MAX_BYTES, MAX_SECS = 500, 200 * 1024 * 1024, 240
        t0 = time.time()
        n_files = n_bytes = n_lines = 0
        first_ts = last_ts = None
        by_req = defaultdict(lambda: defaultdict(int))      # requester -> class -> count
        by_req_op = defaultdict(lambda: defaultdict(int))   # requester -> op -> count
        egress = defaultdict(int)                           # requester -> bytes_sent on GET
        by_prefix_put = defaultdict(int)                    # top-level key prefix -> puts
        TS_FMT = "%d/%b/%Y:%H:%M:%S"
        for o in recent:
            if n_files >= MAX_FILES or n_bytes >= MAX_BYTES or time.time() - t0 > MAX_SECS:
                break
            body = s3.get_object(Bucket=LOG_BUCKET, Key=o["Key"])["Body"].read()
            n_files += 1
            n_bytes += len(body)
            if o["Key"].endswith(".gz"):
                body = gzip.decompress(body)
            for line in body.decode("utf-8", "ignore").splitlines():
                m = LOG_RE.match(line)
                if not m:
                    continue
                n_lines += 1
                ts = m.group(3).split(" ")[0]
                try:
                    tsd = datetime.strptime(ts, TS_FMT)
                    first_ts = tsd if first_ts is None or tsd < first_ts else first_ts
                    last_ts = tsd if last_ts is None or tsd > last_ts else last_ts
                except Exception:
                    pass
                who = requester_name(m.group(5))
                op = m.group(7)
                cls = op_class(op)
                by_req[who][cls] += 1
                by_req_op[who][op] += 1
                if "GET.OBJECT" in op.upper():
                    try:
                        egress[who] += int(m.group(12)) if m.group(12) != "-" else 0
                    except Exception:
                        pass
                if cls == "tier1" and "PUT" in op.upper():
                    key = m.group(8)
                    parts = key.split("/")
                    by_prefix_put["/".join(parts[:3])] += 1
        span_h = 0.0
        if first_ts and last_ts:
            span_h = max((last_ts - first_ts).total_seconds() / 3600.0, 0.05)
        r.log("parsed %d files, %.1f MB, %s request lines, time span %.2fh (%s..%s)"
              % (n_files, n_bytes / 1e6, "{:,}".format(n_lines), span_h,
                 first_ts, last_ts))
        if n_lines and span_h:
            scale_day = 24.0 / span_h
            rows = []
            for who, cls in by_req.items():
                t1, t2 = cls.get("tier1", 0), cls.get("tier2", 0)
                # in-region Lambda -> S3 reads carry no transfer charge; only
                # anonymous / IAM-user readers (edge proxy, runner, laptop) do
                eg_billable = egress[who] if (who == "anonymous" or who.startswith("iam-user:")) else 0
                usd_day = (t1 * scale_day * S3_TIER1 + t2 * scale_day * S3_TIER2
                           + eg_billable * scale_day / 1e9 * S3_EGRESS_GB)
                rows.append((who, t1, t2, cls.get("lifecycle", 0), egress[who], usd_day))
            rows.sort(key=lambda x: -x[5])
            r.log("")
            r.log("%-40s %10s %10s %9s %9s %10s" % (
                "REQUESTER (session == Lambda name)", "T1/day", "T2/day", "GET GB/d", "$/day", "$/month"))
            for who, t1, t2, lc, eg, usd_day in rows[:30]:
                r.log("%-40s %10s %10s %9.2f %9.3f %10.2f" % (
                    who[:40], "{:,.0f}".format(t1 * scale_day), "{:,.0f}".format(t2 * scale_day),
                    eg * scale_day / 1e9, usd_day, usd_day * 30))
                top_ops = sorted(by_req_op[who].items(), key=lambda x: -x[1])[:3]
                r.log("      ops: " + ", ".join("%s=%d" % (k, v) for k, v in top_ops))
                r.kv(section="F_s3_requesters", requester=who,
                     tier1_per_day=round(t1 * scale_day), tier2_per_day=round(t2 * scale_day),
                     get_gb_per_day=round(eg * scale_day / 1e9, 2),
                     usd_per_month=round(usd_day * 30, 2))
            tot_day = sum(x[5] for x in rows)
            r.log("")
            r.log("all requesters: %s/day -> %s/month in S3 request + egress charges "
                  "(storage byte-hours excluded)" % (money(tot_day), money(tot_day * 30)))
            r.log("top PUT prefixes in sample:")
            for k, v in sorted(by_prefix_put.items(), key=lambda x: -x[1])[:12]:
                r.log("   %-50s %8s puts/day" % (k[:50], "{:,.0f}".format(v * scale_day)))
            OUT["s3_requesters"] = [
                {"who": who, "t1_day": round(t1 * scale_day), "t2_day": round(t2 * scale_day),
                 "get_gb_day": round(eg * scale_day / 1e9, 2), "usd_month": round(usd_day * 30, 2)}
                for who, t1, t2, lc, eg, usd_day in rows[:40]]
            OUT["s3_put_prefixes"] = {k: round(v * scale_day) for k, v in
                                      sorted(by_prefix_put.items(), key=lambda x: -x[1])[:20]}
        else:
            r.warn("no parseable access-log lines in the sample window")
    except Exception as e:
        r.warn("access-log attribution unavailable: %s" % str(e)[:200])

    # ================================================================ G
    r.section("G. Storage state -- live bucket now vs the Aug-26 peak, purge rule, versioning")

    def s3_metric(bucket, metric, stype):
        try:
            res = cw.get_metric_statistics(
                Namespace="AWS/S3", MetricName=metric,
                Dimensions=[{"Name": "BucketName", "Value": bucket},
                            {"Name": "StorageType", "Value": stype}],
                StartTime=NOW - timedelta(days=4), EndTime=NOW, Period=86400,
                Statistics=["Average"])
            pts = sorted(res.get("Datapoints", []), key=lambda p: p["Timestamp"])
            return (pts[-1]["Average"], pts[-1]["Timestamp"]) if pts else (None, None)
        except Exception as e:
            return (None, str(e)[:80])

    for b in (LIVE_BUCKET, LOG_BUCKET):
        sz, ts = s3_metric(b, "BucketSizeBytes", "StandardStorage")
        n, _ = s3_metric(b, "NumberOfObjects", "AllStorageTypes")
        r.log("  %-44s %8s GB  %14s objects  (as of %s)"
              % (b, "%.1f" % (sz / 1e9) if sz else "n/a",
                 "{:,.0f}".format(n) if n else "n/a", str(ts)[:16]))
        r.kv(section="G_storage", bucket=b, gb=round(sz / 1e9, 1) if sz else None,
             objects=int(n) if n else None)
        if b == LIVE_BUCKET and sz:
            OUT["live_bucket_gb"] = round(sz / 1e9, 1)
            OUT["live_bucket_objects"] = int(n) if n else None
            r.log("     Aug-26 peak was ~2,590 GB / 9.69M objects (ops 5024); "
                  "storage at $0.023/GB-mo = %s/month now" % money(sz / 1e9 * 0.023))
    try:
        ver = s3.get_bucket_versioning(Bucket=LIVE_BUCKET).get("Status")
        r.log("  versioning: %s" % ver)
        lc = s3.get_bucket_lifecycle_configuration(Bucket=LIVE_BUCKET).get("Rules", [])
        for rule in lc:
            flt = rule.get("Filter", {})
            pfx = flt.get("Prefix") or (flt.get("And") or {}).get("Prefix") or rule.get("Prefix") or ""
            r.log("  lifecycle %-44s %s prefix='%s' noncurrent_exp=%s exp=%s"
                  % (rule.get("ID", "?")[:44], rule.get("Status"), pfx,
                     (rule.get("NoncurrentVersionExpiration") or {}).get("NoncurrentDays"),
                     (rule.get("Expiration") or {}).get("Days")))
        OUT["lifecycle_rules"] = len(lc)
    except Exception as e:
        r.warn("bucket config read: %s" % str(e)[:140])

    # ================================================================ H
    r.section("H. Standby / non-platform services still billing in September")
    try:
        sep = ce_query(SEP1, TODAY, "MONTHLY", "SERVICE") if TODAY > SEP1 else {}
        found = 0
        for p, groups in sep.items():
            for k, m in sorted(groups.items(), key=lambda x: -x[1]["UnblendedCost"]):
                v = m["UnblendedCost"]
                if v <= 0.005:
                    continue
                n = k.lower()
                flag = any(h in n for h in STANDBY_HINTS)
                if flag:
                    found += 1
                    r.warn("   %-46s %s MTD  (standby-class)" % (k[:46], money(v)))
                else:
                    r.log("   %-46s %s MTD" % (k[:46], money(v)))
                r.kv(section="H_sep_services", service=k, usd_mtd=round(v, 2),
                     standby=flag)
        if not found:
            r.ok("no EC2/ELB/OpenSearch/SageMaker/AppRunner-class lines in September MTD")
    except Exception as e:
        r.warn("September service scan: %s" % str(e)[:140])

    # ================================================================ I
    r.section("I. VERDICT")
    lam_aug_share = OUT.get("aug_lambda_ce", 0.0)
    s3_aug = sum(v for k, v in aug_by_svc.items() if svc_bucket(k) == "S3")
    cw_aug = sum(v for k, v in aug_by_svc.items() if svc_bucket(k) == "CloudWatch")
    r.log("August CE total ............ %s (invoice $484.26)" % money(aug_total))
    r.log("  S3 ....................... %s" % money(s3_aug))
    r.log("  Lambda ................... %s" % money(lam_aug_share))
    r.log("  CloudWatch ............... %s" % money(cw_aug))
    r.log("  everything else .......... %s" % money(aug_total - s3_aug - lam_aug_share - cw_aug))
    r.log("Churn-window excess (Aug 09-29) above the Aug 01-08 baseline: S3 %s / all %s"
          % (money(anomaly_excess_s3), money(anomaly_excess_all)))
    r.log("Baseline run-rate before the anomaly: %s/day = %s/month"
          % (money(baseline_all), money(baseline_all * 30)))
    r.log("September MTD run-rate: %s/day = %s/month projected"
          % (money(OUT.get("sep_mtd_daily_avg", 0)), money(OUT.get("sep_projection", 0))))
    if aug_rank:
        r.log("top Lambda by August cost: " + ", ".join(
            "%s %s" % (k[:32], money(v["usd"])) for k, v in aug_rank[:5]))
    OUT["verdict"] = {
        "aug_total": round(aug_total, 2), "aug_s3": round(s3_aug, 2),
        "aug_lambda": round(lam_aug_share, 2), "aug_cw": round(cw_aug, 2),
        "anomaly_excess_s3": round(anomaly_excess_s3, 2),
        "anomaly_excess_all": round(anomaly_excess_all, 2),
        "baseline_month": round(baseline_all * 30, 2),
        "sep_projection": OUT.get("sep_projection", 0)}

    rp = ROOT / "aws" / "ops" / "reports" / "5163_aug_bill_forensics.json"
    rp.write_text(json.dumps(OUT, indent=1, default=str), encoding="utf-8")
    r.ok("wrote %s" % rp.name)
    if FAILS:
        for f in FAILS:
            r.fail(f)
        sys.exit(1)
    r.ok("ops 5163 complete -- read-only, nothing mutated")
