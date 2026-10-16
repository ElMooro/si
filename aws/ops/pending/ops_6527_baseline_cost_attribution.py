"""ops 6527 - READ-ONLY attribution of the pre-loop baseline (~$24/day) so the cuts are aimed by data.

Khalid (2026-10-16): yes, cut the baseline so the platform runs the whole month under the $500 cap.

Window = the "normal" days before the loop: 2026-09-20 .. 2026-10-11 (complete UTC days).
  1. Cost Explorer: daily average by SERVICE x USAGE_TYPE, and by OPERATION for CloudWatch and S3
     (which API is billed: GetMetricStatistics vs ListMetrics vs PutMetricData; GetObject vs PutObject ...).
  2. CloudTrail management events (free, 90 days): who calls Cost Explorer (event source ce.amazonaws.com).
     CloudWatch metric reads are data events (not logged by default) - attributed by code + invocations instead.
  3. Per-function invocations per day over the window (one GetMetricData pass, ~$0.01) for the call-rate
     arithmetic of the polling engines found in the code.
  4. S3 server access logs for one normal hour: GET bytes (egress) and request counts by requester, client,
     IP prefix and key prefix; PUT/LIST counts by writer session.
Cost of this op: ~6 Cost Explorer requests ($0.06) + metric reads (<$0.02) + a few thousand small log GETs.
"""

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import concurrent.futures as cf
import json
import re
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
START, END = date(2026, 9, 20), date(2026, 10, 12)  # END exclusive -> through Oct 11
NDAYS = (END - START).days
SAMPLE_HOUR = datetime(2026, 10, 10, 15, 0, tzinfo=timezone.utc)
OUT_JSON = ROOT / "aws" / "ops" / "reports" / "6527.json"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, max_pool_connections=48)
ce = boto3.client("ce", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
ct = boto3.client("cloudtrail", region_name=REGION, config=CFG)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
out = {"op": 6527, "ran_at": datetime.now(timezone.utc).isoformat(), "window": [START.isoformat(), END.isoformat()],
       "errors": {}}


def guard(name):
    def deco(fn):
        def run(*a, **k):
            try:
                return fn(*a, **k)
            except Exception as e:  # noqa: BLE001
                out["errors"][name] = "%s: %s" % (type(e).__name__, str(e)[:300])
        return run
    return deco


def ce_grouped(group_by, flt=None):
    agg = defaultdict(float)
    token = None
    while True:
        kw = {"TimePeriod": {"Start": START.isoformat(), "End": END.isoformat()}, "Granularity": "MONTHLY",
              "Metrics": ["UnblendedCost", "UsageQuantity"], "GroupBy": group_by}
        if flt:
            kw["Filter"] = flt
        if token:
            kw["NextPageToken"] = token
        r = ce.get_cost_and_usage(**kw)
        for t in r["ResultsByTime"]:
            for g in t.get("Groups", []):
                agg[tuple(g["Keys"]) + ("usd",)] += float(g["Metrics"]["UnblendedCost"]["Amount"])
                agg[tuple(g["Keys"]) + ("qty",)] += float(g["Metrics"]["UsageQuantity"]["Amount"])
        token = r.get("NextPageToken")
        if not token:
            return agg


@guard("ce_service_usage")
def ce_service_usage(r):
    agg = ce_grouped([{"Type": "DIMENSION", "Key": "SERVICE"}, {"Type": "DIMENSION", "Key": "USAGE_TYPE"}])
    rows = [{"service": k[0], "usage_type": k[1], "usd_day": round(v / NDAYS, 4),
             "qty_day": round(agg.get((k[0], k[1], "qty"), 0) / NDAYS, 2)}
            for k, v in agg.items() if k[2] == "usd" and v > 0]
    rows.sort(key=lambda x: -x["usd_day"])
    out["usage_per_day"] = rows
    out["total_per_day"] = round(sum(x["usd_day"] for x in rows), 2)
    r.log("normal-window total $%.2f/day over %d days" % (out["total_per_day"], NDAYS))
    for x in rows[:25]:
        r.log("  %-32s %-40s $%7.3f/day  qty %s" % (x["service"][:32], x["usage_type"][:40], x["usd_day"], x["qty_day"]))


@guard("ce_operations")
def ce_operations(r):
    for svc in ("AmazonCloudWatch", "Amazon Simple Storage Service", "AWS Lambda", "Amazon SageMaker"):
        agg = ce_grouped([{"Type": "DIMENSION", "Key": "OPERATION"}, {"Type": "DIMENSION", "Key": "USAGE_TYPE"}],
                         {"Dimensions": {"Key": "SERVICE", "Values": [svc]}})
        rows = [{"operation": k[0], "usage_type": k[1], "usd_day": round(v / NDAYS, 4),
                 "qty_day": round(agg.get((k[0], k[1], "qty"), 0) / NDAYS, 1)}
                for k, v in agg.items() if k[2] == "usd" and v > 0.0]
        rows.sort(key=lambda x: -x["usd_day"])
        out.setdefault("operations", {})[svc] = rows[:30]
        r.log("%s by operation:" % svc)
        for x in rows[:10]:
            r.log("    %-34s %-34s $%7.3f/day  qty/day %s" % (x["operation"][:34], x["usage_type"][:34], x["usd_day"], x["qty_day"]))


@guard("cloudtrail_ce")
def cloudtrail_ce(r):
    callers, names = Counter(), Counter()
    start = datetime.now(timezone.utc) - timedelta(days=14)
    token, pages = None, 0
    while pages < 120:
        kw = {"LookupAttributes": [{"AttributeKey": "EventSource", "AttributeValue": "ce.amazonaws.com"}],
              "StartTime": start, "MaxResults": 50}
        if token:
            kw["NextToken"] = token
        resp = ct.lookup_events(**kw)
        pages += 1
        for e in resp.get("Events", []):
            names[e.get("EventName")] += 1
            try:
                ui = json.loads(e["CloudTrailEvent"]).get("userIdentity", {})
                arn = ui.get("arn", "?")
                who = arn.split("/")[-1] if "assumed-role" in arn else arn.split(":")[-1]
            except Exception:  # noqa: BLE001
                who = e.get("Username") or "?"
            callers[who] += 1
        token = resp.get("NextToken")
        if not token:
            break
        time.sleep(0.55)  # LookupEvents allows 2 requests/second
    out["ce_callers_14d"] = callers.most_common(20)
    out["ce_events_14d"] = names.most_common(10)
    out["ce_lookup_pages"] = pages
    r.log("Cost Explorer callers, last 14 days (%d pages%s): %s" % (pages, ", truncated" if token else "", callers.most_common(8)))
    r.log("Cost Explorer events: %s" % names.most_common(6))


@guard("invocations")
def invocations(r):
    names = [f["FunctionName"] for p in lam.get_paginator("list_functions").paginate() for f in p["Functions"]]
    start = datetime(START.year, START.month, START.day, tzinfo=timezone.utc)
    end = datetime(END.year, END.month, END.day, tzinfo=timezone.utc)
    q = [{"Id": "i%d" % i, "ReturnData": True, "MetricStat": {
        "Metric": {"Namespace": "AWS/Lambda", "MetricName": "Invocations",
                   "Dimensions": [{"Name": "FunctionName", "Value": n}]},
        "Period": int((end - start).total_seconds()), "Stat": "Sum"}} for i, n in enumerate(names)]
    vals = {}
    for i in range(0, len(q), 500):
        token = None
        while True:
            kw = {"MetricDataQueries": q[i:i + 500], "StartTime": start, "EndTime": end}
            if token:
                kw["NextToken"] = token
            resp = cw.get_metric_data(**kw)
            for res in resp["MetricDataResults"]:
                vals[res["Id"]] = vals.get(res["Id"], 0.0) + sum(res.get("Values") or [])
            token = resp.get("NextToken")
            if not token:
                break
    per_day = {names[int(k[1:])]: round(v / NDAYS, 1) for k, v in vals.items()}
    top = sorted(per_day.items(), key=lambda kv: -kv[1])
    out["invocations_per_day"] = dict(top)
    out["fleet_invocations_per_day"] = round(sum(per_day.values()), 0)
    r.log("fleet invocations/day %.0f; top: %s" % (out["fleet_invocations_per_day"], top[:12]))
    watch = ("justhodl-fleet-error-monitor", "justhodl-ai", "justhodl-health-monitor", "justhodl-event-flow-monitor",
             "justhodl-import-sentinel", "justhodl-cost-anomaly", "justhodl-fleet-integrity", "justhodl-fleet-monitor",
             "justhodl-fleet-freshness-monitor", "justhodl-feed-heartbeat")
    out["pollers_per_day"] = {w: per_day.get(w) for w in watch}
    r.log("pollers invocations/day: %s" % out["pollers_per_day"])


LOG_RE = re.compile(r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" "([^"]*)"')


@guard("access_logs")
def access_logs(r):
    lg = s3.get_bucket_logging(Bucket=BUCKET).get("LoggingEnabled") or {}
    out["bucket_logging"] = {"target": lg.get("TargetBucket"), "prefix": lg.get("TargetPrefix"),
                             "format": lg.get("TargetObjectKeyFormat")}
    r.log("bucket logging: %s" % out["bucket_logging"])
    if not lg:
        return
    tb, tp = lg["TargetBucket"], lg.get("TargetPrefix") or ""
    fmt = lg.get("TargetObjectKeyFormat") or {}
    if "PartitionedPrefix" in fmt:
        pfx = "%s%s/%s/%s/%s/%s-" % (tp, ACCOUNT, REGION, BUCKET, SAMPLE_HOUR.strftime("%Y/%m/%d"), SAMPLE_HOUR.strftime("%Y-%m-%d-%H"))
    else:
        pfx = "%s%s" % (tp, SAMPLE_HOUR.strftime("%Y-%m-%d-%H"))
    keys, token = [], None
    while len(keys) < 12000:
        kw = {"Bucket": tb, "Prefix": pfx, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        keys += [o["Key"] for o in resp.get("Contents", [])]
        if not resp.get("IsTruncated"):
            break
        token = resp.get("NextContinuationToken")
    out["log_sample"] = {"prefix": pfx, "objects": len(keys)}
    r.log("log objects for %s: %d (prefix %s)" % (SAMPLE_HOUR.isoformat(), len(keys), pfx))
    if not keys:
        probe = s3.list_objects_v2(Bucket=tb, Prefix=tp, MaxKeys=5).get("Contents", [])
        out["log_probe"] = [(o["Key"], o["LastModified"].isoformat()) for o in probe]
        r.warn("no logs for the sample hour; first keys under the prefix: %s" % out["log_probe"])
        return

    def read(k):
        try:
            return s3.get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
        except Exception:  # noqa: BLE001
            return ""

    gets_bytes, gets_n, ops, writers, ua_bytes, ip_bytes = Counter(), Counter(), Counter(), Counter(), Counter(), Counter()
    req_bytes = Counter()
    with cf.ThreadPoolExecutor(max_workers=48) as ex:
        for text in ex.map(read, keys):
            for line in text.splitlines():
                m = LOG_RE.match(line)
                if not m:
                    continue
                ip, requester, op, key = m.group(4), m.group(5), m.group(7), m.group(8)
                sent = m.group(12)
                ua = m.group(17)
                ops[op] += 1
                who = requester.split("/")[-1] if "assumed-role" in requester else ("anonymous" if requester == "-" else requester.split(":")[-1])
                b = int(sent) if sent.isdigit() else 0
                pfx2 = "/".join(key.split("/")[:2]) if key != "-" else "-"
                if op == "REST.GET.OBJECT":
                    gets_bytes[pfx2] += b
                    gets_n[pfx2] += 1
                    req_bytes[who] += b
                    family = ua.split("/")[0][:40] if ua and ua != "-" else "-"
                    ua_bytes[family] += b
                    ip_bytes[".".join(ip.split(".")[:2]) + ".*"] += b
                elif op.startswith("REST.PUT") or op.startswith("REST.POST") or op == "REST.GET.BUCKET" or op.startswith("REST.COPY"):
                    writers[(who, op)] += 1
    gb = lambda c: [(k, round(v / 1e9, 3)) for k, v in c.most_common(12)]  # noqa: E731
    out["log_hour"] = {"ops": ops.most_common(15), "get_gb_by_prefix": gb(gets_bytes), "get_n_by_prefix": gets_n.most_common(12),
                       "get_gb_by_requester": gb(req_bytes), "get_gb_by_client": gb(ua_bytes), "get_gb_by_ip": gb(ip_bytes),
                       "tier1_by_writer": [("%s %s" % k, v) for k, v in writers.most_common(15)]}
    r.log("ops in the hour: %s" % ops.most_common(8))
    r.log("GET GB by requester: %s" % gb(req_bytes)[:6])
    r.log("GET GB by client: %s" % gb(ua_bytes)[:6])
    r.log("GET GB by key prefix: %s" % gb(gets_bytes)[:8])
    r.log("Tier-1 (PUT/LIST/COPY) by writer: %s" % writers.most_common(8))


def main():
    with report("ops_6527_baseline_cost_attribution") as r:
        r.heading("ops 6527 - attribute the pre-loop baseline (read-only)")
        r.section("Cost Explorer")
        ce_service_usage(r)
        ce_operations(r)
        r.section("CloudTrail - Cost Explorer callers")
        cloudtrail_ce(r)
        r.section("Invocations")
        invocations(r)
        r.section("S3 access logs")
        access_logs(r)
        if out["errors"]:
            r.section("Section errors")
            for k, v in out["errors"].items():
                r.warn("%s: %s" % (k, v))
        OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
        r.ok("wrote %s" % OUT_JSON.relative_to(ROOT))
        if "ce_service_usage" in out["errors"]:
            r.fail("core Cost Explorer section failed")
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
