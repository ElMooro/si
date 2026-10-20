"""ops 6557 - READ-ONLY data pull for the per-engine dollar-cost report (Khalid, 2026-10-20: "dollar cost per engine").

Collects what ops 6546 did not: CloudWatch Logs ingestion per log group (the CloudWatch line was ~$8/day normal, the
second-largest after S3), CloudWatch API call counts per operation (AWS/Usage), custom metric + alarm inventories,
Cost Explorer daily usage types for the normal window (Sep 15 - Oct 11) and the frozen window (Oct 17 - 19), DynamoDB
table sizes and Lambda code size.  Written gzip+base64 next to the report and to the private bucket.
Cost of this run: ~10 Cost Explorer calls ($0.10) + ~3,000 metric reads (~$0.03).
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import base64
import concurrent.futures as cf
import gzip
import json
import os
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION, ACCOUNT = "us-east-1", "857687956942"
PRIVATE = "justhodl-ops-private-857687956942"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=120, max_pool_connections=20)
NORMAL = ("2026-09-15", "2026-10-12")      # CE End is exclusive: 27 days
FROZEN = ("2026-10-17", "2026-10-20")
OUT = {"generated_at": datetime.now(timezone.utc).isoformat(), "errors": []}


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


def pages(client, op, key, **kw):
    out = []
    for p in client.get_paginator(op).paginate(**kw):
        out += p.get(key, [])
    return out


def ce_usage(log):
    ce = c("ce")
    rows = []
    for a, b in (NORMAL, FROZEN):
        token = None
        while True:
            kw = {"TimePeriod": {"Start": a, "End": b}, "Granularity": "DAILY", "Metrics": ["UnblendedCost", "UsageQuantity"],
                  "GroupBy": [{"Type": "DIMENSION", "Key": "SERVICE"}, {"Type": "DIMENSION", "Key": "USAGE_TYPE"}]}
            if token:
                kw["NextPageToken"] = token
            r = ce.get_cost_and_usage(**kw)
            for t in r["ResultsByTime"]:
                for g in t.get("Groups", []):
                    usd = float(g["Metrics"]["UnblendedCost"]["Amount"])
                    qty = float(g["Metrics"]["UsageQuantity"]["Amount"])
                    if usd or qty:
                        rows.append([t["TimePeriod"]["Start"], g["Keys"][0], g["Keys"][1], round(usd, 5), round(qty, 3)])
            token = r.get("NextPageToken")
            if not token:
                break
    OUT["ce_usage_daily"] = rows
    log("cost explorer: %d (day, service, usage type) rows" % len(rows))


def metric_sum(cw, queries, start, end):
    vals = {}
    for i in range(0, len(queries), 500):
        token = None
        while True:
            kw = {"MetricDataQueries": queries[i:i + 500], "StartTime": start, "EndTime": end}
            if token:
                kw["NextToken"] = token
            r = cw.get_metric_data(**kw)
            for res in r["MetricDataResults"]:
                vals[res["Id"]] = vals.get(res["Id"], 0.0) + sum(res.get("Values") or [])
            token = r.get("NextToken")
            if not token:
                break
    return vals


def logs_ingestion(log):
    logs, cw = c("logs"), c("cloudwatch")
    groups = pages(logs, "describe_log_groups", "logGroups")
    OUT["log_groups"] = [{"name": g["logGroupName"], "stored_bytes": g.get("storedBytes", 0), "retention_days": g.get("retentionInDays"),
                          "created": g.get("creationTime")} for g in groups]
    start = datetime(2026, 9, 15, tzinfo=timezone.utc)
    end = datetime(2026, 10, 12, tzinfo=timezone.utc)
    period = int((end - start).total_seconds())
    q = [{"Id": "g%d" % i, "ReturnData": True, "MetricStat": {"Metric": {"Namespace": "AWS/Logs", "MetricName": "IncomingBytes",
          "Dimensions": [{"Name": "LogGroupName", "Value": g["logGroupName"]}]}, "Period": period, "Stat": "Sum"}}
         for i, g in enumerate(groups)]
    vals = metric_sum(cw, q, start, end)
    OUT["log_ingest_bytes_normal"] = {g["logGroupName"]: vals.get("g%d" % i, 0.0) for i, g in enumerate(groups)}
    fstart, fend = datetime(2026, 10, 17, tzinfo=timezone.utc), datetime(2026, 10, 20, tzinfo=timezone.utc)
    fq = [dict(x, MetricStat=dict(x["MetricStat"], Period=int((fend - fstart).total_seconds()))) for x in q]
    fv = metric_sum(cw, fq, fstart, fend)
    OUT["log_ingest_bytes_frozen"] = {g["logGroupName"]: fv.get("g%d" % i, 0.0) for i, g in enumerate(groups) if fv.get("g%d" % i)}
    tot = sum(OUT["log_ingest_bytes_normal"].values()) / 27 / 1e9
    log("logs: %d groups, %.2f GB/day ingested in the normal window (%.1f GB stored total)" % (
        len(groups), tot, sum(g.get("storedBytes", 0) for g in groups) / 1e9))


def cw_usage(log):
    cw = c("cloudwatch")
    metrics = pages(cw, "list_metrics", "Metrics", Namespace="AWS/Usage")
    keep = [m for m in metrics if m["MetricName"] == "CallCount"]
    start = datetime(2026, 9, 15, tzinfo=timezone.utc)
    end = datetime(2026, 10, 12, tzinfo=timezone.utc)
    q = [{"Id": "u%d" % i, "ReturnData": True, "MetricStat": {"Metric": m, "Period": int((end - start).total_seconds()), "Stat": "Sum"}}
         for i, m in enumerate(keep)]
    vals = metric_sum(cw, q, start, end)
    OUT["api_calls_normal"] = [[{d["Name"]: d["Value"] for d in m["Dimensions"]}, vals.get("u%d" % i, 0.0)] for i, m in enumerate(keep)]
    log("AWS/Usage: %d CallCount series" % len(keep))
    # custom metrics inventory: count per namespace (MetricMonitorUsage is $0.30 per metric-month)
    ns = {}
    token = None
    while True:
        r = cw.list_metrics(**({"NextToken": token} if token else {}))
        for m in r.get("Metrics", []):
            ns[m["Namespace"]] = ns.get(m["Namespace"], 0) + 1
        token = r.get("NextToken")
        if not token:
            break
    OUT["metrics_per_namespace"] = ns
    alarms = pages(cw, "describe_alarms", "MetricAlarms")
    OUT["alarms"] = [{"name": a["AlarmName"], "namespace": a.get("Namespace"), "metric": a.get("MetricName"),
                      "dims": {d["Name"]: d["Value"] for d in a.get("Dimensions", [])}, "state": a.get("StateValue"),
                      "actions": len(a.get("AlarmActions") or [])} for a in alarms]
    log("metrics: %d namespaces, %d metrics total; %d alarms" % (len(ns), sum(ns.values()), len(alarms)))


def dynamo(log):
    d = c("dynamodb")
    out = []
    for name in pages(d, "list_tables", "TableNames"):
        t = d.describe_table(TableName=name)["Table"]
        out.append({"name": name, "bytes": t.get("TableSizeBytes"), "items": t.get("ItemCount"),
                    "billing": (t.get("BillingModeSummary") or {}).get("BillingMode", "PROVISIONED"),
                    "rcu": (t.get("ProvisionedThroughput") or {}).get("ReadCapacityUnits"),
                    "wcu": (t.get("ProvisionedThroughput") or {}).get("WriteCapacityUnits")})
    OUT["dynamodb"] = out
    log("dynamodb: %d tables" % len(out))


def lambda_sizes(log):
    lam = c("lambda")
    fns = pages(lam, "list_functions", "Functions")
    OUT["lambda_sizes"] = {f["FunctionName"]: {"code_bytes": f.get("CodeSize"), "mem": f.get("MemorySize"),
                                               "runtime": f.get("Runtime"), "pkg": f.get("PackageType"),
                                               "last_modified": f.get("LastModified")} for f in fns}
    log("lambda: %d functions, %.1f GB of code" % (len(fns), sum(f.get("CodeSize", 0) for f in fns) / 1e9))


def main():
    with report("ops_6557_cost_attribution_data") as r:
        r.heading("ops 6557 - per-engine cost attribution data (read-only)")
        for step in (ce_usage, logs_ingestion, cw_usage, dynamo, lambda_sizes):
            try:
                step(r.log)
            except Exception as e:  # noqa: BLE001 - every step is independent
                OUT["errors"].append("%s: %s" % (step.__name__, str(e)[:200]))
                r.warn("%s failed: %s" % (step.__name__, type(e).__name__))
        blob = gzip.compress(json.dumps(OUT, separators=(",", ":"), default=str).encode(), 9)
        c("s3").put_object(Bucket=PRIVATE, Key="audit/cost-attribution-%s.json.gz" % datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ"),
                           Body=blob, ServerSideEncryption="AES256")
        out = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd())) / "aws" / "ops" / "reports" / "latest" / "ops_6557_cost_attribution_data_data.md"
        b64 = base64.b64encode(blob).decode()
        out.write_text("# ops 6557 cost attribution data (gzip+base64 JSON)\n\n```\n" +
                       "\n".join(b64[i:i + 120] for i in range(0, len(b64), 120)) + "\n```\n", encoding="utf-8")
        r.log("data: %d KB compressed -> %s" % (len(blob) // 1024, out.name))
        r.ok("done (%d errors)" % len(OUT["errors"]))


if __name__ == "__main__":
    main()
