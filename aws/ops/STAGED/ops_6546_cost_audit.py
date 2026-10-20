"""ops 6546 - deep cost audit, READ-ONLY (Khalid, 2026-10-20).

"I accrued a $3000 charge ... I want every single [engine] to run once every 24 hours ... a lot of [lambdas] must be
duplicate or just wasting money ... never goes past $600 ... give me dollar cost per engine".

STAGED (direct lane, no serial twin).  Mutates nothing.  Collects everything the cost plan needs:

  1. Cost Explorer: monthly by service (Jun..Oct), daily total and daily by service Sep 1..yesterday, usage types
     over the normal window, cost by region, Lambda resource-level cost if the account has it enabled
  2. every Lambda: config (memory, timeout, arch, code hash, runtime, package), resource policy principals,
     async destinations, and DAILY CloudWatch Invocations / Duration / Errors / Throttles Sep 1..yesterday
  3. CloudWatch Logs: IncomingBytes per log group per day, stored bytes and retention of every group
  4. every trigger: EventBridge rules (all buses) + targets, Scheduler schedules (all groups) with expression and
     input, event source mappings, S3 bucket notifications, SNS lambda subscriptions, API Gateway, Step Functions
  5. everything that is not Lambda: SageMaker, ECR, DynamoDB, EC2/EBS/NAT/EIP/snapshots in EVERY region, Lambda
     counts in other regions, S3 bucket sizes/versioning, alarms, budgets, the spend governor's state + control
  6. S3 request attribution per engine: a sample of the server access logs for a normal day (Oct 10) and a frozen
     day (Oct 18), requests per requester session (for Lambda the session IS the function name) and operation class

Outputs: aws/ops/reports/latest/ops_6546_cost_audit.md (summary) and ops_6546_cost_audit_data.md (gzip+base64 JSON
of everything above, decoded by Claude in the sandbox) + s3://justhodl-ops-private-857687956942/audit/ops6546.json.gz.
Cost of this op: ~25 Cost Explorer requests ($0.25), ~5k CloudWatch metrics ($0.05), ~10k S3 GETs (<$0.01).
"""

from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import base64
import concurrent.futures as cf
import gzip
import json
import os
import re
import sys
import time

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
PRIVATE = "justhodl-ops-private-857687956942"
CFG = Config(retries={"max_attempts": 10, "mode": "standard"}, connect_timeout=10, read_timeout=60,
             max_pool_connections=40)
_cl = {}
NOW = datetime.now(timezone.utc)
TODAY = NOW.date()
START = date(2026, 9, 1)
D = {"generated_at": NOW.isoformat(), "errors": []}


def c(name, region=REGION):
    k = (name, region)
    if k not in _cl:
        _cl[k] = boto3.client(name, region_name=region, config=CFG)
    return _cl[k]


def err(e):
    if isinstance(e, ClientError):
        x = e.response.get("Error", {})
        return "%s: %s" % (x.get("Code"), str(x.get("Message") or "")[:200])
    return "%s: %s" % (type(e).__name__, str(e)[:200])


def safe(label, fn, default=None):
    try:
        return fn()
    except Exception as e:  # noqa: BLE001 - an audit records what it could not read and keeps going
        D["errors"].append("%s: %s" % (label, err(e)))
        print("[audit] %s failed: %s" % (label, err(e)), flush=True)
        return default


def pages(client, op, key, **kw):
    out = []
    for p in client.get_paginator(op).paginate(**kw):
        out += p.get(key, [])
    return out


# ------------------------------------------------------------------------------------------- 1. Cost Explorer --
def ce_query(start, end, gran, group=None, filt=None, metric="UnblendedCost"):
    rows, token = [], None
    while True:
        kw = {"TimePeriod": {"Start": str(start), "End": str(end)}, "Granularity": gran, "Metrics": [metric]}
        if group:
            kw["GroupBy"] = [{"Type": "DIMENSION", "Key": g} for g in group]
        if filt:
            kw["Filter"] = filt
        if token:
            kw["NextPageToken"] = token
        resp = c("ce").get_cost_and_usage(**kw)
        for t in resp["ResultsByTime"]:
            day = t["TimePeriod"]["Start"]
            if group:
                for g in t.get("Groups", []):
                    rows.append([day] + g["Keys"] + [round(float(g["Metrics"][metric]["Amount"]), 4)])
            else:
                rows.append([day, round(float(t["Total"][metric]["Amount"]), 4)])
        token = resp.get("NextPageToken")
        if not token:
            return rows


def cost_explorer(r):
    r.section("1. Billing (Cost Explorer)")
    D["ce_monthly_service"] = safe("ce monthly", lambda: ce_query(date(2026, 6, 1), TODAY, "MONTHLY", ["SERVICE"]), [])
    D["ce_daily_total"] = safe("ce daily", lambda: ce_query(START, TODAY, "DAILY"), [])
    D["ce_daily_service"] = safe("ce daily svc", lambda: ce_query(START, TODAY, "DAILY", ["SERVICE"]), [])
    D["ce_usage_normal"] = safe("ce usage normal", lambda: ce_query(date(2026, 9, 29), date(2026, 10, 12), "MONTHLY",
                                                                     ["SERVICE", "USAGE_TYPE"]), [])
    D["ce_usage_frozen"] = safe("ce usage frozen", lambda: ce_query(date(2026, 10, 17), TODAY, "MONTHLY",
                                                                     ["SERVICE", "USAGE_TYPE"]), [])
    D["ce_region"] = safe("ce region", lambda: ce_query(date(2026, 9, 1), TODAY, "MONTHLY", ["REGION"]), [])
    D["ce_record_type"] = safe("ce record", lambda: ce_query(date(2026, 8, 1), TODAY, "MONTHLY", ["RECORD_TYPE"]), [])
    # resource-level Lambda cost exists only if the account opted in (last 14 days); recorded either way
    def res():
        out, token = [], None
        while True:
            kw = {"TimePeriod": {"Start": str(TODAY - timedelta(days=13)), "End": str(TODAY)}, "Granularity": "DAILY",
                  "Metrics": ["UnblendedCost"], "GroupBy": [{"Type": "DIMENSION", "Key": "RESOURCE_ID"}],
                  "Filter": {"Dimensions": {"Key": "SERVICE", "Values": ["AWS Lambda"]}}}
            if token:
                kw["NextPageToken"] = token
            resp = c("ce").get_cost_and_usage_with_resources(**kw)
            for t in resp["ResultsByTime"]:
                for g in t.get("Groups", []):
                    out.append([t["TimePeriod"]["Start"], g["Keys"][0], round(float(g["Metrics"]["UnblendedCost"]["Amount"]), 5)])
            token = resp.get("NextPageToken")
            if not token:
                return out
    D["ce_lambda_resource"] = safe("ce lambda resources (opt-in)", res, None)
    months = defaultdict(float)
    for row in D["ce_monthly_service"]:
        months[row[0]] += row[-1]
    r.log("monthly totals: %s" % {k: round(v, 2) for k, v in sorted(months.items())})
    last = sorted(D["ce_daily_total"])[-12:]
    r.log("last 12 days: %s" % [(d[5:], round(v, 2)) for d, v in last])
    r.log("lambda resource-level cost: %s" % ("available, %d rows" % len(D["ce_lambda_resource"])
                                              if D["ce_lambda_resource"] else "not enabled"))


# ---------------------------------------------------------------------------------------------- 2. Lambda --
def lambdas(r):
    r.section("2. Lambda inventory + per-function metrics")
    fns = pages(c("lambda"), "list_functions", "Functions")
    inv = {}
    for f in fns:
        inv[f["FunctionName"]] = {
            "mem": f.get("MemorySize"), "timeout": f.get("Timeout"), "arch": (f.get("Architectures") or ["x86_64"])[0],
            "runtime": f.get("Runtime"), "pkg": f.get("PackageType"), "sha": f.get("CodeSha256"),
            "size": f.get("CodeSize"), "modified": f.get("LastModified"), "role": (f.get("Role") or "").split("/")[-1],
            "desc": (f.get("Description") or "")[:160], "eph": (f.get("EphemeralStorage") or {}).get("Size"),
            "layers": len(f.get("Layers") or []), "snap": (f.get("SnapStart") or {}).get("ApplyOn"),
            "env_keys": sorted((f.get("Environment") or {}).get("Variables", {}).keys())[:40],
        }
    r.log("%d functions" % len(inv))

    def extra(fn):
        out = {}
        try:
            pol = json.loads(c("lambda").get_policy(FunctionName=fn)["Policy"])
            pr = []
            for st in pol.get("Statement", []):
                p = st.get("Principal")
                p = p.get("Service") or p.get("AWS") if isinstance(p, dict) else p
                src = ((st.get("Condition") or {}).get("ArnLike") or {}).get("AWS:SourceArn") or \
                      ((st.get("Condition") or {}).get("ArnEquals") or {}).get("AWS:SourceArn")
                url = ((st.get("Condition") or {}).get("StringEquals") or {}).get("lambda:FunctionUrlAuthType")
                pr.append([str(p), str(src or "")[-120:], url or ""])
            out["policy"] = pr
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                out["policy_err"] = err(e)
        try:
            out["url"] = [u.get("AuthType") for u in c("lambda").list_function_url_configs(FunctionName=fn).get("FunctionUrlConfigs", [])]
        except ClientError:
            pass
        try:
            cfg = c("lambda").get_function_event_invoke_config(FunctionName=fn)
            dest = cfg.get("DestinationConfig") or {}
            out["dest"] = {k: (v or {}).get("Destination") for k, v in dest.items() if (v or {}).get("Destination")}
            out["retries"] = cfg.get("MaximumRetryAttempts")
        except ClientError:
            pass
        try:
            rc = c("lambda").get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
            out["reserved_now"] = rc
        except ClientError:
            pass
        return fn, out

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        for fn, out in ex.map(extra, list(inv)):
            inv[fn].update(out)
    D["lambda"] = inv

    # daily metrics Sep 1 .. today (the frozen days show what still runs)
    metrics = ["Invocations", "Duration", "Errors", "Throttles"]
    queries = []
    names = sorted(inv)
    for i, fn in enumerate(names):
        for j, m in enumerate(metrics):
            queries.append({"Id": "m%d_%d" % (i, j), "ReturnData": True, "MetricStat": {
                "Metric": {"Namespace": "AWS/Lambda", "MetricName": m,
                           "Dimensions": [{"Name": "FunctionName", "Value": fn}]},
                "Period": 86400, "Stat": "Sum"}})
    series = defaultdict(dict)
    t0 = datetime.combine(START, datetime.min.time(), tzinfo=timezone.utc)
    t1 = datetime.combine(TODAY + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    for k in range(0, len(queries), 500):
        chunk, token = queries[k:k + 500], None
        while True:
            kw = {"MetricDataQueries": chunk, "StartTime": t0, "EndTime": t1, "ScanBy": "TimestampAscending"}
            if token:
                kw["NextToken"] = token
            resp = c("cloudwatch").get_metric_data(**kw)
            for res in resp["MetricDataResults"]:
                i, j = (int(x) for x in res["Id"][1:].split("_"))
                key = "%s|%s" % (names[i], metrics[j])
                for ts, v in zip(res["Timestamps"], res["Values"]):
                    series[key][ts.strftime("%Y-%m-%d")] = series[key].get(ts.strftime("%Y-%m-%d"), 0) + v
            token = resp.get("NextToken")
            if not token:
                break
    D["lambda_daily"] = {k: {d: round(v, 1) for d, v in sorted(s.items())} for k, s in series.items()}
    r.log("daily metric series: %d (non-empty)" % len(D["lambda_daily"]))

    # account-level hourly invocations, last 3 days (what runs while frozen)
    def acct():
        resp = c("cloudwatch").get_metric_data(MetricDataQueries=[{"Id": "a", "ReturnData": True, "MetricStat": {
            "Metric": {"Namespace": "AWS/Lambda", "MetricName": "Invocations"}, "Period": 3600, "Stat": "Sum"}}],
            StartTime=NOW - timedelta(days=3), EndTime=NOW, ScanBy="TimestampAscending")
        res = resp["MetricDataResults"][0]
        return [[t.strftime("%m-%dT%H"), v] for t, v in zip(res["Timestamps"], res["Values"])]
    D["acct_invocations_hourly_3d"] = safe("acct invocations", acct, [])


# ------------------------------------------------------------------------------------------------ 3. Logs --
def logs(r):
    r.section("3. CloudWatch Logs")
    groups = pages(c("logs"), "describe_log_groups", "logGroups")
    D["log_groups"] = {g["logGroupName"]: [g.get("storedBytes", 0), g.get("retentionInDays"), g.get("logGroupClass")]
                       for g in groups}
    r.log("%d log groups, stored %.1f GB, never-expire %d" % (
        len(groups), sum(g.get("storedBytes", 0) for g in groups) / 1e9,
        sum(1 for g in groups if not g.get("retentionInDays"))))
    names = sorted(D["log_groups"])
    q = [{"Id": "l%d" % i, "ReturnData": True, "MetricStat": {
        "Metric": {"Namespace": "AWS/Logs", "MetricName": "IncomingBytes",
                   "Dimensions": [{"Name": "LogGroupName", "Value": n}]}, "Period": 86400, "Stat": "Sum"}}
         for i, n in enumerate(names)]
    out = {}
    t0 = datetime.combine(date(2026, 9, 29), datetime.min.time(), tzinfo=timezone.utc)
    t1 = datetime.combine(TODAY + timedelta(days=1), datetime.min.time(), tzinfo=timezone.utc)
    for k in range(0, len(q), 500):
        chunk, token = q[k:k + 500], None
        while True:
            kw = {"MetricDataQueries": chunk, "StartTime": t0, "EndTime": t1, "ScanBy": "TimestampAscending"}
            if token:
                kw["NextToken"] = token
            resp = c("cloudwatch").get_metric_data(**kw)
            for res in resp["MetricDataResults"]:
                if res["Values"]:
                    n = names[int(res["Id"][1:])]
                    d = out.setdefault(n, {})
                    for ts, v in zip(res["Timestamps"], res["Values"]):
                        d[ts.strftime("%Y-%m-%d")] = d.get(ts.strftime("%Y-%m-%d"), 0) + int(v)
            token = resp.get("NextToken")
            if not token:
                break
    D["log_incoming_daily"] = out
    r.log("log groups with ingestion since Sep 29: %d" % len(out))


# --------------------------------------------------------------------------------------------- 4. Triggers --
def triggers(r):
    r.section("4. Triggers")
    rules = []
    buses = safe("buses", lambda: [b["Name"] for b in pages(c("events"), "list_event_buses", "EventBuses")], ["default"])
    for bus in buses:
        for ru in safe("rules " + bus, lambda: pages(c("events"), "list_rules", "Rules", EventBusName=bus), []):
            tg = safe("targets " + ru["Name"], lambda: pages(c("events"), "list_targets_by_rule", "Targets",
                                                            Rule=ru["Name"], EventBusName=bus), [])
            rules.append({"bus": bus, "name": ru["Name"], "state": ru.get("State"), "sched": ru.get("ScheduleExpression"),
                          "pattern": (ru.get("EventPattern") or "")[:600], "managed": ru.get("ManagedBy"),
                          "targets": [[t.get("Arn"), (t.get("Input") or "")[:300], t.get("Id")] for t in tg]})
    D["rules"] = rules
    r.log("EventBridge rules: %d (%d enabled, %d scheduled)" % (
        len(rules), sum(1 for x in rules if x["state"] == "ENABLED"), sum(1 for x in rules if x["sched"])))

    groups = safe("schedule groups", lambda: [g["Name"] for g in pages(c("scheduler"), "list_schedule_groups",
                                                                          "ScheduleGroups")], ["default"])
    lst = []
    for g in groups:
        lst += safe("schedules " + g, lambda: pages(c("scheduler"), "list_schedules", "Schedules", GroupName=g), [])

    def one(s):
        for i in range(6):
            try:
                x = c("scheduler").get_schedule(Name=s["Name"], GroupName=s["GroupName"])
                t = x.get("Target") or {}
                return {"group": s["GroupName"], "name": s["Name"], "state": x.get("State"),
                        "expr": x.get("ScheduleExpression"), "tz": x.get("ScheduleExpressionTimezone"),
                        "arn": t.get("Arn"), "input": (t.get("Input") or "")[:300],
                        "end": str(x.get("EndDate") or ""), "after": x.get("ActionAfterCompletion"),
                        "flex": (x.get("FlexibleTimeWindow") or {}).get("Mode")}
            except ClientError as e:
                if e.response["Error"]["Code"] in ("ThrottlingException", "TooManyRequestsException"):
                    time.sleep(1 + i)
                    continue
                return {"group": s["GroupName"], "name": s["Name"], "err": err(e)}
        return {"group": s["GroupName"], "name": s["Name"], "err": "throttled"}

    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        D["schedules"] = list(ex.map(one, lst))
    r.log("Scheduler schedules: %d (%d enabled)" % (len(D["schedules"]),
                                                    sum(1 for x in D["schedules"] if x.get("state") == "ENABLED")))

    D["esm"] = [[m.get("FunctionArn", "").split(":")[-1], (m.get("EventSourceArn") or "")[-120:], m.get("State"),
                 m.get("BatchSize")] for m in safe("esm", lambda: pages(c("lambda"), "list_event_source_mappings",
                                                                        "EventSourceMappings"), [])]
    r.log("event source mappings: %d" % len(D["esm"]))

    notif = {}
    buckets = safe("buckets", lambda: c("s3").list_buckets()["Buckets"], [])
    D["buckets"] = {}
    for b in buckets:
        name = b["Name"]
        loc = safe("loc " + name, lambda: c("s3").get_bucket_location(Bucket=name).get("LocationConstraint") or "us-east-1",
                   "?")
        s3r = c("s3", loc if loc and loc != "?" else REGION)
        n = safe("notif " + name, lambda: s3r.get_bucket_notification_configuration(Bucket=name), {}) or {}
        n.pop("ResponseMetadata", None)
        if any(n.get(k) for k in ("LambdaFunctionConfigurations", "QueueConfigurations", "TopicConfigurations",
                                  "EventBridgeConfiguration")) or "EventBridgeConfiguration" in n:
            notif[name] = {"lambda": [[x.get("LambdaFunctionArn", "").split(":")[-1], x.get("Events"),
                                       json.dumps(x.get("Filter") or {})[:200]] for x in n.get("LambdaFunctionConfigurations", [])],
                           "sqs": len(n.get("QueueConfigurations", [])), "sns": len(n.get("TopicConfigurations", [])),
                           "eventbridge": "EventBridgeConfiguration" in n}
        ver = safe("ver " + name, lambda: s3r.get_bucket_versioning(Bucket=name).get("Status"), None)
        D["buckets"][name] = {"region": loc, "versioning": ver, "created": str(b.get("CreationDate"))[:10]}
    D["s3_notifications"] = notif
    r.log("buckets: %d; with event notifications: %s" % (len(buckets), sorted(notif)))

    D["sns_lambda"] = [[s.get("TopicArn", "").split(":")[-1], s.get("Endpoint", "").split(":")[-1]]
                       for s in safe("sns", lambda: pages(c("sns"), "list_subscriptions", "Subscriptions"), [])
                       if s.get("Protocol") == "lambda"]
    D["apigw_rest"] = [a.get("name") for a in safe("apigw", lambda: pages(c("apigateway"), "get_rest_apis", "items"), [])]
    D["apigw_http"] = [a.get("Name") for a in safe("apigwv2", lambda: c("apigatewayv2").get_apis().get("Items", []), [])]
    D["sfn"] = [s["name"] for s in safe("sfn", lambda: pages(c("stepfunctions"), "list_state_machines", "stateMachines"), [])]
    r.log("SNS->lambda %d, REST APIs %d, HTTP APIs %d, state machines %d" % (
        len(D["sns_lambda"]), len(D["apigw_rest"]), len(D["apigw_http"]), len(D["sfn"])))


# ---------------------------------------------------------------------------------------- 5. Everything else --
def other(r):
    r.section("5. Non-Lambda resources")
    sm = c("sagemaker")
    D["sagemaker"] = {
        "endpoints": [[e["EndpointName"], e["EndpointStatus"]] for e in safe("sm ep", lambda: pages(sm, "list_endpoints", "Endpoints"), [])],
        "notebooks": [[n["NotebookInstanceName"], n["NotebookInstanceStatus"]] for n in
                      safe("sm nb", lambda: pages(sm, "list_notebook_instances", "NotebookInstances"), [])],
        "models": len(safe("sm models", lambda: pages(sm, "list_models", "Models"), [])),
        "domains": [d.get("DomainName") for d in safe("sm domains", lambda: pages(sm, "list_domains", "Domains"), [])],
        "jobs_oct": len(safe("sm jobs", lambda: pages(sm, "list_training_jobs", "TrainingJobSummaries",
                                                      CreationTimeAfter=datetime(2026, 10, 1, tzinfo=timezone.utc)), [])),
    }
    ecr = []
    for repo in safe("ecr", lambda: pages(c("ecr"), "describe_repositories", "repositories"), []):
        imgs = safe("ecr imgs " + repo["repositoryName"], lambda: pages(c("ecr"), "describe_images", "imageDetails",
                                                                        repositoryName=repo["repositoryName"]), [])
        ecr.append([repo["repositoryName"], len(imgs), round(sum(i.get("imageSizeInBytes", 0) for i in imgs) / 1e9, 2),
                    bool(safe("ecr lc", lambda: c("ecr").get_lifecycle_policy(repositoryName=repo["repositoryName"]), None))])
    D["ecr"] = ecr
    ddb = []
    for t in safe("ddb", lambda: pages(c("dynamodb"), "list_tables", "TableNames"), []):
        x = safe("ddb " + t, lambda: c("dynamodb").describe_table(TableName=t)["Table"], {}) or {}
        pitr = safe("pitr " + t, lambda: c("dynamodb").describe_continuous_backups(TableName=t)["ContinuousBackupsDescription"]
                    ["PointInTimeRecoveryDescription"]["PointInTimeRecoveryStatus"], None)
        ddb.append([t, (x.get("BillingModeSummary") or {}).get("BillingMode", "PROVISIONED"),
                    round(x.get("TableSizeBytes", 0) / 1e9, 3), x.get("ItemCount"),
                    (x.get("ProvisionedThroughput") or {}).get("ReadCapacityUnits"),
                    (x.get("ProvisionedThroughput") or {}).get("WriteCapacityUnits"), pitr,
                    bool((x.get("StreamSpecification") or {}).get("StreamEnabled"))])
    D["dynamodb"] = ddb

    regions = safe("regions", lambda: [x["RegionName"] for x in c("ec2").describe_regions()["Regions"]], [REGION])
    reg = {}

    def scan(rg):
        e = boto3.client("ec2", region_name=rg, config=CFG)
        out = {}
        out["instances"] = [[i["InstanceId"], i["InstanceType"], i["State"]["Name"]]
                            for rv in safe("ec2 " + rg, lambda: pages(e, "describe_instances", "Reservations"), [])
                            for i in rv["Instances"]]
        out["volumes_gb"] = sum(v["Size"] for v in safe("vol " + rg, lambda: pages(e, "describe_volumes", "Volumes"), []))
        out["nat"] = [n["NatGatewayId"] for n in safe("nat " + rg, lambda: pages(e, "describe_nat_gateways", "NatGateways"), [])
                      if n.get("State") not in ("deleted", "deleting")]
        out["eips"] = len(safe("eip " + rg, lambda: e.describe_addresses()["Addresses"], []))
        out["snap_gb"] = sum(s.get("VolumeSize", 0) for s in safe("snap " + rg, lambda: pages(
            e, "describe_snapshots", "Snapshots", OwnerIds=["self"]), []))
        lam = boto3.client("lambda", region_name=rg, config=CFG)
        out["lambdas"] = len(safe("lambda " + rg, lambda: pages(lam, "list_functions", "Functions"), []))
        return rg, out

    with cf.ThreadPoolExecutor(max_workers=8) as ex:
        for rg, out in ex.map(scan, regions):
            if any(out.values()):
                reg[rg] = out
    D["regions"] = reg
    r.log("regions with anything: %s" % {k: {kk: (len(vv) if isinstance(vv, list) else vv) for kk, vv in v.items()}
                                         for k, v in reg.items()})

    # bucket sizes from the daily S3 storage metrics
    sizes = {}
    for name, meta in D["buckets"].items():
        cw = c("cloudwatch", meta["region"] if meta["region"] not in ("?", None) else REGION)
        for st in ("StandardStorage", "StandardIAStorage", "IntelligentTieringFAStorage", "GlacierInstantRetrievalStorage"):
            v = safe("size " + name, lambda: cw.get_metric_statistics(
                Namespace="AWS/S3", MetricName="BucketSizeBytes", StartTime=NOW - timedelta(days=3), EndTime=NOW,
                Period=86400, Statistics=["Average"],
                Dimensions=[{"Name": "BucketName", "Value": name}, {"Name": "StorageType", "Value": st}])["Datapoints"], [])
            if v:
                sizes.setdefault(name, {})[st] = round(sorted(v, key=lambda x: x["Timestamp"])[-1]["Average"] / 1e9, 1)
        n = safe("objs " + name, lambda: cw.get_metric_statistics(
            Namespace="AWS/S3", MetricName="NumberOfObjects", StartTime=NOW - timedelta(days=3), EndTime=NOW,
            Period=86400, Statistics=["Average"],
            Dimensions=[{"Name": "BucketName", "Value": name}, {"Name": "StorageType", "Value": "AllStorageTypes"}])["Datapoints"], [])
        if n:
            sizes.setdefault(name, {})["objects"] = int(sorted(n, key=lambda x: x["Timestamp"])[-1]["Average"])
    D["bucket_sizes_gb"] = sizes
    r.log("bucket sizes (GB): %s" % sizes)

    D["alarms"] = len(safe("alarms", lambda: pages(c("cloudwatch"), "describe_alarms", "MetricAlarms"), []))
    D["budgets"] = [[b["BudgetName"], b["BudgetLimit"]["Amount"], b.get("TimeUnit"),
                     (b.get("CalculatedSpend") or {}).get("ActualSpend", {}).get("Amount"),
                     (b.get("CalculatedSpend") or {}).get("ForecastedSpend", {}).get("Amount")]
                    for b in safe("budgets", lambda: c("budgets").describe_budgets(AccountId=ACCOUNT).get("Budgets", []), [])]
    r.log("budgets: %s" % D["budgets"])
    for key in ("state.json", "control.json"):
        D["governor_" + key.split(".")[0]] = safe("gov " + key, lambda: json.loads(
            c("s3").get_object(Bucket=PRIVATE, Key="spend-governor/" + key)["Body"].read()), None)
    st = D.get("governor_state") or {}
    r.log("governor: stage %s since %s, ledger entries %d" % (st.get("stage"), st.get("stage_since"),
                                                              len(st.get("ledger") or {})))


# ------------------------------------------------------------------------- 6. S3 requests per engine (logs) --
LOG_RE = re.compile(r'^(\S+) (\S+) \[([^\]]+)\] (\S+) (\S+) (\S+) (\S+) (\S+) "([^"]*)" (\S+) (\S+) (\S+) (\S+) (\S+) (\S+) '
                    r'"([^"]*)" "([^"]*)"')


def who(req):
    if "assumed-role" in req and req.count("/") >= 2:
        return req.split("/")[-1]           # Lambda: the session name is the function name
    if ":user/" in req:
        return "iam-user:" + req.split("/")[-1]
    if req == "-":
        return "anonymous"
    return req.split(":")[-1][:60]


def s3_attrib(r):
    r.section("6. S3 requests per engine (server access logs)")
    lg = c("s3").get_bucket_logging(Bucket=BUCKET).get("LoggingEnabled") or {}
    tb, tp = lg.get("TargetBucket"), lg.get("TargetPrefix") or ""
    if not tb:
        r.warn("access logging off")
        return
    base = "%s%s/%s/%s/" % (tp, ACCOUNT, REGION, BUCKET)
    out = {}
    for day in ("2026/10/10", "2026/10/18"):
        keys, token = [], None
        while True:
            kw = {"Bucket": tb, "Prefix": base + day + "/", "MaxKeys": 1000}
            if token:
                kw["ContinuationToken"] = token
            resp = c("s3").list_objects_v2(**kw)
            keys += [o["Key"] for o in resp.get("Contents", [])]
            if not resp.get("IsTruncated") or len(keys) >= 800000:
                break
            token = resp.get("NextContinuationToken")
        n = 5000
        step = max(1, len(keys) // n)
        sample = keys[::step][:n]
        scale = len(keys) / float(len(sample) or 1)

        def read(k):
            try:
                return c("s3").get_object(Bucket=tb, Key=k)["Body"].read().decode("utf-8", "replace")
            except Exception:  # noqa: BLE001
                return ""

        cnt = Counter()
        gbytes = Counter()
        with cf.ThreadPoolExecutor(max_workers=32) as ex:
            for text in ex.map(read, sample):
                for line in text.splitlines():
                    m = LOG_RE.match(line)
                    if not m:
                        continue
                    op = m.group(7)
                    cls = "t1" if any(x in op for x in (".PUT.", ".POST.", ".COPY.", "GET.BUCKET", ".LIST")) else \
                        "lc" if op.startswith("S3.") else "t2"
                    w = who(m.group(5))
                    cnt[(w, cls)] += 1
                    if op == "REST.GET.OBJECT" and m.group(12).isdigit():
                        gbytes[w] += int(m.group(12))
        per = {}
        for (w, cls), v in cnt.items():
            per.setdefault(w, {})[cls] = int(v * scale)
        for w, b in gbytes.items():
            per.setdefault(w, {})["get_gb"] = round(b * scale / 1e9, 3)
        out[day] = {"log_objects": len(keys), "sampled": len(sample), "scale": round(scale, 2), "per": per}
        top = sorted(per.items(), key=lambda kv: -(kv[1].get("t1", 0) * 5 + kv[1].get("t2", 0) * 0.4))[:12]
        r.log("%s: %d log objects (x%.1f); top by request cost: %s" % (
            day, len(keys), scale, [(k, v.get("t1", 0), v.get("t2", 0)) for k, v in top]))
    D["s3_requests"] = out


def main():
    with report("ops_6546_cost_audit") as r:
        r.heading("ops 6546 - deep cost audit (read-only)")
        t = time.time()
        for step in (cost_explorer, lambdas, logs, triggers, other, s3_attrib):
            try:
                step(r)
            except Exception as e:  # noqa: BLE001
                D["errors"].append("%s: %s" % (step.__name__, err(e)))
                r.warn("%s failed: %s" % (step.__name__, err(e)))
            r.log("elapsed %.0fs" % (time.time() - t))
        blob = gzip.compress(json.dumps(D, default=str, separators=(",", ":")).encode(), 9)
        safe("put private", lambda: c("s3").put_object(Bucket=PRIVATE, Key="audit/ops6546.json.gz", Body=blob,
                                                       ServerSideEncryption="AES256"))
        repo = Path(os.environ.get("GITHUB_WORKSPACE", os.getcwd()))
        out = repo / "aws" / "ops" / "reports" / "latest" / "ops_6546_cost_audit_data.md"
        out.parent.mkdir(parents=True, exist_ok=True)
        b64 = base64.b64encode(blob).decode()
        out.write_text("# ops 6546 data (gzip+base64 JSON)\n\n```\n" +
                       "\n".join(b64[i:i + 120] for i in range(0, len(b64), 120)) + "\n```\n", encoding="utf-8")
        r.log("data: %d KB compressed -> %s" % (len(blob) // 1024, out.name))
        r.log("errors recorded: %d %s" % (len(D["errors"]), D["errors"][:30]))
        critical = [e for e in D["errors"] if e.split(":")[0] in ("cost_explorer", "lambdas", "triggers")]
        if critical or "lambda" not in D:
            r.fail("a core section could not be read: %s" % critical)
            sys.exit(1)
        r.ok("audit complete")


if __name__ == "__main__":
    main()
