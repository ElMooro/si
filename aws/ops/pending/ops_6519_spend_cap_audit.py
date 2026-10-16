"""ops 6519 - READ-ONLY audit for the $500/month hard cap (no AWS mutation of any kind).

Khalid (2026-10-16): "Implement a $500 hard cap spending on my aws monthly bill".

State found in the repo before this op:
  * ops_set_500_monthly_budget.py (another lane, run 38903843468) created the AWS Budget
    JustHodl-Monthly-Hard-Cap-500 = $500 COST MONTHLY with EMAIL alerts at 50/80/100% ACTUAL
    and 100% FORECASTED  -> alerting only, nothing stops spend.
  * ops_budget_hard_stop_action.py (run 38907093924) created IAM policy JustHodl-Budget-Exceeded-Deny
    but CreateBudgetAction FAILED (AccessDenied: role JustHodl-Budgets-Action-Role was never created);
    the script still printed "configured" and the run went green. No enforcement exists.

This op measures, read-only, everything the enforcement layer (justhodl-spend-governor) must be
calibrated on: month-to-date spend by service and record type, daily run-rate since Sep 1, the
month-end forecast, the residual (non-stoppable) daily floor, the budget's own calculated spend,
the billing metric, every spend surface the governor would have to stop (Lambda + concurrency,
EventBridge rules, Scheduler schedules, event source mappings, S3 notifications, SageMaker,
EC2/RDS/etc.), the alert channels, and whether the runner identity can create what the governor needs.

Cost of this op: ~6 Cost Explorer requests ($0.06) + ~4 GetMetricData calls (<$0.02).
Writes aws/ops/reports/6519.json + reports/latest/ops_6519_spend_cap_audit.md.
"""

from datetime import datetime, timezone, timedelta, date
from pathlib import Path
import concurrent.futures as cf
import json
import os
import sys
import time
import urllib.request

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
BUDGET = "JustHodl-Monthly-Hard-Cap-500"
GOV_ROLE = "justhodl-spend-governor-role"
OUT_JSON = ROOT / "aws" / "ops" / "reports" / "6519.json"
CFG = Config(retries={"max_attempts": 8, "mode": "adaptive"}, read_timeout=60, connect_timeout=15)


def c(svc, region=REGION):
    return boto3.client(svc, region_name=region, config=CFG)


NOW = datetime.now(timezone.utc)
TODAY = NOW.date()
TOMORROW = TODAY + timedelta(days=1)
MONTH_START = TODAY.replace(day=1)
NEXT_MONTH = (MONTH_START + timedelta(days=32)).replace(day=1)
PREV_MONTH_START = (MONTH_START - timedelta(days=1)).replace(day=1)

out = {"ran_at": NOW.isoformat(), "op": 6519, "read_only": True, "errors": {}}


def mask_email(a):
    if not isinstance(a, str) or "@" not in a:
        return a
    u, d = a.split("@", 1)
    return (u[:1] + "***@" + d) if u else a


def guard(key):
    """Decorator: run a section, record its error instead of aborting the audit."""
    def deco(fn):
        def run(*a, **k):
            try:
                return fn(*a, **k)
            except Exception as e:  # noqa: BLE001 - audit must survive any one section
                out["errors"][key] = "%s: %s" % (type(e).__name__, str(e)[:400])
                return None
        return run
    return deco


def amt(x):
    try:
        return round(float(x), 4)
    except Exception:
        return None


# ---------------------------------------------------------------- cost --
@guard("cost_mtd_by_service")
def cost_mtd_by_service():
    ce = c("ce")
    r = ce.get_cost_and_usage(
        TimePeriod={"Start": MONTH_START.isoformat(), "End": TOMORROW.isoformat()},
        Granularity="MONTHLY", Metrics=["UnblendedCost", "NetUnblendedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}])
    rows = []
    for g in (r["ResultsByTime"][0]["Groups"] if r.get("ResultsByTime") else []):
        rows.append({"service": g["Keys"][0],
                     "unblended": amt(g["Metrics"]["UnblendedCost"]["Amount"]),
                     "net": amt(g["Metrics"]["NetUnblendedCost"]["Amount"])})
    rows.sort(key=lambda x: -(x["unblended"] or 0))
    out["mtd_by_service"] = rows
    out["mtd_total_unblended"] = round(sum(x["unblended"] or 0 for x in rows), 2)
    out["mtd_total_net"] = round(sum(x["net"] or 0 for x in rows), 2)


@guard("cost_mtd_by_record_type")
def cost_mtd_by_record_type():
    ce = c("ce")
    r = ce.get_cost_and_usage(
        TimePeriod={"Start": MONTH_START.isoformat(), "End": TOMORROW.isoformat()},
        Granularity="MONTHLY", Metrics=["UnblendedCost"],
        GroupBy=[{"Type": "DIMENSION", "Key": "RECORD_TYPE"}])
    out["mtd_by_record_type"] = {g["Keys"][0]: amt(g["Metrics"]["UnblendedCost"]["Amount"])
                                 for g in (r["ResultsByTime"][0]["Groups"] if r.get("ResultsByTime") else [])}


@guard("cost_daily_by_service")
def cost_daily_by_service():
    ce = c("ce")
    start = PREV_MONTH_START
    days = {}
    token = None
    while True:
        kw = dict(TimePeriod={"Start": start.isoformat(), "End": TOMORROW.isoformat()},
                  Granularity="DAILY", Metrics=["UnblendedCost"],
                  GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}])
        if token:
            kw["NextPageToken"] = token
        r = ce.get_cost_and_usage(**kw)
        for t in r["ResultsByTime"]:
            d = t["TimePeriod"]["Start"]
            per = days.setdefault(d, {"total": 0.0, "estimated": t.get("Estimated", False), "services": {}})
            for g in t.get("Groups", []):
                v = float(g["Metrics"]["UnblendedCost"]["Amount"])
                per["services"][g["Keys"][0]] = round(v, 4)
                per["total"] += v
            per["total"] = round(per["total"], 4)
        token = r.get("NextPageToken")
        if not token:
            break
    out["daily"] = days
    prev = [v["total"] for k, v in days.items() if k < MONTH_START.isoformat()]
    out["prev_month_total"] = round(sum(prev), 2)
    cur = sorted((k, v["total"]) for k, v in days.items() if k >= MONTH_START.isoformat())
    out["daily_current_month"] = cur
    complete = [v for k, v in cur if k < TODAY.isoformat()]
    if complete:
        out["avg_daily_mtd"] = round(sum(complete) / len(complete), 2)
        out["avg_daily_last7"] = round(sum(complete[-7:]) / len(complete[-7:]), 2)
        out["avg_daily_last3"] = round(sum(complete[-3:]) / len(complete[-3:]), 2)
        out["max_daily_mtd"] = round(max(complete), 2)


@guard("cost_forecast")
def cost_forecast():
    ce = c("ce")
    r = ce.get_cost_forecast(TimePeriod={"Start": TODAY.isoformat(), "End": NEXT_MONTH.isoformat()},
                             Metric="UNBLENDED_COST", Granularity="MONTHLY",
                             PredictionIntervalLevel=80)
    out["forecast_rest_of_month"] = {
        "mean": amt(r["Total"]["Amount"]),
        "p80_low": amt(r["ForecastResultsByTime"][0].get("PredictionIntervalLowerBound")),
        "p80_high": amt(r["ForecastResultsByTime"][0].get("PredictionIntervalUpperBound"))}


@guard("cost_usage_types_7d")
def cost_usage_types_7d():
    """Last 7 complete days by usage type -> what keeps billing when every engine is stopped."""
    ce = c("ce")
    start = TODAY - timedelta(days=7)
    agg = {}
    token = None
    while True:
        kw = dict(TimePeriod={"Start": start.isoformat(), "End": TODAY.isoformat()},
                  Granularity="MONTHLY", Metrics=["UnblendedCost"],
                  GroupBy=[{"Type": "DIMENSION", "Key": "USAGE_TYPE"}])
        if token:
            kw["NextPageToken"] = token
        r = ce.get_cost_and_usage(**kw)
        for t in r["ResultsByTime"]:
            for g in t.get("Groups", []):
                agg[g["Keys"][0]] = agg.get(g["Keys"][0], 0.0) + float(g["Metrics"]["UnblendedCost"]["Amount"])
        token = r.get("NextPageToken")
        if not token:
            break
    rows = sorted(({"usage_type": k, "usd_7d": round(v, 4), "usd_day": round(v / 7, 4)} for k, v in agg.items()),
                  key=lambda x: -x["usd_7d"])
    out["usage_types_7d"] = rows[:120]
    # Residual = charges that continue with every engine frozen (storage, hosted zones, alarms,
    # keys, secrets, registry, Studio EFS).  Pattern list is conservative; the governor re-derives it.
    pats = ("TimedStorage", "Storage", "HostedZone", "AlarmMonitorUsage", "KMS-Keys", "Secret",
            "EBS:VolumeUsage", "EBS:Snapshot", "ECR", "DataStorage", "Dashboards", "CW:MetricMonitorUsage",
            "SnapStart-Cached")
    resid = [r for r in rows if any(p.lower() in r["usage_type"].lower() for p in pats)]
    out["residual_floor"] = {"usd_day": round(sum(r["usd_day"] for r in resid), 4),
                             "lines": resid[:40]}


@guard("budgets")
def budgets_state():
    b = c("budgets")
    res = []
    for bud in b.describe_budgets(AccountId=ACCOUNT, MaxResults=100).get("Budgets", []):
        name = bud["BudgetName"]
        item = {"name": name, "type": bud.get("BudgetType"), "time_unit": bud.get("TimeUnit"),
                "limit": bud.get("BudgetLimit"), "time_period": {k: str(v) for k, v in (bud.get("TimePeriod") or {}).items()},
                "calculated": bud.get("CalculatedSpend"), "cost_types": bud.get("CostTypes"),
                "last_updated": str(bud.get("LastUpdatedTime")), "notifications": []}
        try:
            for n in b.describe_notifications_for_budget(AccountId=ACCOUNT, BudgetName=name).get("Notifications", []):
                subs = b.describe_subscribers_for_notification(AccountId=ACCOUNT, BudgetName=name, Notification=n)
                item["notifications"].append({
                    "type": n.get("NotificationType"), "op": n.get("ComparisonOperator"),
                    "threshold": n.get("Threshold"), "threshold_type": n.get("ThresholdType"),
                    "state": n.get("NotificationState"),
                    "subscribers": [{"type": s["SubscriptionType"], "address": mask_email(s["Address"])}
                                    for s in subs.get("Subscribers", [])]})
        except Exception as e:  # noqa: BLE001
            item["notifications_error"] = str(e)[:200]
        res.append(item)
    out["budgets"] = res
    try:
        acts = b.describe_budget_actions_for_account(AccountId=ACCOUNT).get("Actions", [])
        out["budget_actions"] = [{"budget": a.get("BudgetName"), "type": a.get("ActionType"),
                                  "status": a.get("Status"), "threshold": a.get("ActionThreshold")} for a in acts]
    except Exception as e:  # noqa: BLE001
        out["errors"]["budget_actions"] = str(e)[:200]


@guard("billing_metric")
def billing_metric():
    cw = c("cloudwatch")
    r = cw.get_metric_statistics(Namespace="AWS/Billing", MetricName="EstimatedCharges",
                                 Dimensions=[{"Name": "Currency", "Value": "USD"}],
                                 StartTime=NOW - timedelta(days=3), EndTime=NOW, Period=21600,
                                 Statistics=["Maximum"])
    pts = sorted(r.get("Datapoints", []), key=lambda p: p["Timestamp"])
    out["billing_metric"] = {"available": bool(pts),
                             "points": [(p["Timestamp"].isoformat(), round(p["Maximum"], 2)) for p in pts]}


# ------------------------------------------------------------ inventory --
@guard("lambda")
def lambda_inventory():
    lam = c("lambda")
    fns = []
    for page in lam.get_paginator("list_functions").paginate():
        for f in page["Functions"]:
            fns.append({"name": f["FunctionName"], "mem": f.get("MemorySize"), "timeout": f.get("Timeout"),
                        "role": (f.get("Role") or "").split("/")[-1], "runtime": f.get("Runtime"),
                        "arch": (f.get("Architectures") or ["x86_64"])[0]})

    def conc(name):
        for attempt in range(6):
            try:
                return name, lam.get_function_concurrency(FunctionName=name).get("ReservedConcurrentExecutions")
            except ClientError as e:
                if e.response["Error"]["Code"] in ("TooManyRequestsException", "ThrottlingException"):
                    time.sleep(1.5 * (attempt + 1))
                    continue
                return name, "ERR:" + e.response["Error"]["Code"]
        return name, "ERR:throttled"

    with cf.ThreadPoolExecutor(max_workers=5) as ex:
        conc_map = dict(ex.map(conc, [f["name"] for f in fns]))
    for f in fns:
        f["reserved"] = conc_map.get(f["name"])
    try:
        acct = lam.get_account_settings()
    except Exception as e:  # noqa: BLE001
        out["errors"]["lambda_account_settings"] = str(e)[:160]
        acct = {"AccountLimit": {}}
    hist = {}
    for f in fns:
        k = "unset" if f["reserved"] is None else str(f["reserved"])
        hist[k] = hist.get(k, 0) + 1
    out["lambda"] = {"count": len(fns), "reserved_histogram": hist,
                     "account_limit": acct["AccountLimit"].get("ConcurrentExecutions"),
                     "unreserved": acct["AccountLimit"].get("UnreservedConcurrentExecutions"),
                     "roles": sorted({f["role"] for f in fns}),
                     "role_counts": {r: sum(1 for f in fns if f["role"] == r) for r in sorted({f["role"] for f in fns})},
                     "memory_total_mb": sum(f["mem"] or 0 for f in fns)}
    out["_lambda_functions"] = fns
    return fns


@guard("lambda_usage_7d")
def lambda_usage_7d(fns):
    if not fns:
        return
    cw = c("cloudwatch")
    end = NOW.replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=7)
    queries = []
    for i, f in enumerate(fns):
        for m, tag in (("Duration", "d"), ("Invocations", "n")):
            queries.append({"Id": "%s%d" % (tag, i), "ReturnData": True,
                            "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": m,
                                                      "Dimensions": [{"Name": "FunctionName", "Value": f["name"]}]},
                                           "Period": 604800, "Stat": "Sum"}})
    vals = {}
    for i in range(0, len(queries), 500):
        token = None
        while True:
            kw = dict(MetricDataQueries=queries[i:i + 500], StartTime=start, EndTime=end)
            if token:
                kw["NextToken"] = token
            r = cw.get_metric_data(**kw)
            for res in r["MetricDataResults"]:
                vals[res["Id"]] = vals.get(res["Id"], 0.0) + sum(res.get("Values") or [])
            token = r.get("NextToken")
            if not token:
                break
    rows = []
    price_x86, price_arm = 0.0000166667, 0.0000133334
    for i, f in enumerate(fns):
        ms = vals.get("d%d" % i, 0.0)
        gbs = ms / 1000.0 * (f["mem"] or 128) / 1024.0
        usd = gbs * (price_arm if f["arch"] == "arm64" else price_x86) + vals.get("n%d" % i, 0.0) * 2e-7
        rows.append({"fn": f["name"], "gb_s_7d": round(gbs, 1), "invocations_7d": int(vals.get("n%d" % i, 0)),
                     "usd_7d_est": round(usd, 3), "mem": f["mem"], "reserved": f.get("reserved")})
    rows.sort(key=lambda x: -x["usd_7d_est"])
    out["lambda_top_spenders_7d"] = rows[:60]
    out["lambda_est_usd_7d_total"] = round(sum(r["usd_7d_est"] for r in rows), 2)
    out["lambda_active_7d"] = sum(1 for r in rows if r["invocations_7d"] > 0)


@guard("eventbridge_rules")
def eventbridge_rules():
    ev = c("events")
    buses = [b["Name"] for b in ev.list_event_buses().get("EventBuses", [])]
    rules = []
    for bus in buses:
        token = None
        while True:
            kw = {"EventBusName": bus, "Limit": 100}
            if token:
                kw["NextToken"] = token
            r = ev.list_rules(**kw)
            for x in r.get("Rules", []):
                rules.append({"bus": bus, "name": x["Name"], "state": x.get("State"),
                              "schedule": x.get("ScheduleExpression"), "managed_by": x.get("ManagedBy")})
            token = r.get("NextToken")
            if not token:
                break
    lam_targets = 0
    enabled = [x for x in rules if x["state"] == "ENABLED"]
    for x in enabled:
        try:
            t = ev.list_targets_by_rule(Rule=x["name"], EventBusName=x["bus"]).get("Targets", [])
            x["targets"] = [y["Arn"].split(":")[-1] if ":function:" in y["Arn"] else y["Arn"] for y in t]
            x["lambda_targets"] = sum(1 for y in t if ":function:" in y["Arn"])
            lam_targets += x["lambda_targets"]
        except ClientError as e:
            x["targets_error"] = str(e)[:120]
    out["eventbridge"] = {"buses": buses, "rules_total": len(rules), "enabled": len(enabled),
                          "disabled": sum(1 for x in rules if x["state"] != "ENABLED"),
                          "managed_rules": sum(1 for x in rules if x.get("managed_by")),
                          "enabled_with_lambda_targets": sum(1 for x in enabled if x.get("lambda_targets")),
                          "lambda_target_count": lam_targets}
    out["_eventbridge_rules"] = rules


@guard("scheduler")
def scheduler_inventory():
    sch = c("scheduler")
    groups = [g["Name"] for p in sch.get_paginator("list_schedule_groups").paginate() for g in p["ScheduleGroups"]]
    items = []
    for p in sch.get_paginator("list_schedules").paginate():
        for s in p["Schedules"]:
            items.append({"group": s.get("GroupName"), "name": s["Name"], "state": s.get("State"),
                          "target": (s.get("Target") or {}).get("Arn", "").split(":")[-1]})
    roles = {}
    sample = [s for s in items if s["state"] == "ENABLED"][:15]
    for s in sample:
        try:
            g = sch.get_schedule(Name=s["name"], GroupName=s["group"])
            roles[g["Target"].get("RoleArn", "").split("/")[-1]] = roles.get(g["Target"].get("RoleArn", "").split("/")[-1], 0) + 1
            s["expr"] = g.get("ScheduleExpression")
            s["has_end"] = bool(g.get("EndDate"))
            s["action_after"] = g.get("ActionAfterCompletion")
        except ClientError as e:
            s["get_error"] = str(e)[:120]
    out["scheduler"] = {"groups": groups, "total": len(items),
                        "enabled": sum(1 for s in items if s["state"] == "ENABLED"),
                        "disabled": sum(1 for s in items if s["state"] != "ENABLED"),
                        "sampled_target_roles": roles, "sample": sample}
    out["_scheduler"] = items


@guard("event_source_mappings")
def esm_inventory():
    lam = c("lambda")
    items = []
    for p in lam.get_paginator("list_event_source_mappings").paginate():
        for m in p["EventSourceMappings"]:
            items.append({"uuid": m["UUID"], "fn": m.get("FunctionArn", "").split(":")[-1],
                          "source": (m.get("EventSourceArn") or "").split(":")[2] if m.get("EventSourceArn") else "self-managed",
                          "state": m.get("State")})
    out["event_source_mappings"] = items


@guard("s3_notifications")
def s3_notifications():
    s3 = c("s3")
    n = s3.get_bucket_notification_configuration(Bucket=BUCKET)
    out["s3_notifications"] = {
        "lambda": [{"fn": x["LambdaFunctionArn"].split(":")[-1], "events": x.get("Events"),
                    "filter": x.get("Filter")} for x in n.get("LambdaFunctionConfigurations", [])],
        "sqs": len(n.get("QueueConfigurations", [])), "sns": len(n.get("TopicConfigurations", [])),
        "eventbridge": "EventBridgeConfiguration" in n}


@guard("sagemaker")
def sagemaker_inventory():
    sm = c("sagemaker")
    eps = []
    for p in sm.get_paginator("list_endpoints").paginate():
        for e in p["Endpoints"]:
            d = sm.describe_endpoint(EndpointName=e["EndpointName"])
            item = {"name": e["EndpointName"], "status": e["EndpointStatus"], "config": d.get("EndpointConfigName"),
                    "variants": [{"name": v.get("VariantName"), "current": v.get("CurrentInstanceCount"),
                                  "desired": v.get("DesiredInstanceCount"),
                                  "serverless": bool(v.get("CurrentServerlessConfig"))}
                                 for v in d.get("ProductionVariants", [])]}
            try:
                ec = sm.describe_endpoint_config(EndpointConfigName=d["EndpointConfigName"])
                item["async"] = bool(ec.get("AsyncInferenceConfig"))
                item["instance_types"] = [v.get("InstanceType") or "serverless" for v in ec.get("ProductionVariants", [])]
            except ClientError as ex:
                item["config_error"] = str(ex)[:120]
            try:
                item["tags"] = {t["Key"]: t["Value"] for t in sm.list_tags(ResourceArn=d["EndpointArn"]).get("Tags", [])}
            except ClientError:
                pass
            eps.append(item)

    def safe(label, fn):
        try:
            return fn()
        except Exception as ex:  # noqa: BLE001
            return "ERR:%s:%s" % (type(ex).__name__, str(ex)[:90])

    out["sagemaker"] = {
        "endpoints": eps,
        "training_in_progress": safe("training", lambda: [j["TrainingJobName"] for j in sm.list_training_jobs(StatusEquals="InProgress", MaxResults=50).get("TrainingJobSummaries", [])]),
        "processing_in_progress": safe("processing", lambda: [j["ProcessingJobName"] for j in sm.list_processing_jobs(StatusEquals="InProgress", MaxResults=50).get("ProcessingJobSummaries", [])]),
        "transform_in_progress": safe("transform", lambda: [j["TransformJobName"] for j in sm.list_transform_jobs(StatusEquals="InProgress", MaxResults=50).get("TransformJobSummaries", [])]),
        "hpo_in_progress": safe("hpo", lambda: [j["HyperParameterTuningJobName"] for j in sm.list_hyper_parameter_tuning_jobs(StatusEquals="InProgress", MaxResults=50).get("HyperParameterTuningJobSummaries", [])]),
        "notebooks_in_service": safe("notebooks", lambda: [n["NotebookInstanceName"] for n in sm.list_notebook_instances(StatusEquals="InService").get("NotebookInstances", [])]),
        "apps": safe("apps", lambda: [{"domain": a.get("DomainId"), "type": a.get("AppType"), "name": a.get("AppName"), "status": a.get("Status")}
                                       for a in sm.list_apps(MaxResults=100).get("Apps", []) if a.get("Status") not in ("Deleted",)]),
        "domains": safe("domains", lambda: [d["DomainId"] for d in sm.list_domains().get("Domains", [])]),
    }
    aas = c("application-autoscaling")
    out["sagemaker"]["scalable_targets"] = [
        {"resource": t["ResourceId"], "min": t["MinCapacity"], "max": t["MaxCapacity"],
         "suspended": t.get("SuspendedState")}
        for t in aas.describe_scalable_targets(ServiceNamespace="sagemaker").get("ScalableTargets", [])]
    cw = c("cloudwatch")
    al = cw.describe_alarms(AlarmNamePrefix="jh-owned").get("MetricAlarms", [])
    out["sagemaker"]["owned_alarms"] = [{"name": a["AlarmName"], "actions_enabled": a["ActionsEnabled"],
                                          "state": a["StateValue"], "actions": [x.split(":")[-1][:60] for x in a.get("AlarmActions", [])]}
                                         for a in al]


@guard("other_compute")
def other_compute():
    res = {}
    for region in (REGION, "us-west-2"):
        r = {}
        ec2 = c("ec2", region)
        try:
            inst = [i for rr in ec2.describe_instances(Filters=[{"Name": "instance-state-name", "Values": ["running", "pending"]}]).get("Reservations", []) for i in rr["Instances"]]
            r["ec2_running"] = [{"id": i["InstanceId"], "type": i["InstanceType"],
                                 "name": next((t["Value"] for t in i.get("Tags", []) if t["Key"] == "Name"), None)} for i in inst]
        except Exception as e:  # noqa: BLE001
            r["ec2_error"] = str(e)[:120]
        for label, call in (
            ("nat_gateways", lambda: [n["NatGatewayId"] for n in ec2.describe_nat_gateways(Filters=[{"Name": "state", "Values": ["available", "pending"]}]).get("NatGateways", [])]),
            ("eips_unattached", lambda: [a.get("PublicIp") for a in ec2.describe_addresses().get("Addresses", []) if not a.get("AssociationId")]),
            ("ebs_volumes_gb", lambda: sum(v["Size"] for v in ec2.describe_volumes().get("Volumes", []))),
            ("load_balancers", lambda: [lb["LoadBalancerName"] for lb in c("elbv2", region).describe_load_balancers().get("LoadBalancers", [])]),
            ("rds_instances", lambda: [{"id": d["DBInstanceIdentifier"], "status": d["DBInstanceStatus"], "class": d["DBInstanceClass"]} for d in c("rds", region).describe_db_instances().get("DBInstances", [])]),
            ("rds_clusters", lambda: [{"id": d["DBClusterIdentifier"], "status": d["Status"]} for d in c("rds", region).describe_db_clusters().get("DBClusters", [])]),
            ("elasticache", lambda: [x["CacheClusterId"] for x in c("elasticache", region).describe_cache_clusters().get("CacheClusters", [])]),
            ("opensearch", lambda: [x["DomainName"] for x in c("opensearch", region).list_domain_names().get("DomainNames", [])]),
            ("apprunner", lambda: [{"name": s["ServiceName"], "status": s["Status"]} for s in c("apprunner", region).list_services().get("ServiceSummaryList", [])]),
            ("ecs_clusters", lambda: c("ecs", region).list_clusters().get("clusterArns", [])),
            ("eks_clusters", lambda: c("eks", region).list_clusters().get("clusters", [])),
            ("kinesis_streams", lambda: c("kinesis", region).list_streams().get("StreamNames", [])),
        ):
            try:
                r[label] = call()
            except Exception as e:  # noqa: BLE001
                r[label] = "ERR:" + type(e).__name__ + ":" + str(e)[:90]
        res[region] = r
    # ECS running tasks (us-east-1 only)
    try:
        ecs = c("ecs")
        running = []
        for cl in res[REGION].get("ecs_clusters") or []:
            if isinstance(cl, str) and cl.startswith("arn:"):
                svcs = ecs.list_services(cluster=cl).get("serviceArns", [])
                if svcs:
                    for s in ecs.describe_services(cluster=cl, services=svcs[:10]).get("services", []):
                        running.append({"cluster": cl.split("/")[-1], "service": s["serviceName"],
                                        "desired": s["desiredCount"], "running": s["runningCount"]})
        res[REGION]["ecs_services"] = running
    except Exception as e:  # noqa: BLE001
        res[REGION]["ecs_services_error"] = str(e)[:120]
    # Step Functions
    try:
        sfn = c("stepfunctions")
        machines = []
        for p in sfn.get_paginator("list_state_machines").paginate():
            for m in p["stateMachines"]:
                run = sfn.list_executions(stateMachineArn=m["stateMachineArn"], statusFilter="RUNNING", maxResults=100).get("executions", [])
                machines.append({"name": m["name"], "type": m.get("type"), "running": len(run)})
        res["stepfunctions"] = machines
    except Exception as e:  # noqa: BLE001
        res["stepfunctions_error"] = str(e)[:120]
    # DynamoDB provisioned capacity
    try:
        ddb = c("dynamodb")
        prov = []
        tables = [t for p in ddb.get_paginator("list_tables").paginate() for t in p["TableNames"]]
        for t in tables[:150]:
            d = ddb.describe_table(TableName=t)["Table"]
            mode = (d.get("BillingModeSummary") or {}).get("BillingMode", "PROVISIONED")
            if mode == "PROVISIONED":
                pt = d.get("ProvisionedThroughput", {})
                prov.append({"table": t, "rcu": pt.get("ReadCapacityUnits"), "wcu": pt.get("WriteCapacityUnits")})
        res["dynamodb"] = {"tables": len(tables), "provisioned": prov}
    except Exception as e:  # noqa: BLE001
        res["dynamodb_error"] = str(e)[:120]
    out["other_compute"] = res


@guard("s3_size")
def s3_size():
    cw = c("cloudwatch")
    sizes = {}
    for st in ("StandardStorage", "StandardIAStorage", "IntelligentTieringFAStorage", "GlacierInstantRetrievalStorage"):
        r = cw.get_metric_statistics(Namespace="AWS/S3", MetricName="BucketSizeBytes",
                                     Dimensions=[{"Name": "BucketName", "Value": BUCKET}, {"Name": "StorageType", "Value": st}],
                                     StartTime=NOW - timedelta(days=3), EndTime=NOW, Period=86400, Statistics=["Average"])
        pts = sorted(r.get("Datapoints", []), key=lambda p: p["Timestamp"])
        if pts:
            sizes[st] = round(pts[-1]["Average"] / 1e9, 1)
    out["s3_live_bucket_gb"] = sizes


# --------------------------------------------------------- alert paths --
@guard("alerts")
def alert_paths():
    sns = c("sns")
    topics = [t["TopicArn"] for p in sns.get_paginator("list_topics").paginate() for t in p["Topics"]]
    jt = [t for t in topics if "justhodl" in t.lower() or "budget" in t.lower() or "spend" in t.lower()]
    subs = {}
    for t in jt:
        try:
            subs[t.split(":")[-1]] = [{"protocol": s["Protocol"],
                                       "endpoint": mask_email(s["Endpoint"]) if s["Protocol"] == "email" else s["Endpoint"].split(":")[-1],
                                       "pending": s["SubscriptionArn"] == "PendingConfirmation"}
                                      for s in sns.list_subscriptions_by_topic(TopicArn=t).get("Subscriptions", [])]
        except ClientError as e:
            subs[t.split(":")[-1]] = "ERR:" + str(e)[:80]
    out["sns_topics"] = subs
    cw = c("cloudwatch")
    guards = []
    for p in cw.get_paginator("describe_alarms").paginate(AlarmNamePrefix="justhodl-guard"):
        for a in p["MetricAlarms"]:
            guards.append({"name": a["AlarmName"], "state": a["StateValue"], "threshold": a.get("Threshold"),
                           "actions_enabled": a["ActionsEnabled"]})
    out["guard_alarms"] = guards
    total = 0
    for p in cw.get_paginator("describe_alarms").paginate():
        total += len(p["MetricAlarms"]) + len(p.get("CompositeAlarms", []))
    out["alarms_total"] = total
    # Telegram bot health (token never printed)
    tok = None
    try:
        ssm = c("ssm")
        tok = ssm.get_parameter(Name="/justhodl/telegram/bot_token", WithDecryption=True)["Parameter"]["Value"]
        req = urllib.request.Request("https://api.telegram.org/bot%s/getMe" % tok, headers={"User-Agent": "justhodl-ops/6519"})
        with urllib.request.urlopen(req, timeout=10) as r:
            out["telegram_ssm"] = {"ok": json.loads(r.read().decode()).get("ok")}
    except Exception as e:  # noqa: BLE001
        msg = str(e)
        if tok:
            msg = msg.replace(tok, "<redacted>")
        out["telegram_ssm"] = {"ok": False, "error": msg[:120]}
    if os.environ.get("TELEGRAM_BOT_TOKEN") or os.environ.get("TELEGRAM_TOKEN"):
        tok2 = os.environ.get("TELEGRAM_BOT_TOKEN") or os.environ.get("TELEGRAM_TOKEN")
        try:
            req = urllib.request.Request("https://api.telegram.org/bot%s/getMe" % tok2, headers={"User-Agent": "justhodl-ops/6519"})
            with urllib.request.urlopen(req, timeout=10) as r:
                out["telegram_runner_secret"] = {"ok": json.loads(r.read().decode()).get("ok")}
        except Exception as e:  # noqa: BLE001
            out["telegram_runner_secret"] = {"ok": False, "error": type(e).__name__}


# ------------------------------------------------------------- identity --
@guard("iam")
def iam_state():
    sts = c("sts")
    ident = sts.get_caller_identity()
    out["runner_arn"] = ident["Arn"]
    iam = c("iam")
    st = {}
    for role in ("JustHodl-Budgets-Action-Role", "justhodl-scheduler-role", GOV_ROLE, "lambda-execution-role"):
        try:
            r = iam.get_role(RoleName=role)["Role"]
            st[role] = {"exists": True, "trust": [s.get("Principal") for s in r["AssumeRolePolicyDocument"]["Statement"]]}
        except ClientError as e:
            st[role] = {"exists": False, "code": e.response["Error"]["Code"]}
    try:
        arn = "arn:aws:iam::%s:policy/JustHodl-Budget-Exceeded-Deny" % ACCOUNT
        ent = iam.list_entities_for_policy(PolicyArn=arn)
        st["JustHodl-Budget-Exceeded-Deny"] = {"exists": True,
                                                "attached_roles": [x["RoleName"] for x in ent.get("PolicyRoles", [])],
                                                "attached_users": [x["UserName"] for x in ent.get("PolicyUsers", [])],
                                                "attached_groups": [x["GroupName"] for x in ent.get("PolicyGroups", [])]}
    except ClientError as e:
        st["JustHodl-Budget-Exceeded-Deny"] = {"exists": False, "code": e.response["Error"]["Code"]}
    out["iam"] = st
    if ":user/" in ident["Arn"]:
        user = ident["Arn"].split("/")[-1]
        try:
            out["runner_user_attached"] = len(iam.list_attached_user_policies(UserName=user)["AttachedPolicies"])
            groups = [g["GroupName"] for g in iam.list_groups_for_user(UserName=user)["Groups"]]
            out["runner_groups"] = {g: [p["PolicyName"] for p in iam.list_attached_group_policies(GroupName=g)["AttachedPolicies"]] for g in groups}
        except ClientError as e:
            out["errors"]["runner_policies"] = str(e)[:200]
    needed = ["budgets:ViewBudget", "budgets:ModifyBudget", "ce:GetCostAndUsage", "ce:GetCostForecast",
              "sns:CreateTopic", "sns:SetTopicAttributes", "sns:Subscribe", "sns:Publish",
              "lambda:CreateFunction", "lambda:AddPermission", "lambda:PutFunctionConcurrency",
              "lambda:DeleteFunctionConcurrency", "lambda:TagResource", "lambda:InvokeFunction",
              "scheduler:CreateSchedule", "scheduler:UpdateSchedule", "scheduler:GetSchedule",
              "cloudwatch:PutMetricAlarm", "cloudwatch:PutMetricData", "events:DisableRule", "events:EnableRule",
              "s3:PutObject", "s3:GetObject", "organizations:DescribeOrganization"]
    iam_needed = ["iam:CreateRole", "iam:PutRolePolicy", "iam:PassRole", "iam:GetRole", "iam:TagRole",
                  "iam:UpdateAssumeRolePolicy"]
    sim = {}
    try:
        for chunk in (needed,):
            r = iam.simulate_principal_policy(PolicySourceArn=ident["Arn"], ActionNames=chunk, ResourceArns=["*"])
            for e in r["EvaluationResults"]:
                sim[e["EvalActionName"]] = e["EvalDecision"]
        r = iam.simulate_principal_policy(PolicySourceArn=ident["Arn"], ActionNames=iam_needed,
                                          ResourceArns=["arn:aws:iam::%s:role/%s" % (ACCOUNT, GOV_ROLE)])
        for e in r["EvaluationResults"]:
            sim[e["EvalActionName"] + "@gov-role"] = e["EvalDecision"]
        r = iam.simulate_principal_policy(PolicySourceArn=ident["Arn"], ActionNames=["iam:PassRole"],
                                          ResourceArns=["arn:aws:iam::%s:role/justhodl-scheduler-role" % ACCOUNT])
        sim["iam:PassRole@scheduler-role"] = r["EvaluationResults"][0]["EvalDecision"]
    except ClientError as e:
        out["errors"]["simulate"] = str(e)[:200]
    out["runner_simulation"] = sim
    try:
        o = c("organizations").describe_organization()["Organization"]
        out["organization"] = {"member": True, "master_is_self": o.get("MasterAccountId") == ACCOUNT,
                               "feature_set": o.get("FeatureSet")}
    except ClientError as e:
        out["organization"] = {"member": False, "code": e.response["Error"]["Code"]}


def main():
    with report("ops_6519_spend_cap_audit") as r:
        r.heading("ops 6519 - $500 hard cap: read-only spend-surface audit")
        cost_mtd_by_service()
        cost_mtd_by_record_type()
        cost_daily_by_service()
        cost_forecast()
        cost_usage_types_7d()
        budgets_state()
        billing_metric()
        fns = lambda_inventory()
        lambda_usage_7d(fns)
        eventbridge_rules()
        scheduler_inventory()
        esm_inventory()
        s3_notifications()
        sagemaker_inventory()
        other_compute()
        s3_size()
        alert_paths()
        iam_state()

        r.section("Spend")
        r.kv(metric="mtd_total_unblended", value=out.get("mtd_total_unblended"))
        r.kv(metric="prev_month_total", value=out.get("prev_month_total"))
        r.kv(metric="avg_daily_mtd", value=out.get("avg_daily_mtd"))
        r.kv(metric="avg_daily_last3", value=out.get("avg_daily_last3"))
        r.kv(metric="forecast_rest_of_month_mean", value=(out.get("forecast_rest_of_month") or {}).get("mean"))
        r.kv(metric="residual_floor_usd_day", value=(out.get("residual_floor") or {}).get("usd_day"))
        r.kv(metric="lambda_est_usd_7d_total", value=out.get("lambda_est_usd_7d_total"))
        for row in (out.get("mtd_by_service") or [])[:12]:
            r.log("MTD %-45s $%.2f" % (row["service"], row["unblended"] or 0))
        r.section("Budgets")
        for b in out.get("budgets") or []:
            r.log("%s limit=%s actual=%s forecast=%s notifications=%d" % (
                b["name"], (b.get("limit") or {}).get("Amount"),
                ((b.get("calculated") or {}).get("ActualSpend") or {}).get("Amount"),
                ((b.get("calculated") or {}).get("ForecastedSpend") or {}).get("Amount"),
                len(b.get("notifications") or [])))
        r.log("budget actions: %s" % json.dumps(out.get("budget_actions")))
        r.section("Surfaces")
        r.log("lambda: %s" % json.dumps({k: v for k, v in (out.get("lambda") or {}).items() if k not in ("roles",)})[:900])
        r.log("eventbridge: %s" % json.dumps(out.get("eventbridge")))
        r.log("scheduler: total=%s enabled=%s roles=%s" % ((out.get("scheduler") or {}).get("total"), (out.get("scheduler") or {}).get("enabled"), (out.get("scheduler") or {}).get("sampled_target_roles")))
        r.log("esm: %d  s3->lambda notifications: %d" % (len(out.get("event_source_mappings") or []), len((out.get("s3_notifications") or {}).get("lambda") or [])))
        r.log("sagemaker endpoints: %s" % json.dumps([(e["name"], e["status"], e.get("async"), e.get("instance_types")) for e in (out.get("sagemaker") or {}).get("endpoints", [])]))
        r.section("Identity / alerts")
        r.log("runner: %s" % out.get("runner_arn"))
        r.log("simulation: %s" % json.dumps(out.get("runner_simulation")))
        r.log("iam: %s" % json.dumps(out.get("iam")))
        r.log("sns: %s" % json.dumps(out.get("sns_topics"))[:900])
        r.log("telegram ssm: %s runner-secret: %s" % (out.get("telegram_ssm"), out.get("telegram_runner_secret")))
        if out["errors"]:
            r.section("Section errors (non-fatal)")
            for k, v in out["errors"].items():
                r.warn("%s -> %s" % (k, v))
        OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
        r.ok("wrote %s" % OUT_JSON.relative_to(ROOT))
        core_missing = [k for k in ("cost_mtd_by_service", "budgets", "lambda") if k in out["errors"]]
        if core_missing:
            r.fail("core sections failed: %s" % core_missing)
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
