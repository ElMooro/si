"""ops 6534 - READ-ONLY audit before building the runaway guardrails (Khalid 2026-10-16: "never ever again").

Calibrates every threshold from this account's own history and inventories what already exists:
  1. account-level Lambda 5-min Invocations / ConcurrentExecutions, Sep 15 - Oct 11 (normal) vs Oct 12-16 (storm):
     percentiles and the highest level SUSTAINED for 15 min (what a 3-of-3 alarm compares)
  2. hourly invocation maxima of the event coordinator and every function its routes invoke (breaker budgets)
  3. the custom bus justhodl-system-events: rules, patterns, targets, retry policies; bus metrics
  4. existing alarms on runaway/spend metrics and their actions
  5. Cost Anomaly Detection monitors/subscriptions, AWS Budgets + subscribers (emails masked)
  6. SNS topics justhodl-fleet-alerts / justhodl-spend-governor: subscriptions and whether confirmed
  7. justhodl-dashboard-live: top-level prefixes, versioning, lifecycle rules, Lambda notifications
  8. lambda-execution-role (the coordinator's role): policies; simulated DynamoDB/SNS rights
  9. event source mappings, functions with RecursiveLoop=Allow, account concurrency settings
Only the markdown report is kept by the direct lane, so everything decision-relevant is logged.
"""

from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
import concurrent.futures as cf
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUS = "justhodl-system-events"
BUCKET = "justhodl-dashboard-live"
COORD = "justhodl-event-coordinator"
TARGETS = ["justhodl-calibrator", "justhodl-alpha-calibrator", "justhodl-alpha-compass", "justhodl-signal-board",
           "justhodl-miss-calibrator", "justhodl-engine-signal-map", "justhodl-jh-fusion"]
CFG = Config(retries={"max_attempts": 8, "mode": "standard"})
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
ev = boto3.client("events", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
sns = boto3.client("sns", region_name=REGION, config=CFG)
iam = boto3.client("iam", config=CFG)
ce = boto3.client("ce", region_name=REGION, config=CFG)
budgets = boto3.client("budgets", region_name=REGION, config=CFG)
UTC = timezone.utc
errors = {}


def T(*a):
    return datetime(*a, tzinfo=UTC)


def mask(addr):
    s = str(addr or "")
    if "@" in s:
        local, _, dom = s.partition("@")
        return local[:1] + "***@" + dom
    if s.startswith("arn:"):
        return s
    return s[:4] + "***" if s else s


def guard(name, r, fn):
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        errors[name] = "%s: %s" % (type(e).__name__, str(e)[:240])
        r.warn("%s failed: %s" % (name, errors[name]))
        return None


def series(metric, stat, start, end, period, fn=None, ns="AWS/Lambda", dims=None):
    """Datapoints in chunks of <= 1,440 per call."""
    pts, step = [], period * 1440
    cur = start
    while cur < end:
        stop = min(end, datetime.fromtimestamp(cur.timestamp() + step, UTC))
        kw = {"Namespace": ns, "MetricName": metric, "StartTime": cur, "EndTime": stop, "Period": period,
              "Statistics": [stat]}
        d = dims if dims is not None else ([{"Name": "FunctionName", "Value": fn}] if fn else [])
        if d:
            kw["Dimensions"] = d
        pts += [(p["Timestamp"], p[stat]) for p in cw.get_metric_statistics(**kw).get("Datapoints", [])]
        cur = stop
    pts.sort()
    return pts


def pct(vals, q):
    if not vals:
        return 0
    v = sorted(vals)
    return v[min(len(v) - 1, int(q * len(v)))]


def sustained(pts, n=3):
    """Highest value held for n consecutive periods = max over windows of the window minimum."""
    vals = [v for _, v in pts]
    return max((min(vals[i:i + n]) for i in range(0, max(0, len(vals) - n + 1))), default=0)


def dist(pts):
    vals = [v for _, v in pts]
    return {"n": len(vals), "p50": round(pct(vals, .5), 1), "p99": round(pct(vals, .99), 1),
            "p999": round(pct(vals, .999), 1), "max": round(max(vals, default=0), 1),
            "sustained15": round(sustained(pts, 3), 1), "sustained30": round(sustained(pts, 6), 1)}


def account_profile(r):
    normal = (T(2026, 9, 15), T(2026, 10, 12))
    storm = (T(2026, 10, 12, 9, 15), T(2026, 10, 16, 19, 40))
    for metric, stat in (("Invocations", "Sum"), ("ConcurrentExecutions", "Maximum"),
                         ("ConcurrentExecutions", "Average"), ("Throttles", "Sum")):
        n = dist(series(metric, stat, *normal, 300))
        s = dist(series(metric, stat, *storm, 300))
        r.log("account %s %s per 5 min | NORMAL %s | STORM %s" % (metric, stat, json.dumps(n), json.dumps(s)))
    # Which days/times produced the normal maxima (to see what legit bursts look like)
    inv = series("Invocations", "Sum", *normal, 300)
    top = sorted(inv, key=lambda p: -p[1])[:8]
    r.log("normal top 5-min invocation bursts: %s" % [(t.strftime("%m-%d %H:%M"), int(v)) for t, v in top])
    conc = series("ConcurrentExecutions", "Maximum", *normal, 300)
    topc = sorted(conc, key=lambda p: -p[1])[:8]
    r.log("normal top 5-min concurrency peaks: %s" % [(t.strftime("%m-%d %H:%M"), int(v)) for t, v in topc])
    # Duration (GB-s proxy needs memory per function - account Duration sum is still a useful burn signal)
    dur_n = dist(series("Duration", "Sum", *normal, 300))
    dur_s = dist(series("Duration", "Sum", *storm, 300))
    r.log("account Duration Sum ms per 5 min | NORMAL %s | STORM %s" % (json.dumps(dur_n), json.dumps(dur_s)))


def target_profile(r):
    s, e = T(2026, 9, 1), T(2026, 10, 12)
    for fn in [COORD] + TARGETS:
        pts = series("Invocations", "Sum", s, e, 3600, fn=fn)
        vals = [v for _, v in pts]
        daily = Counter()
        for t, v in pts:
            daily[t.strftime("%m-%d")] += v
        r.log("%-30s hourly max %4d p99 %4d | daily max %5d | total %6d | hours active %d" % (
            fn, max(vals, default=0), pct(vals, .99), max(daily.values(), default=0), sum(vals), len(vals)))


def bus(r):
    rules = []
    for page in ev.get_paginator("list_rules").paginate(EventBusName=BUS):
        rules += page["Rules"]
    r.log("bus %s: %d rules" % (BUS, len(rules)))
    for rule in rules:
        tg = ev.list_targets_by_rule(Rule=rule["Name"], EventBusName=BUS).get("Targets", [])
        pat = rule.get("EventPattern") or ""
        r.log("  rule %s state=%s pattern=%s" % (rule["Name"], rule.get("State"), pat[:300]))
        for t in tg:
            r.log("     -> %s retry=%s dlq=%s input=%s" % (t["Arn"].split(":")[-1], t.get("RetryPolicy"),
                                                        bool(t.get("DeadLetterConfig")), bool(t.get("Input"))))
    ms = cw.list_metrics(Namespace="AWS/Events", Dimensions=[{"Name": "EventBusName", "Value": BUS}]).get("Metrics", [])
    r.log("bus metrics available: %s" % sorted({m["MetricName"] for m in ms}))
    for name in sorted({m["MetricName"] for m in ms}):
        n = dist(series(name, "Sum", T(2026, 9, 15), T(2026, 10, 12), 300, ns="AWS/Events",
                        dims=[{"Name": "EventBusName", "Value": BUS}]))
        st = dist(series(name, "Sum", T(2026, 10, 13), T(2026, 10, 14), 300, ns="AWS/Events",
                         dims=[{"Name": "EventBusName", "Value": BUS}]))
        r.log("  bus %s Sum/5min NORMAL %s | STORM(Oct 13) %s" % (name, json.dumps(n), json.dumps(st)))


def alarms(r):
    found = []
    for page in cw.get_paginator("describe_alarms").paginate():
        for a in page.get("MetricAlarms", []):
            name = a["AlarmName"].lower()
            if any(k in name for k in ("runaway", "spend", "governor", "invocation", "concurren", "budget", "cost",
                                       "throttl", "loop", "storm", "heartbeat", "dlq")):
                found.append(a)
    r.log("%d related alarms" % len(found))
    for a in found[:40]:
        r.log("  %s [%s] %s/%s %s %s %s x%s actions=%s" % (
            a["AlarmName"], a.get("StateValue"), a.get("Namespace"), a.get("MetricName"), a.get("Statistic"),
            a.get("ComparisonOperator"), a.get("Threshold"), a.get("EvaluationPeriods"),
            [x.split(":")[-1] for x in a.get("AlarmActions", [])]))


def anomaly_and_budgets(r):
    def mons():
        m = ce.get_anomaly_monitors().get("AnomalyMonitors", [])
        for x in m:
            r.log("anomaly monitor %s type=%s dim=%s arn=...%s" % (x.get("MonitorName"), x.get("MonitorType"),
                                                                x.get("MonitorDimension"), x["MonitorArn"][-12:]))
        subs = ce.get_anomaly_subscriptions().get("AnomalySubscriptions", [])
        for sb in subs:
            r.log("anomaly subscription %s freq=%s threshold=%s expr=%s subscribers=%s monitors=%d" % (
                sb.get("SubscriptionName"), sb.get("Frequency"), sb.get("Threshold"),
                json.dumps(sb.get("ThresholdExpression"))[:200],
                [(x.get("Type"), mask(x.get("Address")), x.get("Status")) for x in sb.get("Subscribers", [])],
                len(sb.get("MonitorArnList", []))))
        if not m:
            r.warn("no Cost Anomaly Detection monitor")
    guard("anomaly", r, mons)

    def bud():
        for b in budgets.describe_budgets(AccountId=ACCOUNT).get("Budgets", []):
            r.log("budget %s %s %s %s" % (b["BudgetName"], b.get("TimeUnit"), b.get("BudgetType"),
                                         (b.get("BudgetLimit") or {}).get("Amount")))
            for n in budgets.describe_notifications_for_budget(AccountId=ACCOUNT, BudgetName=b["BudgetName"]).get(
                    "Notifications", []):
                subs = budgets.describe_subscribers_for_notification(
                    AccountId=ACCOUNT, BudgetName=b["BudgetName"], Notification=n).get("Subscribers", [])
                r.log("   notify %s %s %s%% -> %s" % (n.get("NotificationType"), n.get("ComparisonOperator"),
                                                   n.get("Threshold"),
                                                   [(x["SubscriptionType"], mask(x["Address"])) for x in subs]))
            try:
                acts = budgets.describe_budget_actions_for_budget(AccountId=ACCOUNT, BudgetName=b["BudgetName"])
                for a in acts.get("Actions", []):
                    r.log("   action %s %s status=%s" % (a.get("ActionType"), a.get("ApprovalModel"), a.get("Status")))
            except Exception as e:  # noqa: BLE001
                r.log("   actions: %s" % type(e).__name__)
    guard("budgets", r, bud)


def topics(r):
    for page in sns.get_paginator("list_topics").paginate():
        for t in page["Topics"]:
            arn = t["TopicArn"]
            if not arn.split(":")[-1].startswith("justhodl"):
                continue
            subs = sns.list_subscriptions_by_topic(TopicArn=arn).get("Subscriptions", [])
            r.log("topic %s: %s" % (arn.split(":")[-1], [(x["Protocol"], mask(x["Endpoint"]),
                                                          "PENDING" if x["SubscriptionArn"] == "PendingConfirmation"
                                                          else "confirmed") for x in subs]))


def bucket(r):
    pre = s3.list_objects_v2(Bucket=BUCKET, Delimiter="/").get("CommonPrefixes", [])
    r.log("%s top-level prefixes: %s" % (BUCKET, [p["Prefix"] for p in pre]))
    r.log("versioning: %s" % s3.get_bucket_versioning(Bucket=BUCKET).get("Status"))
    for rule in s3.get_bucket_lifecycle_configuration(Bucket=BUCKET).get("Rules", []):
        f = rule.get("Filter") or {}
        r.log("  lifecycle %s [%s] prefix=%r noncurrent=%s newer_kept=%s expiration=%s abort_mpu=%s" % (
            rule.get("ID"), rule.get("Status"), f.get("Prefix", rule.get("Prefix", f.get("And", {}).get("Prefix"))),
            (rule.get("NoncurrentVersionExpiration") or {}).get("NoncurrentDays"),
            (rule.get("NoncurrentVersionExpiration") or {}).get("NewerNoncurrentVersions"),
            rule.get("Expiration"), bool(rule.get("AbortIncompleteMultipartUpload"))))
    for b in (BUCKET, "justhodl-ops-private-857687956942"):
        try:
            n = s3.get_bucket_notification_configuration(Bucket=b)
            r.log("%s notifications: lambda=%s sqs=%s sns=%s eventbridge=%s" % (
                b, [(c["LambdaFunctionArn"].split(":")[-1], c.get("Events"), json.dumps(c.get("Filter"))[:120])
                    for c in n.get("LambdaFunctionConfigurations", [])],
                len(n.get("QueueConfigurations", [])), len(n.get("TopicConfigurations", [])),
                "EventBridgeConfiguration" in n))
        except Exception as e:  # noqa: BLE001
            r.log("%s notifications: %s" % (b, type(e).__name__))
    sz = series("BucketSizeBytes", "Average", T(2026, 9, 1), T(2026, 10, 17), 86400, ns="AWS/S3",
                dims=[{"Name": "BucketName", "Value": BUCKET}, {"Name": "StorageType", "Value": "StandardStorage"}])
    r.log("BucketSizeBytes TB by day: %s" % [(t.strftime("%m-%d"), round(v / 1e12, 2)) for t, v in sz][-30:])


def role(r):
    name = "lambda-execution-role"
    att = iam.list_attached_role_policies(RoleName=name).get("AttachedPolicies", [])
    inl = iam.list_role_policies(RoleName=name).get("PolicyNames", [])
    r.log("%s attached=%s inline=%s" % (name, [a["PolicyName"] for a in att], inl))
    try:
        sim = iam.simulate_principal_policy(
            PolicySourceArn="arn:aws:iam::%s:role/%s" % (ACCOUNT, name),
            ActionNames=["dynamodb:UpdateItem", "dynamodb:GetItem", "sns:Publish", "dynamodb:CreateTable"],
            ResourceArns=["arn:aws:dynamodb:%s:%s:table/justhodl-event-breaker" % (REGION, ACCOUNT)])
        r.log("simulated on table justhodl-event-breaker: %s" % [(x["EvalActionName"], x["EvalDecision"])
                                                                for x in sim.get("EvaluationResults", [])])
    except Exception as e:  # noqa: BLE001
        r.log("simulate_principal_policy: %s" % type(e).__name__)
    gov = iam.list_role_policies(RoleName="justhodl-spend-governor-role").get("PolicyNames", [])
    r.log("justhodl-spend-governor-role inline=%s" % gov)


def lambda_side(r):
    try:
        acct = lam.get_account_settings()
        r.log("account concurrency limit=%s unreserved=%s" % (
            acct["AccountLimit"].get("ConcurrentExecutions"), acct["AccountLimit"].get("UnreservedConcurrentExecutions")))
    except Exception as e:  # noqa: BLE001
        r.log("account settings: %s" % type(e).__name__)
    esm = []
    for page in lam.get_paginator("list_event_source_mappings").paginate():
        esm += page["EventSourceMappings"]
    r.log("%d event source mappings: %s" % (len(esm), [(m["FunctionArn"].split(":")[-1],
                                                          m.get("EventSourceArn", "?").split(":")[-1][:40],
                                                          m.get("State"), (m.get("ScalingConfig") or {}).get(
                                                              "MaximumConcurrency")) for m in esm][:40]))
    for fn in [COORD] + TARGETS:
        try:
            rc = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
            try:
                eic = lam.get_function_event_invoke_config(FunctionName=fn)
                eic = (eic.get("MaximumRetryAttempts"), eic.get("MaximumEventAgeInSeconds"))
            except lam.exceptions.ResourceNotFoundException:
                eic = "default(2 retries, 6h)"
            r.log("  %-30s reserved=%s eic=%s" % (fn, rc, eic))
        except Exception as e:  # noqa: BLE001
            r.log("  %-30s %s" % (fn, type(e).__name__))
    names = []
    for page in lam.get_paginator("list_functions").paginate():
        names += [f["FunctionName"] for f in page["Functions"]]

    def rec(fn):
        try:
            return fn, lam.get_function_recursion_config(FunctionName=fn).get("RecursiveLoop")
        except Exception as e:  # noqa: BLE001
            return fn, type(e).__name__
    with cf.ThreadPoolExecutor(max_workers=6) as ex:
        res = list(ex.map(rec, names))
    c = Counter(v for _, v in res)
    r.log("%d functions; RecursiveLoop settings: %s; Allow on: %s" % (len(names), dict(c),
                                                                   [f for f, v in res if v == "Allow"][:30]))


def main():
    with report("ops_6534_guardrail_audit") as r:
        r.heading("ops 6534 - audit before the runaway guardrails (read-only)")
        r.section("1. Account Lambda profile, normal vs storm")
        guard("account", r, lambda: account_profile(r))
        r.section("2. Coordinator + route targets, Sep 1 - Oct 11")
        guard("targets", r, lambda: target_profile(r))
        r.section("3. Event bus")
        guard("bus", r, lambda: bus(r))
        r.section("4. Existing alarms")
        guard("alarms", r, lambda: alarms(r))
        r.section("5. Cost Anomaly Detection + Budgets")
        anomaly_and_budgets(r)
        r.section("6. SNS topics")
        guard("topics", r, lambda: topics(r))
        r.section("7. Bucket")
        guard("bucket", r, lambda: bucket(r))
        r.section("8. Coordinator role")
        guard("role", r, lambda: role(r))
        r.section("9. Lambda side")
        guard("lambda", r, lambda: lambda_side(r))
        if errors:
            r.section("Section errors")
            for k, v in errors.items():
                r.warn("%s: %s" % (k, v))
        if "account" in errors and "targets" in errors:
            r.fail("core calibration sections failed")
            sys.exit(1)
        r.ok("audit complete")
    sys.exit(0)


if __name__ == "__main__":
    main()
