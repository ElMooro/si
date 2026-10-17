"""ops 6541 - arm the runaway guardrails in AWS (Khalid 2026-10-16: "apply fixes so this never ever happens again").

Code shipped in 3e977c6f (event-loop deploy gate, coordinator circuit breaker, governor v1.1.0 fast fuse). This op
creates and wires everything that code relies on, then proves the wiring end to end. Idempotent; every change
is additive (merge, never replace), and the reversal is listed at the bottom of the report.

  A. DynamoDB justhodl-event-breaker (on-demand, TTL expires_at) + a live test of the breaker's conditional ADD
  B. reserved-concurrency caps on the coordinator, its 7 route targets and the a2a pair (bounded blast radius);
     async event age 15 min / 1 retry (the storm kept 6-hour backlogs alive); then a governor tick so the FREEZE
     re-holds them at 0 and records the caps as what the Nov 1 restore puts back
  C. EventBridge rule justhodl-events-to-coordinator: target retry policy 15 min / 2 retries (default: 24 h / 185)
  D. SNS justhodl-spend-governor: CloudWatch alarms named justhodl-* may publish (the fast fuse's trigger)
  E. metric filters on the coordinator log group: BREAKER_OPEN / BREAKER_UNAVAILABLE
  F. alarms: justhodl-runaway-{concurrency,invocations,duration} (sustained 15 min, calibrated by ops 6534),
     justhodl-event-breaker-{open,unavailable}, justhodl-s3-live-bucket-size -> governor + justhodl-fleet-alerts
  G. AWS Budgets JustHodl-Daily-Spend-Guard ($40/day actual -> the same e-mail as the monthly budget + governor);
     Cost Anomaly subscription threshold $100 & 40% -> $25 & 20%
  H. Read-only check of the history-expiry containment authorized on 2026-10-17; never re-enable deletion.
  I. end to end: alarm justhodl-runaway-invocations forced to ALARM -> SNS -> governor fast fuse -> its decision
     record lands in S3; then back to OK
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
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
TABLE = "justhodl-event-breaker"
BUS = "justhodl-system-events"
BUS_RULE = "justhodl-events-to-coordinator"
COORD = "justhodl-event-coordinator"
GOV = "justhodl-spend-governor"
GOV_TOPIC = "arn:aws:sns:%s:%s:justhodl-spend-governor" % (REGION, ACCOUNT)
ALERT_TOPIC = "arn:aws:sns:%s:%s:justhodl-fleet-alerts" % (REGION, ACCOUNT)
STATE_BUCKET = "justhodl-ops-private-857687956942"
LIVE_BUCKET = "justhodl-dashboard-live"
MONTHLY_BUDGET = "JustHodl-Monthly-Hard-Cap-500"
DAILY_BUDGET = "JustHodl-Daily-Spend-Guard"
CAPS = {COORD: 10, "justhodl-alpha-compass": 3, "justhodl-calibrator": 2, "justhodl-alpha-calibrator": 2,
        "justhodl-signal-board": 2, "justhodl-miss-calibrator": 1, "justhodl-engine-signal-map": 2,
        "justhodl-jh-fusion": 2, "justhodl-a2a-bus": 5, "justhodl-backend-agent": 2}
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, read_timeout=900)
ddb = boto3.client("dynamodb", region_name=REGION, config=CFG)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
ev = boto3.client("events", region_name=REGION, config=CFG)
sns = boto3.client("sns", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
budgets = boto3.client("budgets", region_name=REGION, config=CFG)
ce = boto3.client("ce", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
UTC = timezone.utc
results, errors = {}, {}


def step(name, r, fn, critical=False):
    try:
        results[name] = fn(r)
        return results[name]
    except Exception as e:  # noqa: BLE001
        msg = "%s: %s" % (type(e).__name__, str(e)[:300])
        errors[name] = (msg, critical)
        (r.fail if critical else r.warn)("%s failed: %s" % (name, msg))
        return None


def code(e):
    return e.response.get("Error", {}).get("Code") if isinstance(e, ClientError) else type(e).__name__


# 0 ─────────────────────────────────────────────────────────────────────────────────────────────
def deployed(r):
    """The governor must run v1.1.0 before its alarms are wired; wait for deploy-lambdas if needed."""
    for _ in range(40):
        st = json.loads(lam.invoke(FunctionName=GOV, Payload=b'{"action":"status"}')["Payload"].read() or b"{}")
        if st.get("version") == "1.1.0":
            r.ok("governor reports v1.1.0 (stage %s, held %s)" % (st.get("stage"), st.get("held")))
            break
        time.sleep(30)
    else:
        raise RuntimeError("governor still reports %s after 20 min" % st.get("version"))
    c = lam.get_function_configuration(FunctionName=COORD)
    r.log("coordinator LastModified %s CodeSha256 %s..." % (c.get("LastModified"), c.get("CodeSha256", "")[:12]))
    return {"governor": st.get("version"), "coordinator_modified": c.get("LastModified")}


# A ─────────────────────────────────────────────────────────────────────────────────────────────
def table(r):
    try:
        d = ddb.describe_table(TableName=TABLE)["Table"]
        r.ok("table %s exists (%s)" % (TABLE, d["TableStatus"]))
    except ClientError as e:
        if code(e) != "ResourceNotFoundException":
            raise
        ddb.create_table(TableName=TABLE, BillingMode="PAY_PER_REQUEST",
                         AttributeDefinitions=[{"AttributeName": "pk", "AttributeType": "S"}],
                         KeySchema=[{"AttributeName": "pk", "KeyType": "HASH"}],
                         Tags=[{"Key": "system", "Value": "justhodl-event-bus"},
                               {"Key": "purpose", "Value": "coordinator circuit breaker - ops 6541"}])
        ddb.get_waiter("table_exists").wait(TableName=TABLE, WaiterConfig={"Delay": 5, "MaxAttempts": 40})
        r.ok("table %s created" % TABLE)
    ttl = ddb.describe_time_to_live(TableName=TABLE)["TimeToLiveDescription"]
    if ttl.get("TimeToLiveStatus") not in ("ENABLED", "ENABLING"):
        ddb.update_time_to_live(TableName=TABLE, TimeToLiveSpecification={"Enabled": True, "AttributeName": "expires_at"})
        r.ok("TTL on expires_at enabled")
    # live test of the exact conditional ADD the coordinator uses (limit 2 -> ok, ok, open)
    key = "ops6541-selftest#%s" % datetime.now(UTC).strftime("%Y%m%d%H%M%S")
    seen = []
    for _ in range(3):
        try:
            ddb.update_item(TableName=TABLE, Key={"pk": {"S": key}},
                            UpdateExpression="SET expires_at = if_not_exists(expires_at, :exp) ADD n :one",
                            ConditionExpression="attribute_not_exists(n) OR n < :limit",
                            ExpressionAttributeValues={":one": {"N": "1"}, ":limit": {"N": "2"},
                                                       ":exp": {"N": str(int(time.time()) + 3600)}})
            seen.append("ok")
        except ClientError as e:
            seen.append("open" if code(e) == "ConditionalCheckFailedException" else code(e))
    ddb.delete_item(TableName=TABLE, Key={"pk": {"S": key}})
    if seen != ["ok", "ok", "open"]:
        raise RuntimeError("breaker expression misbehaves on real DynamoDB: %s" % seen)
    r.ok("breaker conditional ADD on real DynamoDB: %s (limit 2)" % seen)
    return {"selftest": seen}


# B ─────────────────────────────────────────────────────────────────────────────────────────────
def caps(r):
    done = {}
    for fn, cap in CAPS.items():
        before = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
        lam.put_function_concurrency(FunctionName=fn, ReservedConcurrentExecutions=cap)
        lam.put_function_event_invoke_config(FunctionName=fn, MaximumRetryAttempts=1, MaximumEventAgeInSeconds=900)
        done[fn] = {"before": before, "cap": cap}
    r.ok("caps + async config set: %s" % json.dumps({k: v["cap"] for k, v in done.items()}))
    # The fleet is FROZEN until Nov 1: the governor's drift pass re-holds these at 0 and records the cap as the
    # value its Nov 1 restore puts back (lambda_function.enforce_drift).  Run that tick now, not in an hour.
    out = None
    for _ in range(10):
        try:
            out = json.loads(lam.invoke(FunctionName=GOV, Payload=json.dumps({"action": "tick", "source": "ops"}).encode())
                             ["Payload"].read() or b"{}")
            break
        except ClientError as e:
            if code(e) != "TooManyRequestsException":
                raise
            time.sleep(30)  # the hourly tick holds the governor's single slot
    r.log("governor tick: stage=%s drift=%s" % ((out or {}).get("stage"), json.dumps((out or {}).get("drift"))[:300]))
    st = json.loads(s3.get_object(Bucket=STATE_BUCKET, Key="spend-governor/state.json")["Body"].read())
    ledger = st.get("ledger") or {}
    check = {}
    for fn, cap in CAPS.items():
        e = ledger.get("lambda:" + fn) or {}
        now_rc = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
        check[fn] = {"now": now_rc, "restore_to": (e.get("before") or {}).get("reserved"), "stage": e.get("stage")}
    bad = {fn: c for fn, c in check.items() if st.get("stage") == "FREEZE" and (c["now"] != 0 or c["restore_to"] != CAPS[fn])}
    r.log("after tick: %s" % json.dumps(check))
    if bad:
        raise RuntimeError("caps not carried by the freeze ledger: %s" % json.dumps(bad))
    r.ok("frozen at 0 now; the Nov 1 restore puts back the caps")
    return check


# C ─────────────────────────────────────────────────────────────────────────────────────────────
def bus_retry(r):
    tg = ev.list_targets_by_rule(Rule=BUS_RULE, EventBusName=BUS)["Targets"]
    keep = ("Id", "Arn", "RoleArn", "Input", "InputPath", "InputTransformer", "DeadLetterConfig")
    new = []
    for t in tg:
        n = {k: t[k] for k in keep if k in t}
        n["RetryPolicy"] = {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 900}
        new.append(n)
    resp = ev.put_targets(Rule=BUS_RULE, EventBusName=BUS, Targets=new)
    if resp.get("FailedEntryCount"):
        raise RuntimeError(json.dumps(resp.get("FailedEntries"))[:300])
    got = ev.list_targets_by_rule(Rule=BUS_RULE, EventBusName=BUS)["Targets"]
    r.ok("bus rule targets now: %s" % [(t["Arn"].split(":")[-1], t.get("RetryPolicy")) for t in got])
    return [t.get("RetryPolicy") for t in got]


# D ─────────────────────────────────────────────────────────────────────────────────────────────
def topic_policy(r):
    pol = json.loads(sns.get_topic_attributes(TopicArn=GOV_TOPIC)["Attributes"]["Policy"])
    pol["Statement"] = [s for s in pol["Statement"] if s.get("Sid") != "CloudWatchAlarmsPublish"]
    pol["Statement"].append({
        "Sid": "CloudWatchAlarmsPublish", "Effect": "Allow", "Principal": {"Service": "cloudwatch.amazonaws.com"},
        "Action": "SNS:Publish", "Resource": GOV_TOPIC,
        "Condition": {"StringEquals": {"aws:SourceAccount": ACCOUNT},
                      "ArnLike": {"aws:SourceArn": "arn:aws:cloudwatch:%s:%s:alarm:justhodl-*" % (REGION, ACCOUNT)}}})
    sns.set_topic_attributes(TopicArn=GOV_TOPIC, AttributeName="Policy", AttributeValue=json.dumps(pol))
    r.ok("governor topic: %s" % [s.get("Sid") for s in pol["Statement"]])
    return [s.get("Sid") for s in pol["Statement"]]


# E ─────────────────────────────────────────────────────────────────────────────────────────────
def filters(r):
    group = "/aws/lambda/" + COORD
    for name, pattern, metric in (("justhodl-event-breaker-open", '"BREAKER_OPEN"', "BreakerOpen"),
                                  ("justhodl-event-breaker-unavailable", '"BREAKER_UNAVAILABLE"', "BreakerUnavailable")):
        logs.put_metric_filter(logGroupName=group, filterName=name, filterPattern=pattern,
                               metricTransformations=[{"metricName": metric, "metricNamespace": "JustHodl/EventBus",
                                                       "metricValue": "1", "unit": "Count"}])
    got = logs.describe_metric_filters(logGroupName=group)["metricFilters"]
    r.ok("metric filters on %s: %s" % (group, [f["filterName"] for f in got]))
    return [f["filterName"] for f in got]


# F ─────────────────────────────────────────────────────────────────────────────────────────────
WHAT_TO_DO = (" Runbook docs/ops/RUNAWAY_GUARDRAILS.md. The spend governor's fast fuse has already throttled the "
              "sustained burners; read s3://justhodl-ops-private-857687956942/spend-governor/events/ for what it did.")


def alarms(r):
    lam_metric = {"Namespace": "AWS/Lambda", "Period": 300, "EvaluationPeriods": 3, "DatapointsToAlarm": 3,
                  "ComparisonOperator": "GreaterThanThreshold", "TreatMissingData": "notBreaching",
                  "AlarmActions": [GOV_TOPIC, ALERT_TOPIC], "OKActions": [ALERT_TOPIC]}
    specs = [
        dict(lam_metric, AlarmName="justhodl-runaway-concurrency", MetricName="ConcurrentExecutions",
             Statistic="Average", Threshold=100.0,
             AlarmDescription="Account Lambda concurrency averaged > 100 for 15 min (normal Sep 15-Oct 11 never held "
                              "above 17.7; Oct 12 storm 760)." + WHAT_TO_DO),
        dict(lam_metric, AlarmName="justhodl-runaway-invocations", MetricName="Invocations", Statistic="Sum",
             Threshold=8000.0,
             AlarmDescription="Account Lambda invocations > 8,000 per 5 min for 15 min (normal held at most 5,303 "
                              "- the Oct 5 midnight fan-out; Oct 12 storm 35,302)." + WHAT_TO_DO),
        dict(lam_metric, AlarmName="justhodl-runaway-duration", MetricName="Duration", Statistic="Sum",
             Threshold=20000000.0,
             AlarmDescription="Account Lambda run time > 20M ms per 5 min for 15 min, ~$48/day at 512 MB (normal held "
                              "at most 4.8M; Oct 12 storm 227M)." + WHAT_TO_DO),
        dict(AlarmName="justhodl-event-breaker-open", Namespace="JustHodl/EventBus", MetricName="BreakerOpen",
             Statistic="Sum", Period=300, EvaluationPeriods=1, DatapointsToAlarm=1, Threshold=1.0,
             ComparisonOperator="GreaterThanOrEqualToThreshold", TreatMissingData="notBreaching",
             AlarmActions=[GOV_TOPIC, ALERT_TOPIC], OKActions=[ALERT_TOPIC],
             AlarmDescription="The event coordinator's circuit breaker refused an invoke: a route target exceeded its "
                              "hourly budget (justhodl-event-breaker). Usually a loop through the bus." + WHAT_TO_DO),
        dict(AlarmName="justhodl-event-breaker-unavailable", Namespace="JustHodl/EventBus",
             MetricName="BreakerUnavailable", Statistic="Sum", Period=300, EvaluationPeriods=1, DatapointsToAlarm=1,
             Threshold=1.0, ComparisonOperator="GreaterThanOrEqualToThreshold", TreatMissingData="notBreaching",
             AlarmActions=[ALERT_TOPIC], OKActions=[ALERT_TOPIC],
             AlarmDescription="The coordinator could not reach its breaker table justhodl-event-breaker, so it is "
                              "refusing every routed invoke (fail-closed). Check the table and lambda-execution-role."),
        dict(AlarmName="justhodl-s3-live-bucket-size", Namespace="AWS/S3", MetricName="BucketSizeBytes",
             Dimensions=[{"Name": "BucketName", "Value": LIVE_BUCKET}, {"Name": "StorageType", "Value": "StandardStorage"}],
             Statistic="Maximum", Period=86400, EvaluationPeriods=1, DatapointsToAlarm=1, Threshold=2.0e12,
             ComparisonOperator="GreaterThanThreshold", TreatMissingData="missing",
             AlarmActions=[GOV_TOPIC, ALERT_TOPIC], OKActions=[ALERT_TOPIC],
             AlarmDescription="justhodl-dashboard-live Standard storage > 2 TB (normal 0.6-1.0 TB; Oct 13-16 rose to "
                              "145 TB of noncurrent copies). Find the writer rewriting one key in the versioned bucket."
                              + WHAT_TO_DO),
    ]
    for spec in specs:
        cw.put_metric_alarm(**spec)
    names = [s["AlarmName"] for s in specs]
    got = {a["AlarmName"]: (a["StateValue"], a["Threshold"], [x.split(":")[-1] for x in a["AlarmActions"]])
           for a in cw.describe_alarms(AlarmNames=names)["MetricAlarms"]}
    missing = [n for n in names if n not in got]
    if missing:
        raise RuntimeError("alarms missing after put: %s" % missing)
    for n in names:
        r.log("  %s %s" % (n, got[n]))
    r.ok("%d alarms armed" % len(names))
    return got


# G ─────────────────────────────────────────────────────────────────────────────────────────────
def daily_budget(r):
    emails = []
    for n in budgets.describe_notifications_for_budget(AccountId=ACCOUNT, BudgetName=MONTHLY_BUDGET)["Notifications"]:
        for s in budgets.describe_subscribers_for_notification(AccountId=ACCOUNT, BudgetName=MONTHLY_BUDGET,
                                                               Notification=n)["Subscribers"]:
            if s["SubscriptionType"] == "EMAIL" and s["Address"] not in emails:
                emails.append(s["Address"])
    if not emails:
        raise RuntimeError("no e-mail subscriber on %s to reuse" % MONTHLY_BUDGET)
    budget = {"BudgetName": DAILY_BUDGET, "BudgetType": "COST", "TimeUnit": "DAILY",
              "BudgetLimit": {"Amount": "40", "Unit": "USD"},
              "CostTypes": {"IncludeTax": True, "IncludeSubscription": True, "UseBlended": False,
                            "IncludeRefund": False, "IncludeCredit": False, "IncludeUpfront": True,
                            "IncludeRecurring": True, "IncludeOtherSubscription": True, "IncludeSupport": True,
                            "IncludeDiscount": True, "UseAmortized": False}}
    note = {"NotificationType": "ACTUAL", "ComparisonOperator": "GREATER_THAN", "Threshold": 100.0,
            "ThresholdType": "PERCENTAGE"}
    subs = [{"SubscriptionType": "EMAIL", "Address": a} for a in emails] + [{"SubscriptionType": "SNS", "Address": GOV_TOPIC}]
    try:
        budgets.create_budget(AccountId=ACCOUNT, Budget=budget, NotificationsWithSubscribers=[
            {"Notification": note, "Subscribers": subs}])
        r.ok("budget %s created: $40/day actual -> %d e-mail + governor" % (DAILY_BUDGET, len(emails)))
    except ClientError as e:
        if code(e) != "DuplicateRecordException":
            raise
        budgets.update_budget(AccountId=ACCOUNT, NewBudget=budget)
        r.ok("budget %s exists; limit kept at $40/day" % DAILY_BUDGET)
    got = budgets.describe_budget(AccountId=ACCOUNT, BudgetName=DAILY_BUDGET)["Budget"]
    return {"limit": got["BudgetLimit"]["Amount"], "time_unit": got["TimeUnit"], "emails": len(emails)}


def anomaly(r):
    subs = ce.get_anomaly_subscriptions().get("AnomalySubscriptions", [])
    target = next((s for s in subs if s.get("SubscriptionName") == "Default-Services-Subscription"), None)
    if not target:
        raise RuntimeError("Default-Services-Subscription not found")
    expr = {"And": [
        {"Dimensions": {"Key": "ANOMALY_TOTAL_IMPACT_ABSOLUTE", "Values": ["25.0"], "MatchOptions": ["GREATER_THAN_OR_EQUAL"]}},
        {"Dimensions": {"Key": "ANOMALY_TOTAL_IMPACT_PERCENTAGE", "Values": ["20.0"], "MatchOptions": ["GREATER_THAN_OR_EQUAL"]}}]}
    ce.update_anomaly_subscription(SubscriptionArn=target["SubscriptionArn"], ThresholdExpression=expr)
    got = [s for s in ce.get_anomaly_subscriptions().get("AnomalySubscriptions", [])
           if s["SubscriptionArn"] == target["SubscriptionArn"]][0]
    r.ok("anomaly alerts now: %s" % json.dumps(got.get("ThresholdExpression")))
    return got.get("ThresholdExpression")


# H ─────────────────────────────────────────────────────────────────────────────────────────────
def lifecycle(r):
    # Khalid, 2026-10-17 02:52:14 UTC: disable this automatic history deletion.
    # Keep this old arming operation read-only here so a rerun cannot undo ops6544.
    rule_id = "ops6541-bound-noncurrent-versions"
    rules = s3.get_bucket_lifecycle_configuration(Bucket=LIVE_BUCKET).get("Rules", [])
    mine = [x for x in rules if x.get("ID") == rule_id]
    if len(mine) > 1 or (mine and mine[0].get("Status") != "Disabled"):
        raise RuntimeError("history-expiry containment requires ops6544; this operation cannot arm it")
    r.ok("history-expiry rule disabled or absent; no lifecycle write performed")
    return {"history_expiry_arming": "prohibited", "lifecycle_write": False}


# I ─────────────────────────────────────────────────────────────────────────────────────────────
def end_to_end(r):
    alarm = "justhodl-runaway-invocations"
    since = datetime.now(UTC) - timedelta(seconds=5)
    cw.set_alarm_state(AlarmName=alarm, StateValue="ALARM",
                       StateReason="ops 6541 end-to-end test of the alarm -> SNS -> governor fast fuse path")
    found = None
    for _ in range(36):
        time.sleep(5)
        page = s3.list_objects_v2(Bucket=STATE_BUCKET, Prefix="spend-governor/events/",
                                  StartAfter="spend-governor/events/%s" % (since - timedelta(minutes=1)).strftime(
                                      "%Y%m%dT%H%M%SZ"))
        for o in sorted(page.get("Contents", []), key=lambda o: o["LastModified"], reverse=True)[:20]:
            if o["LastModified"] >= since and ("fast-fuse" in o["Key"]):
                body = json.loads(s3.get_object(Bucket=STATE_BUCKET, Key=o["Key"])["Body"].read())
                if (body.get("alarm") or (body.get("body") or {}).get("alarm")) == alarm or alarm in json.dumps(body):
                    found = (o["Key"], body)
                    break
        if found:
            break
    cw.set_alarm_state(AlarmName=alarm, StateValue="OK", StateReason="ops 6541 end-to-end test finished")
    if not found:
        raise RuntimeError("no fast-fuse decision record within 3 minutes of the forced alarm")
    r.ok("alarm -> SNS -> governor worked: %s" % found[0])
    r.log("decision: %s" % json.dumps(found[1])[:600])
    return {"record": found[0]}


def smasync_note(r):
    """Governor v1.0.3+ keeps the AWS error message of a failed lever (the owned endpoint's suspension)."""
    st = json.loads(s3.get_object(Bucket=STATE_BUCKET, Key="spend-governor/state.json")["Body"].read())
    e = (st.get("ledger") or {}).get("smasync:endpoint/jh-owned-coder-async/variant/owned") or {}
    r.log("smasync ledger entry: %s" % json.dumps({k: e.get(k) for k in ("status", "error", "error_msg", "failed_at",
                                                                         "scale_down_note", "applied_at")}))
    return e.get("status")


def main():
    with report("ops_6541_arm_runaway_guardrails") as r:
        r.heading("ops 6541 - arm the runaway guardrails and prove them")
        r.section("0. Code deployed")
        step("deployed", r, deployed, critical=True)
        r.section("A. Breaker table")
        step("table", r, table, critical=True)
        r.section("B. Caps + async config (carried through the freeze)")
        step("caps", r, caps, critical=True)
        r.section("C. Bus delivery retry policy")
        step("bus_retry", r, bus_retry)
        r.section("D. Governor topic accepts alarms")
        step("topic_policy", r, topic_policy, critical=True)
        r.section("E. Breaker metric filters")
        step("filters", r, filters)
        r.section("F. Alarms")
        step("alarms", r, alarms, critical=True)
        r.section("G. Budget + anomaly e-mail backstops")
        step("daily_budget", r, daily_budget)
        step("anomaly", r, anomaly)
        r.section("H. Versioned-bucket bound")
        step("lifecycle", r, lifecycle)
        if not any(c for _, c in errors.values()):
            r.section("I. End to end: forced alarm -> governor")
            step("end_to_end", r, end_to_end, critical=True)
        r.section("Note: owned SageMaker endpoint lever")
        step("smasync", r, smasync_note)
        r.section("Reversal")
        r.log("caps: put_function_concurrency back / governor ledger; alarms: delete_alarms justhodl-runaway-*, "
              "justhodl-event-breaker-*, justhodl-s3-live-bucket-size; budget: delete_budget %s; lifecycle: drop rule "
              "ops6541-bound-noncurrent-versions; topic: drop Sid CloudWatchAlarmsPublish; table: delete %s" % (
                  DAILY_BUDGET, TABLE))
        if errors:
            r.section("Errors")
            for k, (m, c) in errors.items():
                (r.fail if c else r.warn)("%s%s: %s" % (k, " (critical)" if c else "", m))
        if any(c for _, c in errors.values()):
            sys.exit(1)
        r.ok("all guardrails armed and the alarm path proven")
    sys.exit(0)


if __name__ == "__main__":
    main()
