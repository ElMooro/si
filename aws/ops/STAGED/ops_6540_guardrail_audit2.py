"""ops 6540 - READ-ONLY follow-up audit for the runaway guardrails (after ops 6534).

  1. Did the existing guard alarms (justhodl-guard-lambda-concurrency, -invocations-1h) fire during Oct 12-16,
     and where did their notifications go?  (alarm history)
  2. Cost Anomaly subscription: full threshold expression
  3. Prefixes that might rely on S3 version history (backups/, audit-private/, _audit/, archive/, config/,
     state/, khalid/): sample keys, how many keys carry >1 version, dated-key or overwrite-in-place
  4. SES: identities, verification, production access (a confirmation-free alert e-mail path?)
  5. justhodl-a2a-bus / justhodl-backend-agent: activity, reserved concurrency, who set RecursiveLoop=Allow
  6. SNS topic policy of justhodl-spend-governor (alarms will need publish rights)
  7. CloudWatch Logs: does a metric filter exist on the coordinator log group
"""

from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import sys

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
BUCKET = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"})
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
ce = boto3.client("ce", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
sns = boto3.client("sns", region_name=REGION, config=CFG)
logs = boto3.client("logs", region_name=REGION, config=CFG)
UTC = timezone.utc
errors = {}


def mask(addr):
    s = str(addr or "")
    if "@" in s:
        local, _, dom = s.partition("@")
        return local[:1] + "***@" + dom
    return s


def guard(name, r, fn):
    try:
        return fn()
    except Exception as e:  # noqa: BLE001
        errors[name] = "%s: %s" % (type(e).__name__, str(e)[:240])
        r.warn("%s failed: %s" % (name, errors[name]))


def alarm_history(r):
    for name in ("justhodl-guard-lambda-concurrency", "justhodl-guard-lambda-invocations-1h"):
        a = cw.describe_alarms(AlarmNames=[name]).get("MetricAlarms", [])
        if a:
            a = a[0]
            r.log("%s: period=%s eval=%s threshold=%s actions=%s ok_actions=%s" % (
                name, a.get("Period"), a.get("EvaluationPeriods"), a.get("Threshold"), a.get("AlarmActions"),
                a.get("OKActions")))
        hist = cw.describe_alarm_history(AlarmName=name, HistoryItemType="StateUpdate",
                                         StartDate=datetime(2026, 10, 1, tzinfo=UTC), EndDate=datetime.now(UTC),
                                         MaxRecords=50).get("AlarmHistoryItems", [])
        r.log("  state changes since Oct 1: %s" % [(h["Timestamp"].strftime("%m-%d %H:%M"), h["HistorySummary"][:70])
                                                    for h in sorted(hist, key=lambda h: h["Timestamp"])][:20])
        acts = cw.describe_alarm_history(AlarmName=name, HistoryItemType="Action",
                                         StartDate=datetime(2026, 10, 1, tzinfo=UTC), EndDate=datetime.now(UTC),
                                         MaxRecords=20).get("AlarmHistoryItems", [])
        r.log("  actions since Oct 1: %s" % [(h["Timestamp"].strftime("%m-%d %H:%M"), h["HistorySummary"][:110])
                                             for h in sorted(acts, key=lambda h: h["Timestamp"])][:10])


def anomaly(r):
    for sb in ce.get_anomaly_subscriptions().get("AnomalySubscriptions", []):
        r.log("subscription %s arn=...%s freq=%s expr=%s" % (sb.get("SubscriptionName"), sb["SubscriptionArn"][-12:],
                                                           sb.get("Frequency"),
                                                           json.dumps(sb.get("ThresholdExpression"))))


def prefixes(r):
    for pre in ("backups/", "audit-private/", "_audit/", "archive/", "config/", "state/", "khalid/", "ops/",
                "_ops/", "learning/"):
        try:
            page = s3.list_object_versions(Bucket=BUCKET, Prefix=pre, MaxKeys=1000)
            vs = page.get("Versions", [])
            per = Counter(v["Key"] for v in vs)
            multi = {k: n for k, n in per.items() if n > 1}
            sample = sorted(per)[:4]
            newest = max((v["LastModified"] for v in vs), default=None)
            r.log("%-15s keys(sampled)=%4d multi-version=%4d max_versions=%3d truncated=%s newest=%s sample=%s" % (
                pre, len(per), len(multi), max(per.values(), default=0), page.get("IsTruncated"),
                newest.strftime("%Y-%m-%d") if newest else None, sample))
        except Exception as e:  # noqa: BLE001
            r.log("%-15s %s" % (pre, type(e).__name__))


def ses(r):
    v2 = boto3.client("sesv2", region_name=REGION, config=CFG)
    try:
        acct = v2.get_account()
        r.log("SES production access=%s sending=%s" % (acct.get("ProductionAccessEnabled"), acct.get("SendingEnabled")))
    except Exception as e:  # noqa: BLE001
        r.log("SES get_account: %s" % type(e).__name__)
    ids = v2.list_email_identities().get("EmailIdentities", [])
    r.log("SES identities: %s" % [(mask(i["IdentityName"]), i.get("IdentityType"), i.get("VerificationStatus"),
                                    i.get("SendingEnabled")) for i in ids])


def a2a(r):
    end = datetime.now(UTC)
    for fn in ("justhodl-a2a-bus", "justhodl-backend-agent"):
        pts = cw.get_metric_statistics(Namespace="AWS/Lambda", MetricName="Invocations",
                                       Dimensions=[{"Name": "FunctionName", "Value": fn}],
                                       StartTime=end - timedelta(days=30), EndTime=end, Period=86400,
                                       Statistics=["Sum"]).get("Datapoints", [])
        pts = sorted(pts, key=lambda p: p["Timestamp"])
        rc = lam.get_function_concurrency(FunctionName=fn).get("ReservedConcurrentExecutions")
        r.log("%s: reserved=%s invocations/day last 30d (non-zero)=%s" % (
            fn, rc, [(p["Timestamp"].strftime("%m-%d"), int(p["Sum"])) for p in pts if p["Sum"]][-12:]))
    ct = boto3.client("cloudtrail", region_name=REGION, config=CFG)
    try:
        evs = ct.lookup_events(LookupAttributes=[{"AttributeKey": "EventName",
                                                  "AttributeValue": "PutFunctionRecursionConfig"}],
                               MaxResults=20).get("Events", [])
        r.log("PutFunctionRecursionConfig events (90d): %s" % [
            (e["EventTime"].strftime("%Y-%m-%d"), e.get("Username"),
             [x.get("ResourceName") for x in e.get("Resources", [])]) for e in evs])
    except Exception as e:  # noqa: BLE001
        r.log("cloudtrail lookup: %s" % type(e).__name__)


def topic_policy(r):
    arn = "arn:aws:sns:%s:%s:justhodl-spend-governor" % (REGION, ACCOUNT)
    pol = sns.get_topic_attributes(TopicArn=arn)["Attributes"].get("Policy")
    r.log("governor topic policy: %s" % pol)
    arn2 = "arn:aws:sns:%s:%s:justhodl-fleet-alerts" % (REGION, ACCOUNT)
    pol2 = json.loads(sns.get_topic_attributes(TopicArn=arn2)["Attributes"].get("Policy") or "{}")
    r.log("fleet-alerts policy principals: %s" % [(s.get("Sid"), s.get("Principal"), s.get("Action"))
                                                  for s in pol2.get("Statement", [])])


def filters(r):
    for g in ("/aws/lambda/justhodl-event-coordinator", "/aws/lambda/justhodl-spend-governor"):
        f = logs.describe_metric_filters(logGroupName=g).get("metricFilters", [])
        r.log("%s metric filters: %s" % (g, [(x["filterName"], x["filterPattern"][:60]) for x in f]))


def main():
    with report("ops_6540_guardrail_audit2") as r:
        r.heading("ops 6540 - follow-up audit for the runaway guardrails (read-only)")
        r.section("1. Did the old guard alarms fire?")
        guard("alarms", r, lambda: alarm_history(r))
        r.section("2. Cost Anomaly subscription")
        guard("anomaly", r, lambda: anomaly(r))
        r.section("3. Prefixes that may rely on version history")
        guard("prefixes", r, lambda: prefixes(r))
        r.section("4. SES")
        guard("ses", r, lambda: ses(r))
        r.section("5. a2a pair")
        guard("a2a", r, lambda: a2a(r))
        r.section("6. SNS policies")
        guard("topics", r, lambda: topic_policy(r))
        r.section("7. Metric filters")
        guard("filters", r, lambda: filters(r))
        for k, v in errors.items():
            r.warn("%s: %s" % (k, v))
        if len(errors) >= 6:
            r.fail("nearly every section failed")
            sys.exit(1)
        r.ok("audit 2 complete")
    sys.exit(0)


if __name__ == "__main__":
    main()
