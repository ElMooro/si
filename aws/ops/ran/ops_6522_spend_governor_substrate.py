"""ops 6522 - substrate for justhodl-spend-governor (the $500/month hard cap), created BEFORE the engine deploys.

1. Private state bucket justhodl-ops-private-857687956942 (public access blocked, AES256, versioned,
   TLS-only, noncurrent versions expire after 30 days).
2. IAM role justhodl-spend-governor-role: Lambda trust + one least-privilege inline policy covering exactly
   the governor's reads (Budgets, Cost Explorer, CloudWatch) and levers (Lambda concurrency, event source
   mappings, SageMaker endpoints/jobs/notebooks/apps, Application Auto Scaling, EC2, RDS, App Runner, ECS),
   plus the X-Ray and DLQ rights deploy-lambdas requires at CreateFunction.
3. SNS topic justhodl-spend-governor that only AWS Budgets (this account) may publish to.
4. Budget JustHodl-Monthly-Hard-Cap-500 (created by another lane this morning, alerts only):
   end date 2027-10-01 -> open-ended (2087-06-15, the API's "no end"), the SNS topic added as the SNS
   subscriber of every notification, and a 90% ACTUAL notification (email + SNS) as the fifth and last slot.
   The budget amount is never changed here - it is Khalid's lever.
5. justhodl-fleet-alerts gets an email subscription for the budget's own email subscriber(s): the guard
   alarms (invocations / concurrency / S3 growth) were in ALARM all week with Telegram (401) as their only
   channel.  AWS sends one confirmation email; nothing reaches the inbox until it is clicked.
6. control.json (create-if-absent): mode "monitor" until ops 6523 has proven the engine live; October gets a
   cap override (the month was already ~$2.4k by the loop) so the rest of October is a runaway-only guard,
   and $500 applies from 2026-11-01.

The orphan IAM policy JustHodl-Budget-Exceeded-Deny (from the failed Budget Action) is reported, not touched.
"""

from datetime import datetime, timezone
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
STATE_BUCKET = "justhodl-ops-private-%s" % ACCOUNT
ROLE = "justhodl-spend-governor-role"
POLICY_NAME = "justhodl-spend-governor"
GOV_TOPIC = "justhodl-spend-governor"
ALERT_TOPIC_ARN = "arn:aws:sns:%s:%s:justhodl-fleet-alerts" % (REGION, ACCOUNT)
BUDGET = "JustHodl-Monthly-Hard-Cap-500"
NO_END = datetime(2087, 6, 15, tzinfo=timezone.utc)
OCT_CAP = 3500.0
OUT_JSON = ROOT / "aws" / "ops" / "reports" / "6522.json"
CFG = Config(retries={"max_attempts": 8, "mode": "adaptive"})


def c(name):
    return boto3.client(name, region_name=REGION, config=CFG)


TRUST = {"Version": "2012-10-17", "Statement": [
    {"Effect": "Allow", "Principal": {"Service": "lambda.amazonaws.com"}, "Action": "sts:AssumeRole"}]}

POLICY = {"Version": "2012-10-17", "Statement": [
    {"Sid": "Logs", "Effect": "Allow", "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
     "Resource": "arn:aws:logs:%s:%s:*" % (REGION, ACCOUNT)},
    {"Sid": "Xray", "Effect": "Allow", "Action": ["xray:PutTraceSegments", "xray:PutTelemetryRecords"], "Resource": "*"},
    {"Sid": "Dlq", "Effect": "Allow", "Action": "sqs:SendMessage",
     "Resource": "arn:aws:sqs:%s:%s:justhodl-dlq-default" % (REGION, ACCOUNT)},
    {"Sid": "SpendRead", "Effect": "Allow", "Action": ["budgets:ViewBudget", "ce:GetCostAndUsage",
                                                       "cloudwatch:GetMetricData", "cloudwatch:DescribeAlarms",
                                                       "cloudwatch:PutMetricData"], "Resource": "*"},
    {"Sid": "Notify", "Effect": "Allow", "Action": "sns:Publish", "Resource": ALERT_TOPIC_ARN},
    {"Sid": "StateBucket", "Effect": "Allow", "Action": "s3:ListBucket", "Resource": "arn:aws:s3:::" + STATE_BUCKET},
    {"Sid": "StateObjects", "Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject"],
     "Resource": "arn:aws:s3:::%s/spend-governor/*" % STATE_BUCKET},
    {"Sid": "LambdaLevers", "Effect": "Allow", "Action": [
        "lambda:ListFunctions", "lambda:GetFunctionConcurrency", "lambda:PutFunctionConcurrency",
        "lambda:DeleteFunctionConcurrency", "lambda:ListEventSourceMappings", "lambda:GetEventSourceMapping",
        "lambda:UpdateEventSourceMapping"], "Resource": "*"},
    {"Sid": "CanaryInvoke", "Effect": "Allow", "Action": "lambda:InvokeFunction",
     "Resource": "arn:aws:lambda:%s:%s:function:justhodl-spend-governor-canary*" % (REGION, ACCOUNT)},
    {"Sid": "SageMakerLevers", "Effect": "Allow", "Action": [
        "sagemaker:ListEndpoints", "sagemaker:DescribeEndpoint", "sagemaker:DescribeEndpointConfig",
        "sagemaker:ListTags", "sagemaker:AddTags", "sagemaker:DeleteEndpoint", "sagemaker:CreateEndpoint",
        "sagemaker:UpdateEndpointWeightsAndCapacities", "sagemaker:ListTrainingJobs", "sagemaker:StopTrainingJob",
        "sagemaker:ListProcessingJobs", "sagemaker:StopProcessingJob", "sagemaker:ListTransformJobs",
        "sagemaker:StopTransformJob", "sagemaker:ListHyperParameterTuningJobs",
        "sagemaker:StopHyperParameterTuningJob", "sagemaker:ListNotebookInstances",
        "sagemaker:DescribeNotebookInstance", "sagemaker:StopNotebookInstance", "sagemaker:StartNotebookInstance",
        "sagemaker:ListApps", "sagemaker:DeleteApp"], "Resource": "*"},
    {"Sid": "AutoscalingLevers", "Effect": "Allow", "Action": [
        "application-autoscaling:DescribeScalableTargets", "application-autoscaling:RegisterScalableTarget"],
     "Resource": "*"},
    {"Sid": "ComputeLevers", "Effect": "Allow", "Action": [
        "ec2:DescribeInstances", "ec2:StopInstances", "ec2:StartInstances",
        "rds:DescribeDBInstances", "rds:DescribeDBClusters", "rds:StopDBInstance", "rds:StartDBInstance",
        "rds:StopDBCluster", "rds:StartDBCluster",
        "apprunner:ListServices", "apprunner:DescribeService", "apprunner:PauseService", "apprunner:ResumeService",
        "ecs:ListClusters", "ecs:ListServices", "ecs:DescribeServices", "ecs:UpdateService"], "Resource": "*"},
]}


def ensure_bucket(r):
    s3 = c("s3")
    try:
        s3.head_bucket(Bucket=STATE_BUCKET)
        r.log("bucket %s exists" % STATE_BUCKET)
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("404", "NoSuchBucket", "NotFound"):
            raise
        s3.create_bucket(Bucket=STATE_BUCKET)
        r.ok("bucket %s created" % STATE_BUCKET)
    s3.put_public_access_block(Bucket=STATE_BUCKET, PublicAccessBlockConfiguration={
        "BlockPublicAcls": True, "IgnorePublicAcls": True, "BlockPublicPolicy": True, "RestrictPublicBuckets": True})
    s3.put_bucket_encryption(Bucket=STATE_BUCKET, ServerSideEncryptionConfiguration={
        "Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]})
    s3.put_bucket_versioning(Bucket=STATE_BUCKET, VersioningConfiguration={"Status": "Enabled"})
    s3.put_bucket_policy(Bucket=STATE_BUCKET, Policy=json.dumps({"Version": "2012-10-17", "Statement": [
        {"Sid": "TlsOnly", "Effect": "Deny", "Principal": "*", "Action": "s3:*",
         "Resource": ["arn:aws:s3:::" + STATE_BUCKET, "arn:aws:s3:::%s/*" % STATE_BUCKET],
         "Condition": {"Bool": {"aws:SecureTransport": "false"}}}]}))
    s3.put_bucket_lifecycle_configuration(Bucket=STATE_BUCKET, LifecycleConfiguration={"Rules": [
        {"ID": "noncurrent-30d", "Status": "Enabled", "Filter": {"Prefix": ""},
         "NoncurrentVersionExpiration": {"NoncurrentDays": 30},
         "AbortIncompleteMultipartUpload": {"DaysAfterInitiation": 1}}]})
    pab = s3.get_public_access_block(Bucket=STATE_BUCKET)["PublicAccessBlockConfiguration"]
    return all(pab.values())


def ensure_role(r):
    iam = c("iam")
    try:
        iam.get_role(RoleName=ROLE)
        iam.update_assume_role_policy(RoleName=ROLE, PolicyDocument=json.dumps(TRUST))
        r.log("role %s exists - trust re-applied" % ROLE)
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
        iam.create_role(RoleName=ROLE, AssumeRolePolicyDocument=json.dumps(TRUST),
                        Description="justhodl-spend-governor: monthly AWS spend cap enforcement (ops 6522)",
                        Tags=[{"Key": "jh:owner", "Value": "spend-governor"}])
        r.ok("role %s created" % ROLE)
    doc = json.dumps(POLICY, separators=(",", ":"))
    iam.put_role_policy(RoleName=ROLE, PolicyName=POLICY_NAME, PolicyDocument=doc)
    got = iam.get_role_policy(RoleName=ROLE, PolicyName=POLICY_NAME)["PolicyDocument"]
    return {"policy_bytes": len(doc), "statements": len(got.get("Statement", []))}


def ensure_gov_topic(r):
    sns = c("sns")
    arn = sns.create_topic(Name=GOV_TOPIC, Tags=[{"Key": "jh:owner", "Value": "spend-governor"}])["TopicArn"]
    policy = {"Version": "2012-10-17", "Statement": [
        {"Sid": "Owner", "Effect": "Allow", "Principal": {"AWS": "arn:aws:iam::%s:root" % ACCOUNT},
         "Action": ["SNS:Publish", "SNS:Subscribe", "SNS:GetTopicAttributes", "SNS:SetTopicAttributes",
                    "SNS:ListSubscriptionsByTopic", "SNS:DeleteTopic"], "Resource": arn},
        {"Sid": "BudgetsPublish", "Effect": "Allow", "Principal": {"Service": "budgets.amazonaws.com"},
         "Action": "SNS:Publish", "Resource": arn,
         "Condition": {"StringEquals": {"aws:SourceAccount": ACCOUNT},
                       "ArnLike": {"aws:SourceArn": "arn:aws:budgets::%s:*" % ACCOUNT}}}]}
    sns.set_topic_attributes(TopicArn=arn, AttributeName="Policy", AttributeValue=json.dumps(policy))
    r.ok("topic %s ready (Budgets may publish)" % arn)
    return arn


def harden_budget(r, topic_arn):
    b = c("budgets")
    bud = b.describe_budget(AccountId=ACCOUNT, BudgetName=BUDGET)["Budget"]
    res = {"limit": bud["BudgetLimit"], "actual": (bud.get("CalculatedSpend") or {}).get("ActualSpend"),
           "end_before": str((bud.get("TimePeriod") or {}).get("End"))}
    end = (bud.get("TimePeriod") or {}).get("End")
    if end is None or end.year < 2087:
        nb = {k: bud[k] for k in ("BudgetName", "BudgetLimit", "CostFilters", "CostTypes", "TimeUnit", "BudgetType")
              if k in bud and bud[k] not in (None, {})}
        nb["TimePeriod"] = {"Start": bud["TimePeriod"]["Start"], "End": NO_END}
        b.update_budget(AccountId=ACCOUNT, NewBudget=nb)
        r.ok("budget end date %s -> open-ended" % res["end_before"])
    notes = b.describe_notifications_for_budget(AccountId=ACCOUNT, BudgetName=BUDGET).get("Notifications", [])
    emails, added = set(), []
    for n in notes:
        subs = b.describe_subscribers_for_notification(AccountId=ACCOUNT, BudgetName=BUDGET, Notification=n)["Subscribers"]
        emails |= {s["Address"] for s in subs if s["SubscriptionType"] == "EMAIL"}
        sns_subs = [s for s in subs if s["SubscriptionType"] == "SNS"]
        if not sns_subs:
            b.create_subscriber(AccountId=ACCOUNT, BudgetName=BUDGET, Notification=n,
                                Subscriber={"SubscriptionType": "SNS", "Address": topic_arn})
            added.append("%s %s%%" % (n["NotificationType"], n["Threshold"]))
        elif sns_subs[0]["Address"] != topic_arn:
            r.warn("notification %s %s already has SNS %s - left alone" % (n["NotificationType"], n["Threshold"], sns_subs[0]["Address"]))
    r.ok("SNS subscriber added to: %s" % (", ".join(added) or "none needed"))
    have90 = any(n["NotificationType"] == "ACTUAL" and float(n["Threshold"]) == 90.0 for n in notes)
    if not have90 and len(notes) < 5:
        subs = [{"SubscriptionType": "EMAIL", "Address": a} for a in sorted(emails)][:10]
        subs.append({"SubscriptionType": "SNS", "Address": topic_arn})
        b.create_notification(AccountId=ACCOUNT, BudgetName=BUDGET, Notification={
            "NotificationType": "ACTUAL", "ComparisonOperator": "GREATER_THAN", "Threshold": 90.0,
            "ThresholdType": "PERCENTAGE"}, Subscribers=subs)
        r.ok("90%% ACTUAL notification added (email + SNS)")
    after = b.describe_budget(AccountId=ACCOUNT, BudgetName=BUDGET)["Budget"]
    res.update({"end_after": str(after["TimePeriod"].get("End")), "limit_after": after["BudgetLimit"],
                "notifications": len(b.describe_notifications_for_budget(AccountId=ACCOUNT, BudgetName=BUDGET)["Notifications"]),
                "sns_added_to": added, "n_emails": len(emails)})
    return res, sorted(emails)


def ensure_email_channel(r, emails):
    sns = c("sns")
    subs = sns.list_subscriptions_by_topic(TopicArn=ALERT_TOPIC_ARN).get("Subscriptions", [])
    have = {s["Endpoint"]: s["SubscriptionArn"] for s in subs if s["Protocol"] == "email"}
    out = {}
    for e in emails:
        if e in have:
            out["%s***" % e[:1]] = "confirmed" if have[e] != "PendingConfirmation" else "pending"
            continue
        sns.subscribe(TopicArn=ALERT_TOPIC_ARN, Protocol="email", Endpoint=e)
        out["%s***" % e[:1]] = "confirmation email sent"
    r.ok("fleet-alerts email channel: %s" % json.dumps(out))
    return out


def ensure_control(r):
    s3 = c("s3")
    key = "spend-governor/control.json"
    try:
        cur = json.loads(s3.get_object(Bucket=STATE_BUCKET, Key=key)["Body"].read())
        r.log("control.json exists - left as is (mode=%s)" % cur.get("mode"))
        return cur
    except ClientError as e:
        if e.response["Error"]["Code"] not in ("NoSuchKey", "404"):
            raise
    control = {
        "mode": "monitor",
        "exempt_functions": ["justhodl-guardrail-notify", "justhodl-supabase-keepalive", "justhodl-dr-snapshot"],
        "exempt_prefixes": [],
        "brake_always": ["justhodl-ai", "justhodl-student-rsi", "justhodl-factory-grader"],
        "cap_overrides": {"2026-10": {"cap_usd": OCT_CAP, "reason": (
            "October was already ~$2.4k when the cap was armed (ops 6519/6520: miss-calibrator <-> "
            "event-coordinator loop). The override keeps the rest of October a runaway-only guard; "
            "the $500 budget applies from 2026-11-01.")}},
        "thresholds": {},
        "fuse_interval_h": 6,
        "written_by": "ops 6522",
        "written_at": datetime.now(timezone.utc).isoformat(),
    }
    s3.put_object(Bucket=STATE_BUCKET, Key=key, Body=json.dumps(control, indent=1).encode(),
                  ContentType="application/json", ServerSideEncryption="AES256")
    r.ok("control.json written (mode=monitor, Oct override $%.0f)" % OCT_CAP)
    return control


def main():
    out = {"op": 6522, "ran_at": datetime.now(timezone.utc).isoformat()}
    with report("ops_6522_spend_governor_substrate") as r:
        r.heading("ops 6522 - spend governor substrate")
        failed = []
        steps = (("bucket", lambda: ensure_bucket(r)), ("role", lambda: ensure_role(r)))
        for name, fn in steps:
            try:
                out[name] = fn()
            except Exception as e:  # noqa: BLE001
                out[name] = "ERR %s" % e
                failed.append(name)
                r.fail("%s: %s" % (name, e))
        try:
            topic = ensure_gov_topic(r)
            out["gov_topic"] = topic
            out["budget"], emails = harden_budget(r, topic)
            out["email_channel"] = ensure_email_channel(r, emails)
        except Exception as e:  # noqa: BLE001
            failed.append("budget/sns")
            r.fail("budget/sns: %s" % e)
        if "bucket" not in failed:
            try:
                out["control"] = {k: v for k, v in ensure_control(r).items() if k != "cap_overrides"}
            except Exception as e:  # noqa: BLE001
                failed.append("control")
                r.fail("control: %s" % e)
        try:
            ent = c("iam").list_entities_for_policy(PolicyArn="arn:aws:iam::%s:policy/JustHodl-Budget-Exceeded-Deny" % ACCOUNT)
            out["orphan_deny_policy_attachments"] = sum(len(ent.get(k, [])) for k in ("PolicyRoles", "PolicyUsers", "PolicyGroups"))
            r.log("orphan policy JustHodl-Budget-Exceeded-Deny attachments: %s (left untouched)" % out["orphan_deny_policy_attachments"])
        except ClientError:
            pass
        time.sleep(10)  # IAM propagation before the engine push creates the function
        out["role_visible"] = bool(c("iam").get_role(RoleName=ROLE))
        OUT_JSON.write_text(json.dumps(out, indent=1, default=str))
        r.ok("wrote %s" % OUT_JSON.relative_to(ROOT))
        if failed:
            r.fail("failed steps: %s" % failed)
            sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
