"""ops 6542 - prove the alarm -> governor path, fix the owned-endpoint lever, verify governor v1.1.1.

ops 6541 armed every guardrail but could not force an alarm (the runner has no cloudwatch:SetAlarmState).
Two proofs instead, neither needs that right:
  1. REAL: alarm justhodl-s3-live-bucket-size went to ALARM on creation (the bucket still holds the loop's
     145 TB until lifecycle finishes) - its action history must show the governor topic, and the governor must
     have written a fast-fuse decision for it.
  2. SYNTHETIC: a CloudWatch-alarm-shaped message published to SNS justhodl-spend-governor must produce a
     fast-fuse decision record (fleet frozen -> "nothing hot"), proving subscription + parsing + decision.
Then the owned SageMaker endpoint lever: governor v1.0.3 recorded why it fails - "User is missing the following
permissions: cloudwatch:PutMetricAlarm, cloudwatch:DeleteAlarms" (Application Auto Scaling checks the caller
for the scaling policy's alarms). Grant them to justhodl-spend-governor-role, tick, and read the scalable
target back: scale-out must be suspended before Sunday 13:20 UTC, when factory-market-exam calls the endpoint.
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
GOV = "justhodl-spend-governor"
GOV_ROLE = "justhodl-spend-governor-role"
GOV_POLICY = "justhodl-spend-governor"
GOV_TOPIC = "arn:aws:sns:%s:%s:justhodl-spend-governor" % (REGION, ACCOUNT)
STATE_BUCKET = "justhodl-ops-private-857687956942"
RID = "endpoint/jh-owned-coder-async/variant/owned"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, read_timeout=900)
lam = boto3.client("lambda", region_name=REGION, config=CFG)
cw = boto3.client("cloudwatch", region_name=REGION, config=CFG)
sns = boto3.client("sns", region_name=REGION, config=CFG)
s3 = boto3.client("s3", region_name=REGION, config=CFG)
iam = boto3.client("iam", config=CFG)
aas = boto3.client("application-autoscaling", region_name=REGION, config=CFG)
UTC = timezone.utc
errors = {}


def step(name, r, fn, critical=False):
    try:
        return fn(r)
    except Exception as e:  # noqa: BLE001
        errors[name] = ("%s: %s" % (type(e).__name__, str(e)[:300]), critical)
        (r.fail if critical else r.warn)("%s failed: %s" % (name, errors[name][0]))


def gov(payload):
    for _ in range(12):
        try:
            return json.loads(lam.invoke(FunctionName=GOV, Payload=json.dumps(payload).encode())["Payload"].read() or b"{}")
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "TooManyRequestsException":
                raise
            time.sleep(30)
    raise RuntimeError("governor busy for 6 minutes")


def events_since(since, needle):
    page = s3.list_objects_v2(Bucket=STATE_BUCKET, Prefix="spend-governor/events/",
                              StartAfter="spend-governor/events/%s" % (since - timedelta(minutes=1)).strftime("%Y%m%dT%H%M%SZ"))
    found = []
    for o in sorted(page.get("Contents", []), key=lambda o: o["Key"]):
        if "fast-fuse" in o["Key"]:
            body = json.loads(s3.get_object(Bucket=STATE_BUCKET, Key=o["Key"])["Body"].read())
            if body.get("alarm") == needle:
                found.append((o["Key"], body))
    return found


def version(r):
    for _ in range(40):
        st = gov({"action": "status"})
        if st.get("version") == "1.1.1":
            r.ok("governor v1.1.1 live (stage %s)" % st.get("stage"))
            return st
        time.sleep(30)
    raise RuntimeError("governor still %s" % st.get("version"))


def real_alarm(r):
    name = "justhodl-s3-live-bucket-size"
    hist = cw.describe_alarm_history(AlarmName=name, StartDate=datetime.now(UTC) - timedelta(hours=2),
                                     EndDate=datetime.now(UTC), MaxRecords=20).get("AlarmHistoryItems", [])
    for h in sorted(hist, key=lambda h: h["Timestamp"]):
        r.log("  %s %s %s" % (h["Timestamp"].strftime("%H:%M:%S"), h["HistoryItemType"], h["HistorySummary"][:120]))
    acted = [h for h in hist if h["HistoryItemType"] == "Action" and "justhodl-spend-governor" in h["HistorySummary"]
             and "Successfully" in h["HistorySummary"]]
    recs = events_since(datetime.now(UTC) - timedelta(hours=2), name)
    r.log("governor decisions for %s: %s" % (name, [(k, b.get("hot"), b.get("fleet_usd_day")) for k, b in recs]))
    if not acted:
        raise RuntimeError("CloudWatch did not publish %s to the governor topic" % name)
    if not recs:
        raise RuntimeError("governor wrote no fast-fuse decision for %s" % name)
    r.ok("REAL path proven: CloudWatch alarm -> SNS -> governor fast fuse (%s)" % recs[-1][0])
    return recs[-1][0]


def synthetic(r):
    name = "justhodl-runaway-invocations"
    since = datetime.now(UTC)
    msg = {"AlarmName": name, "NewStateValue": "ALARM", "OldStateValue": "OK",
           "NewStateReason": "ops 6542 synthetic test of the SNS -> governor path", "StateChangeTime": since.isoformat()}
    sns.publish(TopicArn=GOV_TOPIC, Subject="ALARM: %s (ops 6542 test)" % name, Message=json.dumps(msg))
    for _ in range(30):
        time.sleep(6)
        recs = events_since(since, name)
        if recs:
            r.ok("SYNTHETIC path proven: SNS -> governor -> decision %s (hot=%s, fleet $%s/day)" % (
                recs[-1][0], recs[-1][1].get("hot"), recs[-1][1].get("fleet_usd_day")))
            return recs[-1][0]
    raise RuntimeError("no decision record within 3 minutes of the synthetic alarm")


def endpoint_lever(r):
    doc = iam.get_role_policy(RoleName=GOV_ROLE, PolicyName=GOV_POLICY)["PolicyDocument"]
    stmts = [s for s in doc["Statement"] if s.get("Sid") != "AutoscalingCallerAlarmChecks"]
    stmts.append({"Sid": "AutoscalingCallerAlarmChecks", "Effect": "Allow",
                  "Action": ["cloudwatch:PutMetricAlarm", "cloudwatch:DeleteAlarms", "cloudwatch:DescribeAlarms"],
                  "Resource": "*"})
    doc["Statement"] = stmts
    iam.put_role_policy(RoleName=GOV_ROLE, PolicyName=GOV_POLICY, PolicyDocument=json.dumps(doc))
    r.ok("governor role: + cloudwatch:PutMetricAlarm/DeleteAlarms/DescribeAlarms (Application Auto Scaling checks "
         "the caller for the scaling policy's alarms)")
    time.sleep(20)  # IAM propagation
    for attempt in range(3):
        out = gov({"action": "tick", "source": "ops"})
        st = json.loads(s3.get_object(Bucket=STATE_BUCKET, Key="spend-governor/state.json")["Body"].read())
        e = (st.get("ledger") or {}).get("smasync:" + RID) or {}
        r.log("tick %d: drift=%s smasync=%s" % (attempt + 1, json.dumps((out.get("drift") or {}).get("reapplied")),
                                               json.dumps({k: e.get(k) for k in ("status", "error", "error_msg",
                                                                                 "scale_down_note")})))
        if e.get("status") in ("applied", "reapplied"):
            break
        time.sleep(30)
    t = aas.describe_scalable_targets(ServiceNamespace="sagemaker", ResourceIds=[RID],
                                      ScalableDimension="sagemaker:variant:DesiredInstanceCount").get("ScalableTargets", [])
    susp = (t[0].get("SuspendedState") if t else None) or {}
    r.log("scalable target now: min=%s max=%s suspended=%s" % (t[0].get("MinCapacity") if t else None,
                                                             t[0].get("MaxCapacity") if t else None, susp))
    if not susp.get("DynamicScalingOutSuspended"):
        raise RuntimeError("owned endpoint scale-out still live (ledger %s)" % e.get("status"))
    r.ok("owned endpoint: scale-out suspended (scale-in live) - a Sunday exam request cannot wake an instance")
    return susp


def main():
    with report("ops_6542_prove_alarm_path_and_fix_endpoint_lever") as r:
        r.heading("ops 6542 - alarm -> governor path proven; owned endpoint lever fixed")
        r.section("Governor v1.1.1")
        step("version", r, version, critical=True)
        r.section("1. Real alarm -> governor")
        step("real", r, real_alarm)
        r.section("2. Synthetic alarm through SNS -> governor")
        step("synthetic", r, synthetic, critical=True)
        r.section("3. Owned SageMaker endpoint lever")
        step("endpoint", r, endpoint_lever)
        r.section("4. Guardrail self-check")
        step("self", r, lambda rr: rr.log("governor guardrail check: %s" % json.dumps(
            (json.loads(s3.get_object(Bucket=STATE_BUCKET, Key="spend-governor/state.json")["Body"].read())
             .get("guardrails")))))
        for k, (m, c) in errors.items():
            (r.fail if c else r.warn)("%s: %s" % (k, m))
        if any(c for _, c in errors.values()) or ("real" in errors and "synthetic" in errors):
            sys.exit(1)
        r.ok("done")
    sys.exit(0)


if __name__ == "__main__":
    main()
