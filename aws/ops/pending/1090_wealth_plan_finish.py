"""ops 1090 — fresh attempt at wealth-plan gap closure (ops 1088/1089 workflow stuck).

Same 3 actions:
  1. Add data/wealth-plan-snapshot.json to freshness manifest
  2. Create EventBridge rule wealth-plan-daily-warmup (cron 11:30 UTC daily)
  3. Test-invoke + verify
"""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-wealth-plan"
SNAPSHOT_KEY = "data/wealth-plan-snapshot.json"
MANIFEST_KEY = "data/_freshness-manifest.json"
RULE_NAME = "wealth-plan-daily-warmup"
CRON = "cron(30 11 ? * * *)"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())


def main():
    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    events = boto3.client("events", region_name=REGION)
    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    # 1. Freshness manifest
    m = json.loads(s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)["Body"].read())
    before = len(m.get("key_overrides", {}))
    m.setdefault("key_overrides", {})
    m["key_overrides"][SNAPSHOT_KEY] = {
        "max_age_h": 30,
        "description": "Wealth Plan default-profile snapshot — daily 11:30 UTC warmup + every Function URL hit. Alert if >30h stale.",
    }
    m["_last_updated"] = datetime.now(timezone.utc).isoformat()
    m["_last_updater"] = "ops/1090"
    s3.put_object(Bucket=BUCKET, Key=MANIFEST_KEY, Body=json.dumps(m, indent=2).encode(), ContentType="application/json")
    report["freshness"] = {"before": before, "after": len(m["key_overrides"]), "added": SNAPSHOT_KEY}

    # 2. EventBridge daily warmup
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{FN}"
    events.put_rule(Name=RULE_NAME, ScheduleExpression=CRON, State="ENABLED",
                    Description="Daily warmup for Wealth Plan default scenario.")
    events.put_targets(Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        lam.add_permission(FunctionName=FN, StatementId=f"AllowEB-{RULE_NAME}",
                           Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                           SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}")
        report["schedule"] = {"rule": RULE_NAME, "cron": CRON, "permission": "ADDED"}
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            report["schedule"] = {"rule": RULE_NAME, "cron": CRON, "permission": "EXISTS"}
        else:
            raise

    # 3. Test invoke
    try:
        ts_before = s3.get_object(Bucket=BUCKET, Key=SNAPSHOT_KEY)["LastModified"]
    except Exception:
        ts_before = None
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse")
    report["invoke_status"] = inv["StatusCode"]
    time.sleep(3)
    o = s3.get_object(Bucket=BUCKET, Key=SNAPSHOT_KEY)
    report["snapshot_refreshed"] = ts_before is None or o["LastModified"] > ts_before
    report["snapshot_size_kb"] = round(o["ContentLength"] / 1024, 1)

    # 4. Kick freshness monitor
    try:
        lam.invoke(FunctionName="justhodl-fleet-freshness-monitor", InvocationType="Event")
        report["monitor_kicked"] = True
    except Exception as e:
        report["monitor_kick_err"] = str(e)[:120]

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1090.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
