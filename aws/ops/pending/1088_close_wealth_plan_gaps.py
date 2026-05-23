"""ops 1088 — close remaining audit gaps:
   1. Add data/wealth-plan-snapshot.json to freshness manifest
   2. Add daily EventBridge schedule to keep snapshot fresh + warm container
      (Lambda is interactive-only today; if no user hits the URL for days,
      snapshot goes stale and a freshness alert is impossible)
   3. Verify the schedule actually invokes the Lambda (test invoke + read S3
      LastModified delta)
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
CRON = "cron(30 11 ? * * *)"  # daily 11:30 UTC (after macro-calendar at 11:00)
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())


def main():
    s3 = boto3.client("s3", region_name=REGION)
    lam = boto3.client("lambda", region_name=REGION)
    events = boto3.client("events", region_name=REGION)

    report = {"started_at": datetime.now(timezone.utc).isoformat()}

    # ── Step 1: Freshness manifest ─────────────────────────────────────
    print("1) Updating freshness manifest...")
    try:
        m_obj = s3.get_object(Bucket=BUCKET, Key=MANIFEST_KEY)
        manifest = json.loads(m_obj["Body"].read())
    except Exception as e:
        report["manifest_load_err"] = str(e)[:120]
        manifest = {"key_overrides": {}}

    before = len(manifest.get("key_overrides", {}))
    manifest.setdefault("key_overrides", {})
    manifest["key_overrides"][SNAPSHOT_KEY] = {
        "max_age_h": 30,  # 30h: daily warmup at 11:30 UTC + 6h grace
        "description": "Wealth Plan default-profile snapshot — refreshed by daily warmup + every Function URL hit. Alert if >30h stale (warmup failure).",
    }
    manifest["_last_updated"] = datetime.now(timezone.utc).isoformat()
    manifest["_last_updater"] = "ops/1088"

    s3.put_object(
        Bucket=BUCKET,
        Key=MANIFEST_KEY,
        Body=json.dumps(manifest, indent=2).encode(),
        ContentType="application/json",
    )
    after = len(manifest["key_overrides"])
    report["freshness"] = {"before": before, "after": after, "added": SNAPSHOT_KEY}

    # ── Step 2: EventBridge daily warmup ───────────────────────────────
    print("2) Creating EventBridge schedule + wiring permission...")
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{FN}"
    events.put_rule(
        Name=RULE_NAME,
        ScheduleExpression=CRON,
        State="ENABLED",
        Description="Daily warmup for Wealth Plan: refreshes snapshot + warms container so user-facing latency stays low.",
    )
    events.put_targets(Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        lam.add_permission(
            FunctionName=FN,
            StatementId=f"AllowEB-{RULE_NAME}",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}",
        )
        report["schedule"] = {"rule": RULE_NAME, "cron": CRON, "permission": "ADDED"}
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            report["schedule"] = {"rule": RULE_NAME, "cron": CRON, "permission": "EXISTS"}
        else:
            raise

    # ── Step 3: Test invoke + verify snapshot timestamp moved ──────────
    print("3) Test-invoking + reading snapshot...")
    try:
        o_before = s3.get_object(Bucket=BUCKET, Key=SNAPSHOT_KEY)
        ts_before = o_before["LastModified"]
    except Exception:
        ts_before = None

    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse", LogType="Tail")
    report["invoke_status"] = inv["StatusCode"]
    report["fn_err"] = inv.get("FunctionError")

    time.sleep(3)
    try:
        o_after = s3.get_object(Bucket=BUCKET, Key=SNAPSHOT_KEY)
        ts_after = o_after["LastModified"]
        report["snapshot_refresh"] = {
            "before_lm": ts_before.isoformat() if ts_before else None,
            "after_lm": ts_after.isoformat(),
            "refreshed": ts_before is None or ts_after > ts_before,
            "size_kb": round(o_after["ContentLength"] / 1024, 1),
        }
        body = json.loads(o_after["Body"].read())
        report["snapshot_verdict"] = (body.get("verdict") or {}).get("status")
        report["snapshot_prob_success"] = (body.get("monte_carlo") or {}).get("probability_of_success")
    except Exception as e:
        report["snapshot_check_err"] = str(e)[:200]

    # ── Step 4: Kick freshness monitor to consume new manifest ─────────
    print("4) Kicking freshness monitor...")
    try:
        inv2 = lam.invoke(FunctionName="justhodl-fleet-freshness-monitor", InvocationType="Event")
        report["monitor_kick"] = inv2["StatusCode"]
    except Exception as e:
        report["monitor_kick_err"] = str(e)[:120]

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    out = os.path.join(REPO_ROOT, "aws/ops/reports/1088.json")
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
