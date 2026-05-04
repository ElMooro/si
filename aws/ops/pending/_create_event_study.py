"""
Create justhodl-event-study Lambda + daily schedule + smoke test.
"""
import os
import time
import zipfile
import io
import json
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-event-study"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-event-study/source"
FRED_KEY = "2f057499936072679d8843d7fce99989"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def make_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(SOURCE_DIR):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, SOURCE_DIR)
                zf.write(full, rel)
    buf.seek(0)
    return buf.read()


def main():
    with report("create_event_study") as r:
        r.heading("Create justhodl-event-study + smoke test")

        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")

        try:
            lam.get_function(FunctionName=LAMBDA_NAME)
            r.log(f"  function exists — updating")
            lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
            time.sleep(3)
            lam.update_function_configuration(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                MemorySize=1024,
                Timeout=600,
                Environment={"Variables": {"FRED_KEY": FRED_KEY}},
            )
            r.ok(f"  ✓ updated")
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Role=ROLE_ARN,
                Code={"ZipFile": zb},
                MemorySize=1024,
                Timeout=600,
                Environment={"Variables": {"FRED_KEY": FRED_KEY}},
                Description="Event Study Automation — algorithmic detection + forward returns",
            )
            r.ok(f"  ✓ created")

        r.section("EventBridge schedule (daily 14 UTC)")
        try:
            rule_name = f"{LAMBDA_NAME}-daily"
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="cron(0 14 * * ? *)",
                State="ENABLED",
                Description=f"Daily trigger for {LAMBDA_NAME}",
            )
            arn = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
            events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": arn}])
            try:
                lam.add_permission(
                    FunctionName=LAMBDA_NAME,
                    StatementId=f"{rule_name}-perm",
                    Action="lambda:InvokeFunction",
                    Principal="events.amazonaws.com",
                    SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}",
                )
            except lam.exceptions.ResourceConflictException:
                pass
            r.ok(f"  ✓ wired")
        except Exception as e:
            r.fail(f"  ✗ {e}")

        time.sleep(8)

        r.section("Smoke test")
        try:
            t0 = time.time()
            inv = lam.invoke(FunctionName=LAMBDA_NAME, Payload=b"{}")
            payload = inv["Payload"].read().decode()
            r.log(f"  status: {inv['StatusCode']} duration: {time.time()-t0:.1f}s")
            r.log(f"  resp: {payload[:500]}")
            if inv.get("FunctionError"):
                r.fail(f"  ✗ {inv['FunctionError']}")
                return
        except Exception as e:
            r.fail(f"  ✗ {e}")
            return

        r.section("S3 verify")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/event-study.json")
            data = json.loads(obj["Body"].read())
            r.log(f"  as_of: {data.get('as_of_date')}")
            r.log(f"  active_themes: {data.get('active_themes')}")
            r.log(f"  expected_21d_return: {data.get('expected_21d_return_from_active_pct')}%")

            r.section("📊 Event class summaries")
            for name, study in (data.get("studies") or {}).items():
                summary_21d = study.get("forward_return_summary", {}).get("21d")
                summary_63d = study.get("forward_return_summary", {}).get("63d")
                active = "🔴 ACTIVE" if study.get("currently_active") else "—"
                r.log(
                    f"  {name:25s} n={study.get('n_with_forward_data'):>3} "
                    f"days_since={study.get('days_since_most_recent')!s:>5} {active}"
                )
                if summary_21d:
                    r.log(
                        f"    21d: mean={summary_21d['mean_pct']:>+5.2f}% median={summary_21d['median_pct']:>+5.2f}% "
                        f"hit={summary_21d['hit_rate_pct']:>4.1f}%"
                    )
                if summary_63d:
                    r.log(
                        f"    63d: mean={summary_63d['mean_pct']:>+5.2f}% median={summary_63d['median_pct']:>+5.2f}% "
                        f"hit={summary_63d['hit_rate_pct']:>4.1f}%"
                    )
                if study.get("most_recent_date"):
                    r.log(f"    most recent: {study['most_recent_date']}")
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
