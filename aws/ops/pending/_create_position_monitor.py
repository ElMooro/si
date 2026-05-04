"""Deploy justhodl-position-monitor + 30min schedule + smoke test."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-position-monitor"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-position-monitor/source"

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
    with report("create_position_monitor") as r:
        # 1. Lambda
        r.heading("1) Create / update justhodl-position-monitor")
        zb = make_zip()
        r.log(f"  zip size: {len(zb):,}b")
        try:
            lam.get_function(FunctionName=LAMBDA_NAME)
            lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
            time.sleep(3)
            for _ in range(20):
                cfg = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
                if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                    break
                time.sleep(2)
            lam.update_function_configuration(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Timeout=60,
                MemorySize=256,
                Role=ROLE_ARN,
            )
            r.ok("  ✓ updated existing")
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Role=ROLE_ARN,
                Handler="lambda_function.lambda_handler",
                Code={"ZipFile": zb},
                Timeout=60,
                MemorySize=256,
                Architectures=["x86_64"],
                Description="Proactive Telegram alerts on position stop/target events + decisive-call changes",
            )
            r.ok("  ✓ created")

        for _ in range(20):
            cfg = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.log(f"  state: Active mod={cfg.get('LastModified')}")
                break
            time.sleep(2)

        # 2. EventBridge schedule — every 30 minutes
        r.heading("2) EventBridge — every 30 minutes")
        rule_name = f"{LAMBDA_NAME}-30min"
        events.put_rule(
            Name=rule_name,
            ScheduleExpression="rate(30 minutes)",
            State="ENABLED",
            Description="Watch open paper positions for stop/target events + decisive-call changes",
        )
        fn_arn = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
        events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])
        try:
            lam.add_permission(
                FunctionName=LAMBDA_NAME,
                StatementId=f"{rule_name}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:857687956942:rule/{rule_name}",
            )
        except lam.exceptions.ResourceConflictException:
            pass
        r.ok(f"  ✓ {rule_name} → rate(30 minutes)")

        # 3. Smoke test
        r.heading("3) Smoke invoke")
        t0 = time.time()
        resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        r.log(f"  resp: {body[:600]}")

        # 4. Verify state file written
        r.heading("4) Verify state file")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="portfolio/position-monitor-state.json")
            d = json.loads(obj["Body"].read())
            r.log(f"  ✓ portfolio/position-monitor-state.json")
            r.log(f"    last_run: {d.get('last_run')}")
            r.log(f"    last_call_verb: {d.get('last_call_verb')}")
            r.log(f"    n_alerts_tracked: {len(d.get('alerts') or {})}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
