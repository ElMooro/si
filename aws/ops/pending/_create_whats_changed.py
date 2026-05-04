"""
Create justhodl-whats-changed Lambda + daily 17 UTC schedule + smoke test.
"""
import os
import time
import zipfile
import io
import json
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-whats-changed"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
SOURCE_DIR = "aws/lambdas/justhodl-whats-changed/source"

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
    with report("create_whats_changed") as r:
        r.heading("Create justhodl-whats-changed + daily schedule")

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
                MemorySize=512,
                Timeout=300,
            )
            r.ok(f"  ✓ updated")
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=LAMBDA_NAME,
                Runtime="python3.12",
                Handler="lambda_function.lambda_handler",
                Role=ROLE_ARN,
                Code={"ZipFile": zb},
                MemorySize=512,
                Timeout=300,
                Description="Daily diff summarizer — what changed across all data files",
            )
            r.ok(f"  ✓ created")

        r.section("EventBridge schedule (daily 17 UTC)")
        try:
            rule_name = f"{LAMBDA_NAME}-daily"
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="cron(0 17 * * ? *)",
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

        time.sleep(6)

        r.section("Smoke test (will be empty diff on first run — only writes today's snapshot)")
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
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/whats-changed.json")
            data = json.loads(obj["Body"].read())
            r.log(f"  as_of: {data.get('as_of_date')}")
            r.log(f"  n_changes: {data.get('n_changes')}")
            r.log(f"  HIGH: {data.get('n_high')}, MED: {data.get('n_med')}, LOW: {data.get('n_low')}")
            r.section("📊 Files processed")
            for f in data.get("files_processed", []):
                r.log(f"  {f.get('file'):45s} prev_snapshot={f.get('prev_snapshot') or 'NEW'} n_changes={f.get('n_changes', 0)}")
            if data.get("changes"):
                r.section("🔄 Today's changes")
                for c in data["changes"][:15]:
                    r.log(f"  [{c.get('severity'):4s}] {c.get('category'):22s} {c.get('summary')}")
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
