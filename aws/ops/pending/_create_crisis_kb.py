"""
Create justhodl-crisis-knowledge-base Lambda + daily EB rule.

The deploy-lambdas workflow zips the source/ directory and uploads to AWS,
but we need to first CREATE the function. After this op, the workflow
will keep it updated on every push to source/.
"""
import json
import time
import zipfile
import io
from pathlib import Path

from ops_report import report
import boto3

REGION = "us-east-1"
ACCOUNT = "857687956942"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
FN_NAME = "justhodl-crisis-knowledge-base"
RULE_NAME = "justhodl-crisis-kb-daily"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)


def main():
    with report("create_crisis_kb_lambda") as r:
        r.heading(f"Create {FN_NAME} Lambda + daily schedule")

        # Zip the source
        src_dir = Path(f"aws/lambdas/{FN_NAME}/source")
        if not src_dir.exists():
            r.fail(f"  source dir missing: {src_dir}")
            return
        config = json.loads(Path(f"aws/lambdas/{FN_NAME}/config.json").read_text())

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in src_dir.rglob("*.py"):
                zf.write(f, f.relative_to(src_dir))
        zip_data = buf.getvalue()
        r.log(f"  zip: {len(zip_data)} bytes")

        # 1. Create Lambda
        r.section("1. Lambda")
        try:
            existing = lam.get_function(FunctionName=FN_NAME)
            r.log(f"  Lambda already exists, updating code")
            lam.update_function_code(FunctionName=FN_NAME, ZipFile=zip_data)
            time.sleep(2)
            lam.update_function_configuration(
                FunctionName=FN_NAME,
                MemorySize=config["memory_size"],
                Timeout=config["timeout"],
                Environment={"Variables": config.get("environment", {})},
            )
            r.ok(f"  ✓ updated {FN_NAME}")
        except lam.exceptions.ResourceNotFoundException:
            r.log(f"  Lambda missing — creating")
            lam.create_function(
                FunctionName=FN_NAME,
                Runtime=config["runtime"],
                Role=ROLE_ARN,
                Handler=config["handler"],
                Code={"ZipFile": zip_data},
                MemorySize=config["memory_size"],
                Timeout=config["timeout"],
                Description=config["description"],
                Environment={"Variables": config.get("environment", {})},
                Tags=config.get("tags", {}),
            )
            r.ok(f"  ✓ created {FN_NAME}")

        # 2. EB rule — daily at 06:00 UTC
        r.section("2. EB rule + permissions")
        try:
            events.describe_rule(Name=RULE_NAME)
            r.log(f"  rule {RULE_NAME} already exists")
        except events.exceptions.ResourceNotFoundException:
            events.put_rule(
                Name=RULE_NAME,
                ScheduleExpression="cron(0 6 * * ? *)",   # 06:00 UTC daily
                State="ENABLED",
                Description="Daily Crisis KB rebuild",
            )
            r.ok(f"  ✓ created rule {RULE_NAME}")

        events.put_targets(
            Rule=RULE_NAME,
            Targets=[{"Id": "1", "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FN_NAME}"}],
        )
        r.ok(f"  ✓ target → {FN_NAME}")

        try:
            lam.add_permission(
                FunctionName=FN_NAME,
                StatementId=f"{RULE_NAME}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE_NAME}",
            )
            r.ok(f"  ✓ added invoke permission")
        except lam.exceptions.ResourceConflictException:
            r.log(f"  permission already exists")

        # 3. Smoke test
        r.section("3. Smoke test")
        try:
            time.sleep(2)
            resp = lam.invoke(
                FunctionName=FN_NAME,
                InvocationType="RequestResponse",
                Payload=b'{"source":"smoke-test"}',
            )
            r.log(f"  invoking {FN_NAME}…")
            body = resp["Payload"].read().decode("utf-8")
            r.ok(f"  ✓ smoke test passed")
            r.log(f"    response: {body[:300]}")
            data = json.loads(body)
            inner = json.loads(data.get("body", "{}"))
            for k, v in inner.items():
                r.log(f"    {k:30s} {v}")
        except Exception as e:
            r.fail(f"  ✗ smoke test failed: {e}")


if __name__ == "__main__":
    main()
