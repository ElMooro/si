"""Create justhodl-earnings-tracker Lambda + 6h EB rule."""
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
FN_NAME = "justhodl-earnings-tracker"
RULE_NAME = "justhodl-earnings-tracker-6h"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)


def main():
    with report("create_earnings_tracker") as r:
        r.heading(f"Create {FN_NAME}")
        src = Path(f"aws/lambdas/{FN_NAME}/source")
        config = json.loads(Path(f"aws/lambdas/{FN_NAME}/config.json").read_text())

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in src.rglob("*.py"):
                zf.write(f, f.relative_to(src))
        zip_data = buf.getvalue()
        r.log(f"  zip: {len(zip_data)} bytes")

        r.section("1. Lambda")
        try:
            lam.get_function(FunctionName=FN_NAME)
            lam.update_function_code(FunctionName=FN_NAME, ZipFile=zip_data)
            time.sleep(2)
            lam.update_function_configuration(
                FunctionName=FN_NAME, MemorySize=config["memory_size"],
                Timeout=config["timeout"],
                Environment={"Variables": config.get("environment", {})},
            )
            r.ok(f"  ✓ updated {FN_NAME}")
        except lam.exceptions.ResourceNotFoundException:
            lam.create_function(
                FunctionName=FN_NAME, Runtime=config["runtime"],
                Role=ROLE_ARN, Handler=config["handler"],
                Code={"ZipFile": zip_data},
                MemorySize=config["memory_size"], Timeout=config["timeout"],
                Description=config["description"],
                Environment={"Variables": config.get("environment", {})},
                Tags=config.get("tags", {}),
            )
            r.ok(f"  ✓ created {FN_NAME}")

        r.section("2. EB rule + permissions (every 6h)")
        try:
            events.describe_rule(Name=RULE_NAME)
            r.log(f"  rule already exists")
        except events.exceptions.ResourceNotFoundException:
            events.put_rule(
                Name=RULE_NAME, ScheduleExpression="rate(6 hours)",
                State="ENABLED", Description="Earnings tracker — every 6h",
            )
            r.ok(f"  ✓ created rule {RULE_NAME}")
        events.put_targets(
            Rule=RULE_NAME,
            Targets=[{"Id": "1", "Arn": f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FN_NAME}"}],
        )
        try:
            lam.add_permission(
                FunctionName=FN_NAME,
                StatementId=f"{RULE_NAME}-invoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{RULE_NAME}",
            )
            r.ok(f"  ✓ added permission")
        except lam.exceptions.ResourceConflictException:
            r.log(f"  permission exists")

        r.section("3. Smoke test")
        try:
            t0 = time.time()
            time.sleep(2)
            resp = lam.invoke(
                FunctionName=FN_NAME,
                InvocationType="RequestResponse",
                Payload=b'{"source":"smoke-test"}',
            )
            r.log(f"  duration: {time.time()-t0:.1f}s")
            body = resp["Payload"].read().decode("utf-8")
            r.log(f"  response: {body[:300]}")
            data = json.loads(body)
            inner = json.loads(data.get("body", "{}"))
            for k, v in inner.items():
                r.log(f"    {k:30s} {v}")
            if inner.get("ok"):
                r.ok(f"  ✓ smoke test passed")
        except Exception as e:
            r.fail(f"  smoke test fail: {e}")


if __name__ == "__main__":
    main()
