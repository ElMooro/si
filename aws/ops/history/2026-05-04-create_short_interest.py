"""
Create the justhodl-short-interest Lambda + 6h EventBridge schedule.

Tracks FINRA daily short volume (last 14 trading days) and Polygon
short interest snapshots (bi-monthly). Surfaces squeeze risks, crowded
shorts, distribution, and covering signals.
"""
import json
import os
import time
import zipfile
import io
import boto3
from ops_report import report

REGION = "us-east-1"
LAMBDA_NAME = "justhodl-short-interest"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"

POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
SOURCE_DIR = "aws/lambdas/justhodl-short-interest/source"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)


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
    with report("create_short_interest") as r:
        r.heading("Create justhodl-short-interest Lambda + schedule")

        r.section("1. Build deployment zip")
        try:
            zb = make_zip()
            r.log(f"  zip size: {len(zb):,}b")
        except Exception as e:
            r.fail(f"  ✗ {e}")
            return

        r.section("2. Create or update Lambda")
        try:
            try:
                lam.get_function(FunctionName=LAMBDA_NAME)
                r.log(f"  function exists — updating code + config")
                lam.update_function_code(FunctionName=LAMBDA_NAME, ZipFile=zb)
                time.sleep(3)
                lam.update_function_configuration(
                    FunctionName=LAMBDA_NAME,
                    Runtime="python3.12",
                    Handler="lambda_function.lambda_handler",
                    MemorySize=512,
                    Timeout=600,
                    Environment={"Variables": {"POLYGON_KEY": POLYGON_KEY}},
                )
                r.ok(f"  ✓ updated")
            except lam.exceptions.ResourceNotFoundException:
                r.log(f"  function does not exist — creating")
                lam.create_function(
                    FunctionName=LAMBDA_NAME,
                    Runtime="python3.12",
                    Handler="lambda_function.lambda_handler",
                    Role=ROLE_ARN,
                    Code={"ZipFile": zb},
                    MemorySize=512,
                    Timeout=600,
                    Environment={"Variables": {"POLYGON_KEY": POLYGON_KEY}},
                    Description="Short positioning tracker (FINRA + Polygon)",
                )
                r.ok(f"  ✓ created")
        except Exception as e:
            r.fail(f"  ✗ {e}")
            return

        r.section("3. EventBridge 6h schedule")
        try:
            rule_name = f"{LAMBDA_NAME}-6h"
            events.put_rule(
                Name=rule_name,
                ScheduleExpression="rate(6 hours)",
                State="ENABLED",
                Description=f"6h trigger for {LAMBDA_NAME}",
            )
            r.log(f"  rule: {rule_name}")
            arn = lam.get_function(FunctionName=LAMBDA_NAME)["Configuration"]["FunctionArn"]
            events.put_targets(
                Rule=rule_name,
                Targets=[{"Id": "1", "Arn": arn}],
            )
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
            r.ok(f"  ✓ schedule wired")
        except Exception as e:
            r.fail(f"  ✗ {e}")

        r.section("4. Smoke test")
        try:
            t0 = time.time()
            inv = lam.invoke(FunctionName=LAMBDA_NAME, Payload=b"{}")
            payload = inv["Payload"].read().decode()
            r.log(f"  status: {inv['StatusCode']}")
            r.log(f"  duration: {time.time()-t0:.1f}s")
            r.log(f"  response: {payload[:400]}")
            if inv.get("FunctionError"):
                r.fail(f"  ✗ function error: {inv['FunctionError']}")
        except Exception as e:
            r.fail(f"  ✗ {e}")


if __name__ == "__main__":
    main()
