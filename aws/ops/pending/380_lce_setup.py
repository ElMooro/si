#!/usr/bin/env python3
"""Step 380 — Bootstrap justhodl-liquidity-credit-engine."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/380_lce_setup.json"
ACCOUNT = "857687956942"
REGION = "us-east-1"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
FN_NAME = "justhodl-liquidity-credit-engine"
SOURCE = "aws/lambdas/justhodl-liquidity-credit-engine/source/lambda_function.py"

lam = boto3.client("lambda", region_name=REGION)
events = boto3.client("events", region_name=REGION)


def _zip(path):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        with open(path) as f:
            zf.writestr("lambda_function.py", f.read())
    return buf.getvalue()


def step_lambda(out):
    code = _zip(SOURCE)
    env = {"S3_BUCKET": "justhodl-dashboard-live",
           "FRED_API_KEY": "2f057499936072679d8843d7fce99989"}
    try:
        lam.get_function(FunctionName=FN_NAME)
        try: lam.get_waiter("function_updated").wait(FunctionName=FN_NAME)
        except Exception: pass
        lam.update_function_code(FunctionName=FN_NAME, ZipFile=code)
        lam.get_waiter("function_updated").wait(FunctionName=FN_NAME)
        lam.update_function_configuration(
            FunctionName=FN_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            Environment={"Variables": env}, MemorySize=512, Timeout=300,
        )
        lam.get_waiter("function_updated").wait(FunctionName=FN_NAME)
        out["lambda"] = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=FN_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            Code={"ZipFile": code}, Environment={"Variables": env},
            MemorySize=512, Timeout=300,
            Tags={"project": "justhodl", "feature": "liquidity-credit"},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=FN_NAME)
        out["lambda"] = "created"


def step_eventbridge(out):
    fn_arn = lam.get_function(FunctionName=FN_NAME)["Configuration"]["FunctionArn"]
    rule_name = "lce-every-6h"
    events.put_rule(
        Name=rule_name,
        ScheduleExpression="rate(6 hours)",
        State="ENABLED",
        Description="Every 6h liquidity & credit engine refresh (FRED daily/H.4.1 weekly)",
    )
    events.put_targets(Rule=rule_name, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        lam.add_permission(
            FunctionName=FN_NAME, StatementId="eb-lce-6h",
            Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{rule_name}",
        )
    except lam.exceptions.ResourceConflictException:
        pass
    out["eventbridge"] = {"rule": rule_name}


def step_initial_run(out):
    try:
        resp = lam.invoke(FunctionName=FN_NAME, InvocationType="RequestResponse", Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        out["initial_run"] = {"status": resp.get("StatusCode"), "body": body[:600]}
    except Exception as e:
        out["initial_run"] = {"error": str(e)[:300]}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "steps": {}}
    try:
        step_lambda(out["steps"])
        step_eventbridge(out["steps"])
        step_initial_run(out["steps"])
        out["status"] = "success"
    except Exception as e:
        out["status"] = "error"
        out["error"] = f"{type(e).__name__}: {e}"
        raise
    finally:
        out["finished"] = datetime.now(timezone.utc).isoformat()
        os.makedirs(os.path.dirname(REPORT), exist_ok=True)
        with open(REPORT, "w") as f:
            json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
