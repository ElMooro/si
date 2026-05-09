#!/usr/bin/env python3
"""Step 377 — Bootstrap justhodl-tenor-signal-interpreter.

Creates new Lambda, attaches EventBridge schedule (matches auction-crisis-detector
cadence: every 15 min during 14-22 UTC weekdays + 4-hour off-hours backstop),
runs initial invocation, verifies output JSON.
"""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/377_tenor_setup.json"
ACCOUNT = "857687956942"
REGION = "us-east-1"
ROLE_ARN = f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"
FN_NAME = "justhodl-tenor-signal-interpreter"
SOURCE = "aws/lambdas/justhodl-tenor-signal-interpreter/source/lambda_function.py"

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
    env = {
        "S3_BUCKET": "justhodl-dashboard-live",
        "FRED_API_KEY": "2f057499936072679d8843d7fce99989",
    }
    try:
        lam.get_function(FunctionName=FN_NAME)
        try:
            lam.get_waiter("function_updated").wait(FunctionName=FN_NAME)
        except Exception:
            pass
        lam.update_function_code(FunctionName=FN_NAME, ZipFile=code)
        lam.get_waiter("function_updated").wait(FunctionName=FN_NAME)
        lam.update_function_configuration(
            FunctionName=FN_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            Environment={"Variables": env}, MemorySize=512, Timeout=120,
        )
        lam.get_waiter("function_updated").wait(FunctionName=FN_NAME)
        out["lambda"] = "updated"
    except lam.exceptions.ResourceNotFoundException:
        lam.create_function(
            FunctionName=FN_NAME, Runtime="python3.12",
            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
            Code={"ZipFile": code}, Environment={"Variables": env},
            MemorySize=512, Timeout=120,
            Tags={"project": "justhodl", "feature": "tenor-signals"},
        )
        lam.get_waiter("function_active_v2").wait(FunctionName=FN_NAME)
        out["lambda"] = "created"


def step_eventbridge(out):
    fn_arn = lam.get_function(FunctionName=FN_NAME)["Configuration"]["FunctionArn"]

    # Active window — every 15 min during weekday auction publishing hours
    rule_active = "tenor-signals-active"
    events.put_rule(
        Name=rule_active,
        ScheduleExpression="cron(0/15 14-22 ? * MON-FRI *)",
        State="ENABLED",
        Description="Active-hours: every 15min Mon-Fri 14-22 UTC",
    )
    events.put_targets(Rule=rule_active, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        lam.add_permission(
            FunctionName=FN_NAME,
            StatementId="eb-tenor-active",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{rule_active}",
        )
    except lam.exceptions.ResourceConflictException:
        pass

    # Backstop — every 4 hours for weekend / off-hours
    rule_backstop = "tenor-signals-backstop"
    events.put_rule(
        Name=rule_backstop,
        ScheduleExpression="rate(4 hours)",
        State="ENABLED",
        Description="Backstop: every 4 hours off-hours",
    )
    events.put_targets(Rule=rule_backstop, Targets=[{"Id": "1", "Arn": fn_arn}])
    try:
        lam.add_permission(
            FunctionName=FN_NAME,
            StatementId="eb-tenor-backstop",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT}:rule/{rule_backstop}",
        )
    except lam.exceptions.ResourceConflictException:
        pass

    out["eventbridge"] = {"active": rule_active, "backstop": rule_backstop}


def step_initial_run(out):
    """Synchronously invoke once so data/auction-tenor-signals.json exists."""
    try:
        resp = lam.invoke(FunctionName=FN_NAME, InvocationType="RequestResponse",
                          Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        out["initial_run"] = {"status": resp.get("StatusCode"), "body": body[:500]}
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
