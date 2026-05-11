#!/usr/bin/env python3
"""Step 432 — Wait for new Lambda to deploy, create EventBridge rule
to run alerts every 30 min, and do a test invoke."""
import io, json, os, time, zipfile
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/432_alerts_setup.json"
NAME = "justhodl-tmp-alerts-setup"
ROLE_ARN = "arn:aws:iam::857687956942:role/lambda-execution-role"
lam = boto3.client("lambda", region_name="us-east-1")

DIAG = '''
import json
import boto3
lam = boto3.client("lambda", region_name="us-east-1")
events = boto3.client("events", region_name="us-east-1")

def lambda_handler(event, context):
    out = {}

    # 1. Confirm justhodl-screener-alerts exists + bump memory if needed
    try:
        cfg = lam.get_function_configuration(FunctionName="justhodl-screener-alerts")
        out["lambda"] = {
            "last_modified": cfg["LastModified"],
            "code_size": cfg["CodeSize"],
            "memory": cfg["MemorySize"],
            "timeout": cfg["Timeout"],
            "env_keys": list((cfg.get("Environment") or {}).get("Variables", {}).keys()),
        }
    except Exception as e:
        out["lambda_err"] = str(e)[:300]
        return {"statusCode": 200, "body": json.dumps(out, default=str)}

    # 2. Create / update EventBridge rule — every 30 min
    rule_name = "justhodl-screener-alerts-30min"
    try:
        r = events.put_rule(
            Name=rule_name,
            ScheduleExpression="rate(30 minutes)",
            State="ENABLED",
            Description="Trigger justhodl-screener-alerts every 30 min"
        )
        out["rule_arn"] = r["RuleArn"]
        # Allow EventBridge to invoke the Lambda
        try:
            lam.add_permission(
                FunctionName="justhodl-screener-alerts",
                StatementId="EventBridgeInvoke",
                Action="lambda:InvokeFunction",
                Principal="events.amazonaws.com",
                SourceArn=r["RuleArn"]
            )
            out["permission_added"] = True
        except lam.exceptions.ResourceConflictException:
            out["permission_added"] = "already_existed"
        # Attach the lambda as target
        target_arn = lam.get_function(FunctionName="justhodl-screener-alerts")["Configuration"]["FunctionArn"]
        events.put_targets(
            Rule=rule_name,
            Targets=[{"Id": "1", "Arn": target_arn}]
        )
        out["target_attached"] = target_arn
    except Exception as e:
        out["rule_err"] = str(e)[:300]

    # 3. Test invoke — synchronous, see what it would alert
    try:
        resp = lam.invoke(
            FunctionName="justhodl-screener-alerts",
            InvocationType="RequestResponse",
            Payload=b"{}")
        body = resp["Payload"].read().decode("utf-8")
        try:
            parsed = json.loads(body)
            out["test_invoke"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
        except Exception:
            out["test_invoke_raw"] = body[:500]
    except Exception as e:
        out["invoke_err"] = str(e)[:300]

    return {"statusCode": 200, "body": json.dumps(out, default=str)}
'''


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    # Wait for new Lambda to deploy
    print("Waiting 90s for deploy-lambdas to publish justhodl-screener-alerts...")
    time.sleep(90)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("lambda_function.py", DIAG)
    zb = buf.getvalue()
    try:
        lam.create_function(FunctionName=NAME, Runtime="python3.12",
                            Handler="lambda_function.lambda_handler", Role=ROLE_ARN,
                            MemorySize=256, Timeout=120, Code={"ZipFile": zb})
        lam.get_waiter("function_active_v2").wait(FunctionName=NAME)
    except Exception:
        lam.update_function_code(FunctionName=NAME, ZipFile=zb)
        lam.get_waiter("function_updated").wait(FunctionName=NAME)
    time.sleep(2)
    resp = lam.invoke(FunctionName=NAME, InvocationType="RequestResponse", Payload=b"{}")
    body = resp["Payload"].read().decode("utf-8")
    try:
        parsed = json.loads(body)
        out["test"] = json.loads(parsed["body"]) if "body" in parsed and parsed["body"] else parsed
    except Exception:
        out["raw"] = body[:6000]
    try: lam.delete_function(FunctionName=NAME)
    except Exception: pass
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f: json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
