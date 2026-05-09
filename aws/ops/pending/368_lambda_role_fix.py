#!/usr/bin/env python3
"""Step 368 — switch openbb-websocket-{handler,broadcast} to lambda-execution-role.

Audit 367 revealed both Lambdas use websocket-lambda-role, which doesn't
have the IAM policies attached by 365. Easiest fix: switch both to
lambda-execution-role (the default JustHodl role) which now has:
  • ddb-websocket-connections (DDB read/write on WebSocketConnections)
  • execute-api-manage-connections (post_to_connection on the WSS API)
  • ssm-push-vapid (SSM read on /justhodl/push/* — admin-token)
"""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REPORT = "aws/ops/reports/368_lambda_role_fix.json"
NEW_ROLE = "arn:aws:iam::857687956942:role/lambda-execution-role"
TARGETS = ["openbb-websocket-handler", "openbb-websocket-broadcast"]

lam = boto3.client("lambda", region_name="us-east-1")


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "fns": {}}
    for fn in TARGETS:
        try:
            cur = lam.get_function(FunctionName=fn)["Configuration"]
            out["fns"][fn] = {"old_role": cur.get("Role"), "old_handler": cur.get("Handler"), "old_runtime": cur.get("Runtime")}
            # Set role + ensure handler points to lambda_handler in the new code,
            # and align runtime to python3.12
            updates = {"Role": NEW_ROLE, "Handler": "lambda_function.lambda_handler", "Runtime": "python3.12"}
            # Wait for any pending updates first
            try:
                lam.get_waiter("function_updated").wait(FunctionName=fn)
            except Exception:
                pass
            lam.update_function_configuration(FunctionName=fn, **updates)
            lam.get_waiter("function_updated").wait(FunctionName=fn)
            after = lam.get_function(FunctionName=fn)["Configuration"]
            out["fns"][fn].update({
                "new_role": after.get("Role"),
                "new_handler": after.get("Handler"),
                "new_runtime": after.get("Runtime"),
                "ok": after.get("Role") == NEW_ROLE,
            })
        except Exception as e:
            out["fns"][fn] = {"error": f"{type(e).__name__}: {e}"}
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
