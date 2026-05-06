#!/usr/bin/env python3
"""Step 267 — Macro-nowcast post-deploy: EB rule + sync invoke.

Step 266 raced with deploy-lambdas.yml (Lambda not deployed when ops ran).
By now (subsequent push) the Lambda exists. This step:

  1. Verify Lambda exists + active
  2. Create EB rule justhodl-macro-nowcast-6h with rate(6 hours)
  3. Add Lambda permission for EB invocation
  4. Add EB target → Lambda
  5. Sync invoke once to populate data/macro-nowcast.json
  6. Verify output + persist verification report
"""
import json
import os
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
LAMBDA_NAME = "justhodl-macro-nowcast"
RULE_NAME = "justhodl-macro-nowcast-6h"
RULE_SCHEDULE = "rate(6 hours)"
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/macro-nowcast.json"
REPORT_PATH = "aws/ops/reports/267_macro_nowcast_postdeploy.json"

lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def verify_lambda():
    try:
        cur = lam.get_function(FunctionName=LAMBDA_NAME)
        return {"exists": True, "arn": cur["Configuration"]["FunctionArn"],
                "state": cur["Configuration"].get("State")}
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return {"exists": False}
        raise


def ensure_eb_rule(lambda_arn):
    eb.put_rule(
        Name=RULE_NAME, ScheduleExpression=RULE_SCHEDULE, State="ENABLED",
        Description="Trigger justhodl-macro-nowcast every 6 hours",
    )
    eb.put_targets(Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": lambda_arn}])
    try:
        lam.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId="EventBridgeInvoke6H",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceConflictException":
            raise


def sync_invoke():
    started = time.time()
    resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                      Payload=b"{}")
    elapsed = time.time() - started
    payload_raw = resp["Payload"].read()
    try:
        payload = json.loads(payload_raw)
    except Exception:
        payload = {"raw": payload_raw[:500].decode(errors="replace")}
    return {"function_error": resp.get("FunctionError"),
            "payload": payload,
            "status_code": resp.get("StatusCode"),
            "elapsed_s": round(elapsed, 1)}


def verify_output():
    out = {}
    try:
        body = json.loads(s3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)["Body"].read())
        out["generated_at"] = body.get("generated_at")
        out["composite_z"] = body.get("composite_z")
        out["regime"] = body.get("regime")
        out["components"] = body.get("components")
    except Exception as e:
        out["err"] = str(e)
    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        out["lambda"] = verify_lambda()
        if not out["lambda"]["exists"]:
            raise RuntimeError("Lambda still not deployed — push deploy-lambdas first")
        ensure_eb_rule(out["lambda"]["arn"])
        out["rule"] = {"name": RULE_NAME, "schedule": RULE_SCHEDULE, "state": "ENABLED"}
        out["sync_invoke"] = sync_invoke()
        time.sleep(2)
        out["verify"] = verify_output()
        out["duration_s"] = round(time.time() - started, 1)
    except Exception as e:
        import traceback
        out["fatal_error"] = str(e)
        out["traceback"] = traceback.format_exc()
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:4000])
    return 0 if "fatal_error" not in out else 1


if __name__ == "__main__":
    raise SystemExit(main())
