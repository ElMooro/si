#!/usr/bin/env python3
"""Step 268 — Retry macro-nowcast post-deploy with longer wait."""
import json, os, time
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
LAMBDA_NAME = "justhodl-macro-nowcast"
RULE_NAME = "justhodl-macro-nowcast-6h"
BUCKET = "justhodl-dashboard-live"
REPORT_PATH = "aws/ops/reports/268_macro_nowcast_retry.json"

lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def main():
    out = {"started": datetime.now(timezone.utc).isoformat()}
    started = time.time()
    # Wait up to 10 min for Lambda to exist
    arn = None
    for i in range(60):
        try:
            cur = lam.get_function(FunctionName=LAMBDA_NAME)
            arn = cur["Configuration"]["FunctionArn"]
            out["lambda_found_after_s"] = round(time.time() - started, 1)
            break
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
            time.sleep(10)
    if not arn:
        out["fatal_error"] = "Lambda still not deployed after 600s"
    else:
        # Wait for Active state
        for _ in range(30):
            cur = lam.get_function(FunctionName=LAMBDA_NAME)
            if cur["Configuration"].get("State") == "Active":
                break
            time.sleep(2)
        # EB rule
        eb.put_rule(Name=RULE_NAME, ScheduleExpression="rate(6 hours)", State="ENABLED")
        eb.put_targets(Rule=RULE_NAME, Targets=[{"Id": "1", "Arn": arn}])
        try:
            lam.add_permission(FunctionName=LAMBDA_NAME, StatementId="EBInvoke6H",
                               Action="lambda:InvokeFunction", Principal="events.amazonaws.com",
                               SourceArn=f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceConflictException":
                raise
        # Sync invoke
        invoke_resp = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse", Payload=b"{}")
        payload_raw = invoke_resp["Payload"].read()
        try:
            payload = json.loads(payload_raw)
        except: payload = {"raw": payload_raw[:500].decode(errors="replace")}
        out["invoke"] = {"status_code": invoke_resp.get("StatusCode"),
                         "function_error": invoke_resp.get("FunctionError"),
                         "payload": payload}
        time.sleep(3)
        try:
            body = json.loads(s3.get_object(Bucket=BUCKET, Key="data/macro-nowcast.json")["Body"].read())
            out["nowcast"] = {
                "generated_at": body.get("generated_at"),
                "composite_z": body.get("composite_z"),
                "regime": body.get("regime"),
                "components": body.get("components"),
            }
        except Exception as e:
            out["s3_err"] = str(e)
    out["duration_s"] = round(time.time() - started, 1)
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w") as f:
        json.dump(out, f, indent=2, default=str)
    print(json.dumps(out, indent=2, default=str)[:3000])
    return 0 if "fatal_error" not in out else 1

if __name__ == "__main__":
    raise SystemExit(main())
