#!/usr/bin/env python3
"""Step 266 — Bootstrap justhodl-macro-nowcast Lambda + 6h EB schedule.

  1. Wait for deploy-lambdas.yml to deploy the Lambda from source/
     (push triggers it automatically; this script just verifies)
  2. Create EB rule justhodl-macro-nowcast-6h with rate(6 hours)
  3. Add Lambda permission for EB invocation
  4. Add EB target pointing at Lambda
  5. Sync invoke to populate data/macro-nowcast.json immediately
  6. Verify output + persist to aws/ops/reports/
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
BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/macro-nowcast.json"
REPORT_PATH = "aws/ops/reports/266_macro_nowcast_bootstrap.json"

lam = boto3.client("lambda", region_name=REGION)
eb = boto3.client("events", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def wait_for_lambda():
    """Wait up to 90s for deploy-lambdas.yml to create the Lambda."""
    for i in range(15):
        try:
            cfg = lam.get_function(FunctionName=LAMBDA_NAME)
            return {"exists": True, "arn": cfg["Configuration"]["FunctionArn"],
                    "last_modified": cfg["Configuration"]["LastModified"]}
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                raise
        print(f"[266] waiting for Lambda… ({i+1}/15)")
        time.sleep(6)
    return {"exists": False, "err": "Lambda not deployed after 90s"}


def ensure_eb_rule():
    rule_arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{RULE_NAME}"
    lambda_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{LAMBDA_NAME}"

    eb.put_rule(
        Name=RULE_NAME,
        ScheduleExpression="rate(6 hours)",
        State="ENABLED",
        Description="Compute composite macro nowcast from data/report.json every 6h",
    )

    try:
        lam.add_permission(
            FunctionName=LAMBDA_NAME,
            StatementId=f"{RULE_NAME}-invoke",
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=rule_arn,
        )
        perm_status = "added"
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            perm_status = "already_exists"
        else:
            raise

    eb.put_targets(
        Rule=RULE_NAME,
        Targets=[{"Id": "1", "Arn": lambda_arn}],
    )
    return {"rule_name": RULE_NAME, "rate": "6 hours", "permission": perm_status}


def invoke_and_verify():
    print(f"[266] invoking {LAMBDA_NAME}…")
    started = time.time()
    inv = lam.invoke(FunctionName=LAMBDA_NAME, InvocationType="RequestResponse",
                     Payload=b"{}")
    payload = json.loads(inv["Payload"].read())
    elapsed = round(time.time() - started, 2)
    out = {
        "status": inv.get("StatusCode"),
        "func_err": inv.get("FunctionError"),
        "payload": payload,
        "elapsed_s": elapsed,
    }
    if inv.get("FunctionError"):
        return out

    # Read the output it produced
    time.sleep(2)
    try:
        body = json.loads(s3.get_object(Bucket=BUCKET, Key=OUTPUT_KEY)["Body"].read())
        out["output_summary"] = {
            "regime": body.get("regime"),
            "normalized_score": body.get("normalized_score"),
            "coverage_pct": body.get("coverage_pct"),
            "n_components_used": body.get("n_components_used"),
            "n_components_failed": body.get("n_components_failed"),
            "top_3_contributors": [
                {"fred_id": c["fred_id"], "label": c["label"],
                 "z": c.get("z"), "contribution": c.get("contribution"),
                 "raw_value": c.get("raw_value"), "error": c.get("error")}
                for c in (body.get("components") or [])[:3]
            ],
        }
    except Exception as e:
        out["output_read_err"] = str(e)[:200]
    return out


def main():
    started = time.time()
    out = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        out["lambda"] = wait_for_lambda()
        if not out["lambda"].get("exists"):
            raise RuntimeError(out["lambda"].get("err"))
        out["eb_rule"] = ensure_eb_rule()
        out["invoke"] = invoke_and_verify()
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
