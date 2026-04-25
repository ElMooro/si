#!/usr/bin/env python3
"""
Step 98 — Final fmp-stock-picks-agent S3 perm grant.

Step 97 couldn't auto-detect the bucket because it's stored in a
variable, not a string literal. The error log revealed the actual
bucket: justhodl-historical-data-1758485495

Grant the role economyapi-lambda-role:
  - s3:GetObject + s3:PutObject + s3:ListBucket on
    justhodl-historical-data-1758485495 (resource + /*)
  - Test invoke to confirm
"""
import json
import os
import time

from ops_report import report
import boto3

REGION = "us-east-1"
iam = boto3.client("iam", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION)

with report("fmp_final_s3_grant") as r:
    r.heading("Final fmp-stock-picks-agent S3 perm grant")

    role_name = "economyapi-lambda-role"
    bucket = "justhodl-historical-data-1758485495"

    policy_doc = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "FmpStockPicksHistorical",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:ListBucket",
            ],
            "Resource": [
                f"arn:aws:s3:::{bucket}",
                f"arn:aws:s3:::{bucket}/*",
            ],
        }],
    }

    try:
        iam.put_role_policy(
            RoleName=role_name,
            PolicyName="FmpStockPicksHistorical",
            PolicyDocument=json.dumps(policy_doc),
        )
        r.ok(f"  Attached FmpStockPicksHistorical to {role_name}")
        r.log(f"  Bucket: {bucket}")
    except Exception as e:
        r.fail(f"  IAM put: {e}")

    time.sleep(8)

    name = "fmp-stock-picks-agent"
    try:
        resp = lam.invoke(FunctionName=name, InvocationType="RequestResponse")
        if resp.get("FunctionError"):
            payload = resp.get("Payload").read().decode()
            r.warn(f"  {name}: still erroring: {payload[:400]}")
        else:
            r.ok(f"  {name}: invoke clean ({resp.get('StatusCode')})")
    except Exception as e:
        r.fail(f"  invoke: {e}")

    r.kv(role=role_name, bucket=bucket)
    r.log("Done")
