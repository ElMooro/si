#!/usr/bin/env python3
"""
Step 365b — Grant github-actions-justhodl IAM user the permissions
ops 365 needs to bootstrap the WebSocket pipeline.

Inline policies are capped at 2048 bytes per user. Using a managed policy
instead (lifts cap to 6144 bytes) and attaching it.
"""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/365b_grant_apigw_perms.json"
USER = "github-actions-justhodl"
POLICY_NAME = "JustHodlWssBootstrap"
ACCOUNT = "857687956942"

iam = boto3.client("iam")

POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow",
         "Action": ["apigateway:GET", "apigateway:POST", "apigateway:PUT",
                    "apigateway:PATCH", "apigateway:DELETE"],
         "Resource": "*"},
        {"Effect": "Allow",
         "Action": ["lambda:UpdateFunctionConfiguration",
                    "lambda:CreateFunctionUrlConfig",
                    "lambda:GetFunctionUrlConfig",
                    "lambda:UpdateFunctionUrlConfig",
                    "lambda:AddPermission", "lambda:RemovePermission",
                    "lambda:GetPolicy"],
         "Resource": f"arn:aws:lambda:us-east-1:{ACCOUNT}:function:*"},
        {"Effect": "Allow",
         "Action": ["s3:GetBucketNotificationConfiguration",
                    "s3:PutBucketNotificationConfiguration"],
         "Resource": f"arn:aws:s3:::justhodl-dashboard-live"},
        {"Effect": "Allow",
         "Action": ["iam:PutRolePolicy", "iam:GetRolePolicy"],
         "Resource": f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"},
    ],
}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "user": USER, "policy": POLICY_NAME}
    arn = f"arn:aws:iam::{ACCOUNT}:policy/{POLICY_NAME}"
    # Create the managed policy if it doesn't exist
    try:
        iam.create_policy(PolicyName=POLICY_NAME, PolicyDocument=json.dumps(POLICY),
                          Description="JustHodl WSS bootstrap perms (used by ops 365)")
        out["policy_created"] = True
    except iam.exceptions.EntityAlreadyExistsException:
        # Update by creating a new version + setting default
        try:
            versions = iam.list_policy_versions(PolicyArn=arn).get("Versions", [])
            # Delete oldest non-default if at limit (max 5)
            non_def = [v for v in versions if not v.get("IsDefaultVersion")]
            if len(versions) >= 5 and non_def:
                non_def.sort(key=lambda v: v["CreateDate"])
                iam.delete_policy_version(PolicyArn=arn, VersionId=non_def[0]["VersionId"])
            iam.create_policy_version(PolicyArn=arn, PolicyDocument=json.dumps(POLICY),
                                      SetAsDefault=True)
            out["policy_updated"] = True
        except Exception as e:
            out["policy_update_error"] = str(e)
    # Attach to user
    try:
        iam.attach_user_policy(UserName=USER, PolicyArn=arn)
        out["attached"] = True
    except Exception as e:
        out["attach_error"] = str(e)
    out["status"] = "success" if out.get("attached") else "error"
    out["finished"] = datetime.now(timezone.utc).isoformat()
    os.makedirs(os.path.dirname(REPORT), exist_ok=True)
    with open(REPORT, "w") as f:
        json.dump(out, f, indent=2, default=str)


if __name__ == "__main__":
    main()
