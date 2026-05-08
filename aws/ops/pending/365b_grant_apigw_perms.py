#!/usr/bin/env python3
"""
Step 365b — Grant github-actions-justhodl IAM user the permissions
ops 365 needs to bootstrap the WebSocket pipeline.

Required perms:
  • apigateway:* (create/get/update WebSocket API + routes + stages)
  • lambda:CreateFunctionUrlConfig, GetFunctionUrlConfig, UpdateFunctionUrlConfig
  • lambda:UpdateFunctionConfiguration (env vars)
  • lambda:AddPermission, RemovePermission
  • s3:PutBucketNotificationConfiguration, GetBucketNotificationConfiguration
  • iam:PutRolePolicy (to attach inline policies on lambda-execution-role)

These are scoped to the resources we actually touch.
"""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/365b_grant_apigw_perms.json"
USER = "github-actions-justhodl"
POLICY_NAME = "wss-bootstrap-perms"

iam = boto3.client("iam")

POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "ApiGatewayFull",
            "Effect": "Allow",
            "Action": [
                "apigateway:GET", "apigateway:POST", "apigateway:PUT",
                "apigateway:PATCH", "apigateway:DELETE",
            ],
            "Resource": "*",
        },
        {
            "Sid": "LambdaConfig",
            "Effect": "Allow",
            "Action": [
                "lambda:UpdateFunctionConfiguration",
                "lambda:CreateFunctionUrlConfig",
                "lambda:GetFunctionUrlConfig",
                "lambda:UpdateFunctionUrlConfig",
                "lambda:DeleteFunctionUrlConfig",
                "lambda:AddPermission",
                "lambda:RemovePermission",
                "lambda:GetPolicy",
            ],
            "Resource": "arn:aws:lambda:us-east-1:857687956942:function:*",
        },
        {
            "Sid": "S3BucketNotification",
            "Effect": "Allow",
            "Action": [
                "s3:GetBucketNotificationConfiguration",
                "s3:PutBucketNotificationConfiguration",
            ],
            "Resource": "arn:aws:s3:::justhodl-dashboard-live",
        },
        {
            "Sid": "IamForLambdaRoles",
            "Effect": "Allow",
            "Action": ["iam:PutRolePolicy", "iam:GetRolePolicy"],
            "Resource": "arn:aws:iam::857687956942:role/lambda-execution-role",
        },
    ],
}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "user": USER, "policy": POLICY_NAME}
    try:
        iam.put_user_policy(UserName=USER, PolicyName=POLICY_NAME,
                            PolicyDocument=json.dumps(POLICY))
        out["status"] = "attached"
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
