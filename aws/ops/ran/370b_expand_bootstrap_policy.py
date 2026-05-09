#!/usr/bin/env python3
"""
Step 370b — Expand JustHodlWssBootstrap managed policy.

371 needs perms not yet granted:
  • lambda:CreateFunction, GetFunction, InvokeFunction (new Lambdas)
  • events:PutRule, PutTargets, DescribeRule, ListTargetsByRule (cron schedule)
  • ssm:PutParameter (saving simulator URL)

Idempotent — creates new policy version, sets as default, rotates oldest if at 5-version cap.
"""
import json, os
from datetime import datetime, timezone
import boto3

REPORT = "aws/ops/reports/370b_expand_bootstrap_policy.json"
ACCOUNT = "857687956942"
POLICY_NAME = "JustHodlWssBootstrap"
POLICY_ARN = f"arn:aws:iam::{ACCOUNT}:policy/{POLICY_NAME}"

iam = boto3.client("iam")

POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {"Effect": "Allow",
         "Action": ["apigateway:GET", "apigateway:POST", "apigateway:PUT",
                    "apigateway:PATCH", "apigateway:DELETE"],
         "Resource": "*"},
        {"Effect": "Allow",
         "Action": ["lambda:CreateFunction",
                    "lambda:GetFunction", "lambda:GetFunctionConfiguration",
                    "lambda:UpdateFunctionConfiguration", "lambda:UpdateFunctionCode",
                    "lambda:InvokeFunction",
                    "lambda:CreateFunctionUrlConfig",
                    "lambda:GetFunctionUrlConfig", "lambda:UpdateFunctionUrlConfig",
                    "lambda:AddPermission", "lambda:RemovePermission",
                    "lambda:GetPolicy", "lambda:TagResource"],
         "Resource": f"arn:aws:lambda:us-east-1:{ACCOUNT}:function:*"},
        {"Effect": "Allow",
         "Action": ["events:PutRule", "events:PutTargets", "events:DescribeRule",
                    "events:ListTargetsByRule", "events:RemoveTargets", "events:DeleteRule"],
         "Resource": f"arn:aws:events:us-east-1:{ACCOUNT}:rule/*"},
        {"Effect": "Allow",
         "Action": ["s3:GetBucketNotificationConfiguration",
                    "s3:PutBucketNotificationConfiguration"],
         "Resource": f"arn:aws:s3:::justhodl-dashboard-live"},
        {"Effect": "Allow",
         "Action": ["ssm:PutParameter", "ssm:GetParameter"],
         "Resource": f"arn:aws:ssm:us-east-1:{ACCOUNT}:parameter/justhodl/*"},
        {"Effect": "Allow",
         "Action": ["iam:PutRolePolicy", "iam:GetRolePolicy", "iam:PassRole"],
         "Resource": f"arn:aws:iam::{ACCOUNT}:role/lambda-execution-role"},
    ],
}


def main():
    out = {"started": datetime.now(timezone.utc).isoformat(), "policy": POLICY_NAME}
    try:
        # Drop oldest non-default version if at the 5-version limit
        versions = iam.list_policy_versions(PolicyArn=POLICY_ARN).get("Versions", [])
        non_default = [v for v in versions if not v.get("IsDefaultVersion")]
        if len(versions) >= 5 and non_default:
            non_default.sort(key=lambda v: v["CreateDate"])
            iam.delete_policy_version(PolicyArn=POLICY_ARN, VersionId=non_default[0]["VersionId"])
            out["dropped_old_version"] = non_default[0]["VersionId"]
        new_v = iam.create_policy_version(
            PolicyArn=POLICY_ARN, PolicyDocument=json.dumps(POLICY), SetAsDefault=True,
        )
        out["new_version"] = new_v["PolicyVersion"]["VersionId"]
        out["status"] = "version_created"
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
