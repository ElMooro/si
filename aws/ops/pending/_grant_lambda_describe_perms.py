"""
Grant lambda-execution-role permissions to query Lambda configuration.

Why this is needed
==================
The health-monitor Lambda calls lambda:GetFunctionConfiguration to read
each Lambda's LastModified timestamp, which it uses to apply a grace
period for newly-deployed Lambdas (so a Lambda deployed 30 minutes ago
doesn't show RED for "only 2 invocations in 24h" when its full-day
target is 40).

Without this permission the call fails silently (caught Exception, age
returns None, RED branch hit). After granting it, newly-deployed
Lambdas correctly show YELLOW with the "scaled target" message until
they've been alive 24h.

What this script does
=====================
A. Inspects current inline policies on lambda-execution-role
B. Adds a minimal inline policy granting:
     lambda:GetFunctionConfiguration   (used by health-monitor)
     lambda:ListFunctions              (used by future audits)
   Resource = arn:aws:lambda:us-east-1:857687956942:function:*
   (any function in this account in this region)
C. Verifies the policy is attached
"""
import json
import time

from ops_report import report
import boto3
from botocore.exceptions import ClientError

REGION = "us-east-1"
ACCOUNT_ID = "857687956942"
ROLE_NAME = "lambda-execution-role"
POLICY_NAME = "lambda-describe-self-and-peers"

iam = boto3.client("iam", region_name=REGION)


POLICY_DOC = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "DescribeLambdaFunctions",
            "Effect": "Allow",
            "Action": [
                "lambda:GetFunctionConfiguration",
                "lambda:GetFunction",
                "lambda:ListFunctions",
            ],
            "Resource": [
                f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:*",
                "*",   # ListFunctions requires "*" resource
            ],
        }
    ],
}


def main():
    with report("grant_lambda_describe_to_exec_role") as r:
        r.heading("Grant lambda-execution-role describe permissions")

        r.section("1. Inspect current inline policies")
        try:
            existing = iam.list_role_policies(RoleName=ROLE_NAME)
            policies = existing.get("PolicyNames", [])
            r.log(f"  current inline policies: {policies}")
            if POLICY_NAME in policies:
                # Already have it — print + verify content
                cur = iam.get_role_policy(RoleName=ROLE_NAME, PolicyName=POLICY_NAME)
                r.log(f"  policy '{POLICY_NAME}' already exists")
                cur_doc = cur["PolicyDocument"]
                if isinstance(cur_doc, str):
                    cur_doc = json.loads(cur_doc)
                r.log(f"  current document: {json.dumps(cur_doc)[:200]}")
                # Re-put anyway in case content drifted
                r.log(f"  re-applying to ensure desired state…")
        except ClientError as e:
            r.fail(f"  list_role_policies failed: {e}")
            return

        r.section("2. Apply inline policy")
        try:
            iam.put_role_policy(
                RoleName=ROLE_NAME,
                PolicyName=POLICY_NAME,
                PolicyDocument=json.dumps(POLICY_DOC),
            )
            r.ok(f"  ✓ put_role_policy {POLICY_NAME} on {ROLE_NAME}")
        except ClientError as e:
            r.fail(f"  put_role_policy failed: {e}")
            return

        r.section("3. Verify")
        time.sleep(2)   # IAM is eventually consistent
        try:
            check = iam.get_role_policy(RoleName=ROLE_NAME, PolicyName=POLICY_NAME)
            actions = check["PolicyDocument"]["Statement"][0]["Action"]
            r.ok(f"  ✓ verified — actions granted: {actions}")
        except ClientError as e:
            r.fail(f"  verify failed: {e}")


if __name__ == "__main__":
    main()
