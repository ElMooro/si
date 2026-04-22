#!/usr/bin/env python3
"""
Attach missing read permissions to the github-actions-justhodl IAM user.

Running the Lambda audit requires reading CloudWatch Metrics and EventBridge
Rules. Neither is covered by the initial policy set. This one-shot script
attaches two AWS-managed read-only policies.

Idempotent — safe to re-run, no-ops if already attached.
"""

import sys
import boto3
from botocore.exceptions import ClientError

IAM_USER = "github-actions-justhodl"

MANAGED_POLICIES = [
    "arn:aws:iam::aws:policy/CloudWatchReadOnlyAccess",
    "arn:aws:iam::aws:policy/AmazonEventBridgeReadOnlyAccess",
]

iam = boto3.client("iam")


def list_attached_policies(user: str):
    attached = iam.list_attached_user_policies(UserName=user)["AttachedPolicies"]
    return {p["PolicyArn"] for p in attached}


def main():
    print(f"=== Fix IAM for audit: {IAM_USER} ===")
    current = list_attached_policies(IAM_USER)
    print(f"Currently attached ({len(current)}):")
    for arn in sorted(current):
        print(f"  {arn}")

    for arn in MANAGED_POLICIES:
        if arn in current:
            print(f"✓ already attached: {arn}")
            continue
        try:
            iam.attach_user_policy(UserName=IAM_USER, PolicyArn=arn)
            print(f"+ attached: {arn}")
        except ClientError as e:
            sys.exit(f"FAIL attaching {arn}: {e}")

    print("✅ Done")


if __name__ == "__main__":
    main()
