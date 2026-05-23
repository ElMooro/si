"""
ops 1073 — fix S3 CRR setup that partially failed in ops 1069.

ops 1069 results:
  dr_bucket: CREATED ✅
  dr_versioning: ENABLED ✅
  iam_role: CREATE_ERR (description regex validation failed) ❌
  iam_policy: ERR (role didn't exist) ❌
  replication_config: APPLIED ❌ (but final_check failed because role attach broke)
  dr_lifecycle: APPLIED ✅

Fix:
  1. Create IAM role with clean description (no special chars)
  2. Attach inline policy granting s3 replication perms
  3. Re-apply replication config now that role exists
  4. Verify with GetBucketReplication
"""
import json, os, time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

ACCOUNT_ID = "857687956942"
SRC_BUCKET = "justhodl-dashboard-live"
DR_BUCKET = "justhodl-dashboard-live-dr"
DR_REGION = "us-west-2"
SRC_REGION = "us-east-1"

ROLE_NAME = "justhodl-s3-replication-role"
POLICY_NAME = "justhodl-s3-replication-policy"

TRUST_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {"Service": "s3.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }
    ],
}

REPLICATION_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetReplicationConfiguration",
                "s3:ListBucket",
            ],
            "Resource": [f"arn:aws:s3:::{SRC_BUCKET}"],
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObjectVersionForReplication",
                "s3:GetObjectVersionAcl",
                "s3:GetObjectVersionTagging",
                "s3:GetObjectRetention",
                "s3:GetObjectLegalHold",
            ],
            "Resource": [f"arn:aws:s3:::{SRC_BUCKET}/*"],
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ReplicateObject",
                "s3:ReplicateDelete",
                "s3:ReplicateTags",
                "s3:GetObjectVersionTagging",
                "s3:ObjectOwnerOverrideToBucketOwner",
            ],
            "Resource": [f"arn:aws:s3:::{DR_BUCKET}/*"],
        },
    ],
}


def main():
    report = {"started_at": datetime.now(timezone.utc).isoformat()}
    iam = boto3.client("iam")
    s3 = boto3.client("s3", region_name=SRC_REGION)

    role_arn = f"arn:aws:iam::{ACCOUNT_ID}:role/{ROLE_NAME}"

    # 1. Create role (clean description, no special chars)
    try:
        iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(TRUST_POLICY),
            Description="S3 cross-region replication role for justhodl DR bucket",
            MaxSessionDuration=3600,
        )
        report["role"] = "CREATED"
        # creation propagation
        time.sleep(8)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "EntityAlreadyExists":
            report["role"] = "ALREADY_EXISTS"
        else:
            report["role"] = f"ERR: {e}"
            return _finish(report)

    # 2. Attach inline policy
    try:
        iam.put_role_policy(
            RoleName=ROLE_NAME,
            PolicyName=POLICY_NAME,
            PolicyDocument=json.dumps(REPLICATION_POLICY),
        )
        report["policy"] = "ATTACHED"
    except ClientError as e:
        report["policy"] = f"ERR: {e}"
        return _finish(report)

    # 3. Apply replication config
    repl_cfg = {
        "Role": role_arn,
        "Rules": [
            {
                "ID": "ReplicateAllToDR",
                "Status": "Enabled",
                "Priority": 1,
                "Filter": {},
                "DeleteMarkerReplication": {"Status": "Disabled"},
                "Destination": {
                    "Bucket": f"arn:aws:s3:::{DR_BUCKET}",
                    "StorageClass": "STANDARD_IA",
                },
            }
        ],
    }
    try:
        s3.put_bucket_replication(
            Bucket=SRC_BUCKET,
            ReplicationConfiguration=repl_cfg,
        )
        report["replication"] = "APPLIED"
    except ClientError as e:
        report["replication"] = f"ERR: {e}"
        return _finish(report)

    # 4. Verify
    try:
        r = s3.get_bucket_replication(Bucket=SRC_BUCKET)
        rules = r.get("ReplicationConfiguration", {}).get("Rules", [])
        report["verify"] = {
            "rule_count": len(rules),
            "first_rule_status": rules[0].get("Status") if rules else None,
            "first_rule_destination": rules[0].get("Destination", {}).get("Bucket") if rules else None,
            "role_arn": r.get("ReplicationConfiguration", {}).get("Role"),
        }
    except ClientError as e:
        report["verify"] = f"ERR: {e}"

    return _finish(report)


def _finish(report):
    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    out_path = os.path.join(os.environ.get("REPO_ROOT", os.getcwd()), "aws", "ops", "reports", "1073.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
