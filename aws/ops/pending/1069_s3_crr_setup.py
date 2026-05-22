#!/usr/bin/env python3
"""
ops 1069 — S3 CROSS-REGION REPLICATION + DR BACKUP BUCKET
==========================================================
Creates DR posture for the entire S3 footprint (24K keys, all engine outputs,
all dashboards). Steps:

  1. Create destination bucket `justhodl-dashboard-live-dr` in us-west-2
  2. Enable versioning on destination
  3. Create IAM role for replication w/ policy
  4. Attach replication config to source bucket
  5. Backfill: list current S3 keys → flag count + size for inventory

Idempotent: skips if bucket/role/config already exist.
"""
import json, os, time, boto3
from datetime import datetime, timezone

ACCT = '857687956942'
SRC_BUCKET = 'justhodl-dashboard-live'
DR_BUCKET = 'justhodl-dashboard-live-dr'
SRC_REGION = 'us-east-1'
DR_REGION = 'us-west-2'
ROLE_NAME = 'justhodl-s3-replication-role'
POLICY_NAME = 'justhodl-s3-replication-policy'

s3_src = boto3.client('s3', region_name=SRC_REGION)
s3_dr = boto3.client('s3', region_name=DR_REGION)
iam = boto3.client('iam', region_name=SRC_REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat()}


# === 1. Create DR bucket in us-west-2 ===
try:
    s3_dr.head_bucket(Bucket=DR_BUCKET)
    report['dr_bucket'] = 'EXISTS'
except s3_dr.exceptions.ClientError as e:
    if '404' in str(e):
        try:
            s3_dr.create_bucket(
                Bucket=DR_BUCKET,
                CreateBucketConfiguration={'LocationConstraint': DR_REGION},
            )
            report['dr_bucket'] = 'CREATED'
            time.sleep(2)
        except Exception as e2:
            report['dr_bucket'] = f'CREATE_ERR: {str(e2)[:200]}'
    else:
        report['dr_bucket'] = f'CHECK_ERR: {str(e)[:200]}'


# === 2. Enable versioning on DR bucket ===
try:
    s3_dr.put_bucket_versioning(
        Bucket=DR_BUCKET,
        VersioningConfiguration={'Status': 'Enabled'},
    )
    report['dr_versioning'] = 'ENABLED'
except Exception as e:
    report['dr_versioning'] = f'ERR: {str(e)[:200]}'


# === 3. Create IAM role for replication ===
trust_policy = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "s3.amazonaws.com"},
        "Action": "sts:AssumeRole",
    }],
}
replication_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetReplicationConfiguration",
                "s3:ListBucket",
            ],
            "Resource": f"arn:aws:s3:::{SRC_BUCKET}",
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObjectVersionForReplication",
                "s3:GetObjectVersionAcl",
                "s3:GetObjectVersionTagging",
            ],
            "Resource": f"arn:aws:s3:::{SRC_BUCKET}/*",
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ReplicateObject",
                "s3:ReplicateDelete",
                "s3:ReplicateTags",
            ],
            "Resource": f"arn:aws:s3:::{DR_BUCKET}/*",
        },
    ],
}

try:
    iam.get_role(RoleName=ROLE_NAME)
    report['iam_role'] = 'EXISTS'
except iam.exceptions.NoSuchEntityException:
    try:
        iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust_policy),
            Description='Replicate justhodl-dashboard-live → us-west-2 DR bucket',
        )
        report['iam_role'] = 'CREATED'
        time.sleep(8)  # IAM propagation
    except Exception as e:
        report['iam_role'] = f'CREATE_ERR: {str(e)[:200]}'

# Attach inline policy (idempotent)
try:
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName=POLICY_NAME,
        PolicyDocument=json.dumps(replication_policy),
    )
    report['iam_policy'] = 'ATTACHED'
except Exception as e:
    report['iam_policy'] = f'ERR: {str(e)[:200]}'

# === 4. Replication config on source bucket ===
role_arn = f"arn:aws:iam::{ACCT}:role/{ROLE_NAME}"
replication_config = {
    'Role': role_arn,
    'Rules': [{
        'ID': 'replicate-all-to-dr',
        'Priority': 1,
        'Status': 'Enabled',
        'Filter': {},
        'DeleteMarkerReplication': {'Status': 'Enabled'},
        'Destination': {
            'Bucket': f'arn:aws:s3:::{DR_BUCKET}',
            'StorageClass': 'STANDARD_IA',  # cheaper for DR
        },
    }],
}
try:
    s3_src.put_bucket_replication(
        Bucket=SRC_BUCKET,
        ReplicationConfiguration=replication_config,
    )
    report['replication_config'] = 'APPLIED'
except Exception as e:
    msg = str(e)[:300]
    report['replication_config'] = f'ERR: {msg}'

# === 5. Confirm ===
try:
    r = s3_src.get_bucket_replication(Bucket=SRC_BUCKET)
    report['final_check'] = {
        'role': r['ReplicationConfiguration']['Role'],
        'rules': len(r['ReplicationConfiguration']['Rules']),
        'first_rule_status': r['ReplicationConfiguration']['Rules'][0]['Status'],
    }
except Exception as e:
    report['final_check'] = f'ERR: {str(e)[:200]}'

# === 6. Lifecycle on DR bucket: keep old versions 90d ===
try:
    s3_dr.put_bucket_lifecycle_configuration(
        Bucket=DR_BUCKET,
        LifecycleConfiguration={
            'Rules': [{
                'ID': 'expire-noncurrent-90d',
                'Status': 'Enabled',
                'Filter': {},
                'NoncurrentVersionExpiration': {'NoncurrentDays': 90},
            }],
        },
    )
    report['dr_lifecycle'] = 'APPLIED'
except Exception as e:
    report['dr_lifecycle'] = f'ERR: {str(e)[:200]}'

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1069.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
