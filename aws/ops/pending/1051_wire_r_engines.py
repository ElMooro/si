#!/usr/bin/env python3
"""
ops 1051 — Wire 9 cold R-series Research Engines with EventBridge schedules

The May 2026 audit found 9 R-series engines deployed but never invoked:
  R1 justhodl-sec-filing-diff
  R2 justhodl-transcript-query
  R3 justhodl-peer-comparison
  R4 justhodl-screen-builder
  R5 justhodl-fedwatch-rate-probability
  R6 justhodl-supply-chain-linkage
  R7 justhodl-cftc-deep-view
  R9 justhodl-fx-decomposition
  R10 justhodl-factor-decomposition

This script creates EventBridge Scheduler schedules for each, idempotently.
Daily at 06:00 UTC for market-data-driven engines, weekly Monday 07:00 UTC
for research-heavy engines.

Idempotent: re-running updates existing schedules.
Writes: aws/ops/reports/1051.json (via S3)
"""

import json
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

REGION = 'us-east-1'
ACCOUNT = '857687956942'
ROLE_ARN = f'arn:aws:iam::{ACCOUNT}:role/lambda-execution-role'

# Engine -> schedule expression and description
# Using EventBridge Scheduler rate/cron expressions
SCHEDULES = {
    # Daily market-data engines (06:00 UTC daily)
    'justhodl-factor-decomposition': {
        'schedule': 'cron(0 6 * * ? *)',
        'description': 'R10 factor decomposition - daily',
    },
    'justhodl-fx-decomposition': {
        'schedule': 'cron(15 6 * * ? *)',
        'description': 'R9 FX decomposition - daily',
    },
    'justhodl-fedwatch-rate-probability': {
        'schedule': 'cron(30 6 * * ? *)',
        'description': 'R5 FedWatch rate probability - daily',
    },
    'justhodl-cftc-deep-view': {
        'schedule': 'cron(45 6 * * ? *)',
        'description': 'R7 CFTC deep view - daily (CFTC publishes Fri, but daily check is cheap)',
    },
    # Weekly research engines (Monday 07:00 UTC, staggered)
    'justhodl-sec-filing-diff': {
        'schedule': 'cron(0 7 ? * MON *)',
        'description': 'R1 SEC filing diff - weekly Monday',
    },
    'justhodl-transcript-query': {
        'schedule': 'cron(15 7 ? * MON *)',
        'description': 'R2 transcript query - weekly Monday',
    },
    'justhodl-peer-comparison': {
        'schedule': 'cron(30 7 ? * MON *)',
        'description': 'R3 peer comparison - weekly Monday',
    },
    'justhodl-screen-builder': {
        'schedule': 'cron(45 7 ? * MON *)',
        'description': 'R4 screen builder - weekly Monday',
    },
    'justhodl-supply-chain-linkage': {
        'schedule': 'cron(0 8 ? * MON *)',
        'description': 'R6 supply-chain linkage - weekly Monday',
    },
}

def main():
    lam = boto3.client('lambda', region_name=REGION)
    scheduler = boto3.client('scheduler', region_name=REGION)

    report = {
        'started_at': datetime.now(timezone.utc).isoformat(),
        'schedules': {},
    }

    # Verify each Lambda exists
    print("Verifying Lambda functions exist...")
    existing = set()
    paginator = lam.get_paginator('list_functions')
    for page in paginator.paginate():
        for fn in page['Functions']:
            existing.add(fn['FunctionName'])

    for engine, cfg in SCHEDULES.items():
        if engine not in existing:
            print(f"  SKIP {engine}: Lambda not found")
            report['schedules'][engine] = {'result': 'LAMBDA_NOT_FOUND'}
            continue

        lambda_arn = f'arn:aws:lambda:{REGION}:{ACCOUNT}:function:{engine}'
        schedule_name = f'justhodl-{engine}-schedule'

        try:
            # Try to get existing schedule
            try:
                scheduler.get_schedule(Name=schedule_name)
                exists = True
            except scheduler.exceptions.ResourceNotFoundException:
                exists = False

            schedule_kwargs = {
                'Name': schedule_name,
                'ScheduleExpression': cfg['schedule'],
                'Target': {
                    'Arn': lambda_arn,
                    'RoleArn': ROLE_ARN,
                },
                'FlexibleTimeWindow': {'Mode': 'OFF'},
                'Description': cfg['description'],
            }

            if exists:
                scheduler.update_schedule(**schedule_kwargs)
                result = 'UPDATED'
            else:
                scheduler.create_schedule(**schedule_kwargs)
                result = 'CREATED'

            print(f"  {result} {schedule_name}: {cfg['schedule']}")
            report['schedules'][engine] = {
                'result': result,
                'schedule': cfg['schedule'],
                'schedule_name': schedule_name,
            }

        except ClientError as e:
            err = str(e)[:300]
            print(f"  ERROR {engine}: {err}")
            report['schedules'][engine] = {'result': 'ERROR', 'error': err}
        except Exception as e:
            err = str(e)[:300]
            print(f"  ERROR {engine}: {err}")
            report['schedules'][engine] = {'result': 'ERROR', 'error': err}

    report['completed_at'] = datetime.now(timezone.utc).isoformat()

    # Write report to S3
    s3 = boto3.client('s3', region_name=REGION)
    try:
        s3.put_object(
            Bucket='justhodl-dashboard-live',
            Key='ops/reports/1051.json',
            Body=json.dumps(report, indent=2),
            ContentType='application/json',
        )
        print("\nReport written to s3://justhodl-dashboard-live/ops/reports/1051.json")
    except Exception as e:
        print(f"\nWarning: could not write S3 report: {e}")

    # Summary
    created = sum(1 for v in report['schedules'].values() if v['result'] == 'CREATED')
    updated = sum(1 for v in report['schedules'].values() if v['result'] == 'UPDATED')
    errors = sum(1 for v in report['schedules'].values() if v['result'] == 'ERROR')
    missing = sum(1 for v in report['schedules'].values() if v['result'] == 'LAMBDA_NOT_FOUND')
    print(f"\nSummary: {created} created, {updated} updated, {errors} errors, {missing} missing")

if __name__ == '__main__':
    main()
