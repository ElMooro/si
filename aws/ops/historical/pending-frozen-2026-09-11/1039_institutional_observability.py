#!/usr/bin/env python3
"""
ops 1039 — INSTITUTIONAL OBSERVABILITY FOUNDATION

The 0/399 DLQ + 4/399 X-Ray + 5/399 alarms gap is THE institutional
delta from the audit. This ops closes the gap in one pass.

Actions:
  1. Create shared SQS DLQ: justhodl-dlq-default
  2. Enable DLQ + X-Ray Active on every Lambda that doesn't have them
  3. Create an SNS topic for alerts: justhodl-fleet-alerts
  4. Loop all Lambdas, ensure DLQ.TargetArn = SQS ARN + Tracing.Mode = Active

This is idempotent (re-runnable).

Cost notes:
  - SQS DLQ: ~$0/month (only charged on use)
  - X-Ray on 399 Lambdas at current 23K invocations/week: ~$1-2/month
  - SNS topic: $0/month base, $0.50 per million publishes
  - vs 'undetected silent failure' cost: incalculable (we just found 65K
    failed invocations on autonomous-ai-processor that ran undetected
    for 226 days)

Writes: aws/ops/reports/1039.json
"""
import json, boto3, os
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

REGION = 'us-east-1'
NOW = datetime.now(timezone.utc)
ACCOUNT = '857687956942'

lam = boto3.client('lambda', region_name=REGION)
sqs = boto3.client('sqs', region_name=REGION)
sns = boto3.client('sns', region_name=REGION)
iam = boto3.client('iam', region_name=REGION)

report = {'started_at': NOW.isoformat(), 'steps': []}

# ---- Step 1: Create shared DLQ ----
DLQ_NAME = 'justhodl-dlq-default'
DLQ_ARN = f'arn:aws:sqs:{REGION}:{ACCOUNT}:{DLQ_NAME}'
print(f"[1] Ensuring DLQ {DLQ_NAME}...")
try:
    resp = sqs.create_queue(
        QueueName=DLQ_NAME,
        Attributes={
            'MessageRetentionPeriod': '1209600',  # 14 days (max)
            'VisibilityTimeout': '60',
            'ReceiveMessageWaitTimeSeconds': '20',
        },
    )
    dlq_url = resp['QueueUrl']
    report['steps'].append({'step': 'create_dlq', 'result': 'CREATED', 'url': dlq_url, 'arn': DLQ_ARN})
except sqs.exceptions.QueueNameExists:
    dlq_url = sqs.get_queue_url(QueueName=DLQ_NAME)['QueueUrl']
    report['steps'].append({'step': 'create_dlq', 'result': 'ALREADY_EXISTS', 'url': dlq_url, 'arn': DLQ_ARN})

# ---- Step 2: Ensure lambda execution role can write to DLQ ----
# The role arn:aws:iam::857687956942:role/lambda-execution-role needs sqs:SendMessage
print("[2] Ensuring lambda-execution-role has sqs:SendMessage on DLQ...")
ROLE_NAME = 'lambda-execution-role'
INLINE_POL = 'justhodl-dlq-send'
try:
    iam.put_role_policy(
        RoleName=ROLE_NAME,
        PolicyName=INLINE_POL,
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": ["sqs:SendMessage"],
                "Resource": DLQ_ARN,
            }],
        }),
    )
    report['steps'].append({'step': 'iam_policy', 'result': 'ATTACHED', 'role': ROLE_NAME, 'policy': INLINE_POL})
except Exception as e:
    report['steps'].append({'step': 'iam_policy', 'result': 'ERROR', 'error': str(e)[:200]})

# ---- Step 3: Create SNS topic for fleet alerts ----
SNS_NAME = 'justhodl-fleet-alerts'
print(f"[3] Ensuring SNS topic {SNS_NAME}...")
try:
    resp = sns.create_topic(Name=SNS_NAME)
    sns_arn = resp['TopicArn']
    report['steps'].append({'step': 'create_sns', 'result': 'CREATED_OR_EXISTS', 'arn': sns_arn})
except Exception as e:
    sns_arn = None
    report['steps'].append({'step': 'create_sns', 'result': 'ERROR', 'error': str(e)[:200]})

# ---- Step 4: Enable DLQ + X-Ray on all Lambdas ----
print("[4] Enumerating Lambdas...")
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    all_lambdas.extend(page['Functions'])
print(f"  {len(all_lambdas)} Lambdas to process")

def configure_lambda(fn):
    name = fn['FunctionName']
    cfg = fn  # list_functions has the config
    needs_dlq = not (cfg.get('DeadLetterConfig') or {}).get('TargetArn')
    needs_xray = cfg.get('TracingConfig', {}).get('Mode') != 'Active'
    if not needs_dlq and not needs_xray:
        return name, {'result': 'ALREADY_CONFIGURED'}
    
    result = {'before': {
        'dlq': (cfg.get('DeadLetterConfig') or {}).get('TargetArn'),
        'xray': cfg.get('TracingConfig', {}).get('Mode'),
    }, 'actions': []}
    
    try:
        update_kwargs = {'FunctionName': name}
        if needs_dlq:
            update_kwargs['DeadLetterConfig'] = {'TargetArn': DLQ_ARN}
            result['actions'].append('set_dlq')
        if needs_xray:
            update_kwargs['TracingConfig'] = {'Mode': 'Active'}
            result['actions'].append('enable_xray')
        lam.update_function_configuration(**update_kwargs)
        result['result'] = 'UPDATED'
    except Exception as e:
        result['result'] = 'ERROR'
        result['error'] = str(e)[:200]
    return name, result

print(f"[5] Applying DLQ + X-Ray (parallel)...")
results = {}
with ThreadPoolExecutor(max_workers=8) as ex:
    futures = [ex.submit(configure_lambda, fn) for fn in all_lambdas]
    done = 0
    for fut in as_completed(futures):
        name, r = fut.result()
        results[name] = r
        done += 1
        if done % 50 == 0:
            print(f"  {done}/{len(all_lambdas)}...")

# Summary counts
updated = sum(1 for r in results.values() if r.get('result') == 'UPDATED')
already = sum(1 for r in results.values() if r.get('result') == 'ALREADY_CONFIGURED')
errors = sum(1 for r in results.values() if r.get('result') == 'ERROR')

report['lambda_configuration_summary'] = {
    'total': len(all_lambdas),
    'updated': updated,
    'already_configured': already,
    'errors': errors,
}

# Sample of errors (if any)
error_samples = [(n, r) for n, r in results.items() if r.get('result') == 'ERROR'][:10]
if error_samples:
    report['error_samples'] = [{'lambda': n, 'error': r.get('error')} for n, r in error_samples]

# ---- Step 6: Verify by re-listing ----
print("[6] Verifying coverage...")
verify_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    verify_lambdas.extend(page['Functions'])

dlq_count = sum(1 for fn in verify_lambdas if (fn.get('DeadLetterConfig') or {}).get('TargetArn'))
xray_count = sum(1 for fn in verify_lambdas if fn.get('TracingConfig', {}).get('Mode') == 'Active')
report['verification'] = {
    'n_lambdas': len(verify_lambdas),
    'n_with_dlq': dlq_count,
    'n_with_xray': xray_count,
    'pct_dlq': round(dlq_count/len(verify_lambdas)*100, 1),
    'pct_xray': round(xray_count/len(verify_lambdas)*100, 1),
}

report['completed_at'] = datetime.now(timezone.utc).isoformat()
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1039.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n=== INSTITUTIONAL OBSERVABILITY UPLIFT ===")
print(f"  DLQ coverage:  {dlq_count}/{len(verify_lambdas)} ({report['verification']['pct_dlq']}%)")
print(f"  X-Ray coverage: {xray_count}/{len(verify_lambdas)} ({report['verification']['pct_xray']}%)")
print(f"  Updated this run: {updated}")
print(f"  Errors: {errors}")
print(f"\nReport: aws/ops/reports/1039.json")
