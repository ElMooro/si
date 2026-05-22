#!/usr/bin/env python3
"""ops 1040 — Probe state after 1039 failed; rerun the sweep defensively."""
import json, boto3, os, time
from datetime import datetime, timezone
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed

REGION = 'us-east-1'
NOW = datetime.now(timezone.utc)
ACCOUNT = '857687956942'
DLQ_NAME = 'justhodl-dlq-default'
DLQ_ARN = f'arn:aws:sqs:{REGION}:{ACCOUNT}:{DLQ_NAME}'
SNS_NAME = 'justhodl-fleet-alerts'

# Aggressive retry config to handle Lambda API throttling
cfg = Config(
    region_name=REGION,
    retries={'max_attempts': 10, 'mode': 'adaptive'},
    read_timeout=60,
    connect_timeout=10,
)

lam = boto3.client('lambda', config=cfg)
sqs = boto3.client('sqs', config=cfg)
sns = boto3.client('sns', config=cfg)
iam = boto3.client('iam', config=cfg)

report = {'started_at': NOW.isoformat(), 'probe': {}, 'rerun': {}}

# ---- Probe current state ----
print("[PROBE] Current state...")
try:
    queue_url = sqs.get_queue_url(QueueName=DLQ_NAME)['QueueUrl']
    report['probe']['dlq'] = {'exists': True, 'url': queue_url}
except sqs.exceptions.QueueDoesNotExist:
    report['probe']['dlq'] = {'exists': False}
except Exception as e:
    report['probe']['dlq'] = {'error': str(e)[:200]}

try:
    topics = sns.list_topics()['Topics']
    sns_arn = None
    for t in topics:
        if t['TopicArn'].endswith(':'+SNS_NAME):
            sns_arn = t['TopicArn']; break
    report['probe']['sns'] = {'arn': sns_arn} if sns_arn else {'exists': False}
except Exception as e:
    report['probe']['sns'] = {'error': str(e)[:200]}

# Get count of Lambdas already configured
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    all_lambdas.extend(page['Functions'])
report['probe']['n_lambdas'] = len(all_lambdas)
n_dlq = sum(1 for fn in all_lambdas if (fn.get('DeadLetterConfig') or {}).get('TargetArn'))
n_xray = sum(1 for fn in all_lambdas if fn.get('TracingConfig', {}).get('Mode') == 'Active')
report['probe']['n_with_dlq'] = n_dlq
report['probe']['n_with_xray'] = n_xray
print(f"  Lambdas: {len(all_lambdas)} | DLQ: {n_dlq} | X-Ray: {n_xray}")

# ---- Step 1: Ensure DLQ exists ----
print("[1] Ensuring DLQ...")
if not report['probe']['dlq'].get('exists'):
    try:
        sqs.create_queue(
            QueueName=DLQ_NAME,
            Attributes={'MessageRetentionPeriod': '1209600', 'VisibilityTimeout': '60'},
        )
        report['rerun']['dlq_created'] = True
    except Exception as e:
        report['rerun']['dlq_error'] = str(e)[:200]

# ---- Step 2: Ensure IAM policy ----
print("[2] Ensuring IAM policy on lambda-execution-role...")
try:
    iam.put_role_policy(
        RoleName='lambda-execution-role',
        PolicyName='justhodl-dlq-send',
        PolicyDocument=json.dumps({
            "Version": "2012-10-17",
            "Statement": [{"Effect": "Allow", "Action": ["sqs:SendMessage"], "Resource": DLQ_ARN}],
        }),
    )
    report['rerun']['iam_policy'] = 'ATTACHED'
except Exception as e:
    report['rerun']['iam_policy_error'] = str(e)[:200]

# ---- Step 3: Ensure SNS ----
print("[3] Ensuring SNS topic...")
try:
    resp = sns.create_topic(Name=SNS_NAME)
    report['rerun']['sns_arn'] = resp['TopicArn']
except Exception as e:
    report['rerun']['sns_error'] = str(e)[:200]

# ---- Step 4: Apply DLQ + X-Ray to remaining Lambdas (idempotent) ----
print("[4] Applying DLQ + X-Ray to Lambdas needing it...")

def apply_config(fn):
    name = fn['FunctionName']
    needs_dlq = not (fn.get('DeadLetterConfig') or {}).get('TargetArn')
    needs_xray = fn.get('TracingConfig', {}).get('Mode') != 'Active'
    if not needs_dlq and not needs_xray:
        return name, 'SKIP'
    kwargs = {'FunctionName': name}
    if needs_dlq:
        kwargs['DeadLetterConfig'] = {'TargetArn': DLQ_ARN}
    if needs_xray:
        kwargs['TracingConfig'] = {'Mode': 'Active'}
    try:
        lam.update_function_configuration(**kwargs)
        return name, 'UPDATED'
    except Exception as e:
        return name, f'ERROR: {str(e)[:150]}'

results = {'UPDATED': [], 'SKIP': [], 'ERROR': []}
# Lower concurrency: 4 workers to avoid throttling
with ThreadPoolExecutor(max_workers=4) as ex:
    futures = [ex.submit(apply_config, fn) for fn in all_lambdas]
    done = 0
    for fut in as_completed(futures):
        name, status = fut.result()
        if status == 'UPDATED': results['UPDATED'].append(name)
        elif status == 'SKIP': results['SKIP'].append(name)
        else: results['ERROR'].append({'lambda': name, 'msg': status})
        done += 1
        if done % 50 == 0:
            print(f"  {done}/{len(all_lambdas)}")

report['rerun']['lambda_config'] = {
    'updated': len(results['UPDATED']),
    'already_configured': len(results['SKIP']),
    'errors': len(results['ERROR']),
    'error_samples': results['ERROR'][:10],
}

# ---- Step 5: Final verification ----
print("[5] Final verification...")
verify = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    verify.extend(page['Functions'])
n_dlq2 = sum(1 for fn in verify if (fn.get('DeadLetterConfig') or {}).get('TargetArn'))
n_xray2 = sum(1 for fn in verify if fn.get('TracingConfig', {}).get('Mode') == 'Active')
report['verification'] = {
    'n_lambdas': len(verify),
    'n_with_dlq': n_dlq2,
    'n_with_xray': n_xray2,
    'pct_dlq': round(n_dlq2/len(verify)*100, 1),
    'pct_xray': round(n_xray2/len(verify)*100, 1),
}

report['completed_at'] = datetime.now(timezone.utc).isoformat()
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1040.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n=== FINAL ===")
print(f"  DLQ:   {n_dlq2}/{len(verify)} ({report['verification']['pct_dlq']}%)")
print(f"  X-Ray: {n_xray2}/{len(verify)} ({report['verification']['pct_xray']}%)")
print(f"  Updated this run: {len(results['UPDATED'])}")
print(f"  Errors: {len(results['ERROR'])}")
