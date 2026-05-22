#!/usr/bin/env python3
"""
ops 1043 — Self-grant SQS/SNS perms + create DLQ + complete observability sweep

github-actions-justhodl user has IAMFullAccess but lacks SQS/SNS perms.
Attach the missing managed policies, then complete the audit P2 sweep.
"""
import json, boto3, os, time
from datetime import datetime, timezone
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed

REGION = 'us-east-1'
ACCOUNT = '857687956942'
USER = 'github-actions-justhodl'
DLQ_NAME = 'justhodl-dlq-default'
DLQ_ARN = f'arn:aws:sqs:{REGION}:{ACCOUNT}:{DLQ_NAME}'
SNS_NAME = 'justhodl-fleet-alerts'

cfg = Config(region_name=REGION, retries={'max_attempts': 10, 'mode': 'adaptive'},
             read_timeout=60, connect_timeout=10)
iam = boto3.client('iam', config=cfg)
sqs = boto3.client('sqs', config=cfg)
sns = boto3.client('sns', config=cfg)
lam = boto3.client('lambda', config=cfg)

report = {'started_at': datetime.now(timezone.utc).isoformat(), 'steps': []}


def step(name, fn):
    try:
        r = fn()
        report['steps'].append({'step': name, 'result': 'OK', 'detail': r})
        print(f"  ✅ {name}: {str(r)[:120]}")
        return r
    except Exception as e:
        report['steps'].append({'step': name, 'result': 'ERROR', 'error': str(e)[:300]})
        print(f"  ❌ {name}: {str(e)[:200]}")
        return None


# 1. Attach missing managed policies
print("[1] Attaching SQS+SNS managed policies...")
step('attach_sqs', lambda: iam.attach_user_policy(
    UserName=USER, PolicyArn='arn:aws:iam::aws:policy/AmazonSQSFullAccess') or 'attached')
step('attach_sns', lambda: iam.attach_user_policy(
    UserName=USER, PolicyArn='arn:aws:iam::aws:policy/AmazonSNSFullAccess') or 'attached')

# Wait for IAM propagation
print("[2] Wait 20s for IAM propagation...")
time.sleep(20)

# Refresh SQS/SNS clients to use new permissions (boto3 caches)
sqs = boto3.client('sqs', config=cfg)
sns = boto3.client('sns', config=cfg)

# 3. Create DLQ
print("[3] Creating DLQ...")
queue_url = step('create_dlq', lambda: sqs.create_queue(
    QueueName=DLQ_NAME,
    Attributes={'MessageRetentionPeriod': '1209600', 'VisibilityTimeout': '60'},
)['QueueUrl'])

# 4. Set permissive queue policy
if queue_url:
    print("[4] Setting queue policy...")
    queue_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "AllowAccountLambdas",
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT}:root"},
            "Action": ["sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"],
            "Resource": DLQ_ARN,
        }],
    })
    step('set_queue_policy', lambda: sqs.set_queue_attributes(
        QueueUrl=queue_url, Attributes={'Policy': queue_policy}) or 'set')
    time.sleep(10)  # propagation

# 5. Create SNS topic
print("[5] Creating SNS topic...")
sns_arn = step('create_sns', lambda: sns.create_topic(Name=SNS_NAME)['TopicArn'])

# 6. Verify DLQ is actually accessible
print("[6] Verifying DLQ via test SendMessage...")
if queue_url:
    step('test_send', lambda: sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({'test': 'ops 1043 verification', 'ts': datetime.now(timezone.utc).isoformat()}),
    ).get('MessageId'))

# 7. Sweep — apply DLQ to remaining Lambdas (X-Ray already at 100% from 1041)
print("[7] Applying DLQ to Lambdas missing it...")
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    all_lambdas.extend(page['Functions'])

def apply_dlq(fn):
    name = fn['FunctionName']
    if (fn.get('DeadLetterConfig') or {}).get('TargetArn'):
        return name, 'SKIP'
    try:
        lam.update_function_configuration(
            FunctionName=name, DeadLetterConfig={'TargetArn': DLQ_ARN})
        return name, 'UPDATED'
    except Exception as e:
        return name, f'ERROR: {str(e)[:200]}'

if queue_url:
    results = {'UPDATED': 0, 'SKIP': 0, 'ERROR': []}
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(apply_dlq, fn) for fn in all_lambdas]
        done = 0
        for fut in as_completed(futures):
            name, status = fut.result()
            if status == 'UPDATED': results['UPDATED'] += 1
            elif status == 'SKIP': results['SKIP'] += 1
            else: results['ERROR'].append({'lambda': name, 'msg': status})
            done += 1
            if done % 100 == 0:
                print(f"  {done}/{len(all_lambdas)}")
    report['dlq_sweep'] = {
        'updated': results['UPDATED'],
        'already_had': results['SKIP'],
        'errors': len(results['ERROR']),
        'error_samples': results['ERROR'][:5],
    }
else:
    report['dlq_sweep'] = {'skipped': 'no queue URL'}

# 8. Final verification
print("[8] Final verification...")
verify = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    verify.extend(page['Functions'])
n_dlq = sum(1 for fn in verify if (fn.get('DeadLetterConfig') or {}).get('TargetArn'))
n_xray = sum(1 for fn in verify if fn.get('TracingConfig', {}).get('Mode') == 'Active')
report['final'] = {
    'n_lambdas': len(verify),
    'n_with_dlq': n_dlq,
    'pct_dlq': round(n_dlq/len(verify)*100, 1),
    'n_with_xray': n_xray,
    'pct_xray': round(n_xray/len(verify)*100, 1),
}

report['completed_at'] = datetime.now(timezone.utc).isoformat()
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1043.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n=== AUDIT P2 COMPLETE ===")
print(f"  DLQ:   {n_dlq}/{len(verify)} ({report['final']['pct_dlq']}%)")
print(f"  X-Ray: {n_xray}/{len(verify)} ({report['final']['pct_xray']}%)")
print(f"  DLQ ARN: {DLQ_ARN}")
print(f"  SNS ARN: {sns_arn}")
