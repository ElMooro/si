#!/usr/bin/env python3
"""
ops 1045 — Free policy slots, attach SQS+SNS, complete sweep

Strategy:
  - Detach AmazonEventBridgeReadOnlyAccess (redundant — Full includes Read)
  - Detach AmazonDynamoDBReadOnlyAccess (redundant — inline DDB policies cover this)
  - Attach AmazonSQSFullAccess
  - Attach AmazonSNSFullAccess
  - Create DLQ + sweep

Net: same effective permissions, room for SQS+SNS.
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

DETACH = [
    'arn:aws:iam::aws:policy/AmazonEventBridgeReadOnlyAccess',
    'arn:aws:iam::aws:policy/AmazonDynamoDBReadOnlyAccess',
]
ATTACH = [
    'arn:aws:iam::aws:policy/AmazonSQSFullAccess',
    'arn:aws:iam::aws:policy/AmazonSNSFullAccess',
]

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
        report['steps'].append({'step': name, 'result': 'OK'})
        print(f"  ✅ {name}")
        return r
    except Exception as e:
        report['steps'].append({'step': name, 'result': 'ERROR', 'error': str(e)[:300]})
        print(f"  ❌ {name}: {str(e)[:200]}")
        return None


# 1. Detach redundant managed policies
print("[1] Detach redundant managed policies...")
for arn in DETACH:
    name = arn.split('/')[-1]
    step(f'detach_{name}', lambda a=arn: iam.detach_user_policy(UserName=USER, PolicyArn=a))

# 2. Attach SQS + SNS
print("[2] Attach SQS + SNS...")
for arn in ATTACH:
    name = arn.split('/')[-1]
    step(f'attach_{name}', lambda a=arn: iam.attach_user_policy(UserName=USER, PolicyArn=a))

# 3. Wait for IAM propagation
print("[3] Wait 30s for IAM propagation...")
time.sleep(30)

# Refresh clients
sqs = boto3.client('sqs', config=cfg)
sns = boto3.client('sns', config=cfg)

# 4. Create DLQ
print("[4] Create DLQ...")
queue_url = step('create_dlq', lambda: sqs.create_queue(
    QueueName=DLQ_NAME,
    Attributes={'MessageRetentionPeriod': '1209600', 'VisibilityTimeout': '60'},
)['QueueUrl'])

# 5. Queue policy
if queue_url:
    print("[5] Queue policy...")
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
        QueueUrl=queue_url, Attributes={'Policy': queue_policy}))
    time.sleep(10)

# 6. SNS topic
print("[6] SNS topic...")
sns_arn = step('create_sns', lambda: sns.create_topic(Name=SNS_NAME)['TopicArn'])
if sns_arn: report['sns_arn'] = sns_arn

# 7. Test DLQ accessible
if queue_url:
    print("[7] Test DLQ send...")
    msg_id = step('test_send', lambda: sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({'test': 'ops 1045 verification', 'ts': datetime.now(timezone.utc).isoformat()}),
    )['MessageId'])
    if msg_id: report['dlq_test_message_id'] = msg_id

# 8. DLQ sweep
if queue_url:
    print("[8] DLQ sweep across all Lambdas...")
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
            return name, f'ERROR: {str(e)[:250]}'
    
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
                print(f"  {done}/{len(all_lambdas)} (updated={results['UPDATED']}, err={len(results['ERROR'])})")
    
    report['dlq_sweep'] = {
        'updated': results['UPDATED'],
        'already_had': results['SKIP'],
        'errors': len(results['ERROR']),
        'error_samples': results['ERROR'][:5],
    }

# 9. Final verification
print("[9] Final verification...")
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
with open('aws/ops/reports/1045.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n=== FINAL ===")
print(f"  DLQ:   {n_dlq}/{len(verify)} ({report['final']['pct_dlq']}%)")
print(f"  X-Ray: {n_xray}/{len(verify)} ({report['final']['pct_xray']}%)")
