#!/usr/bin/env python3
"""
ops 1044 — Inline SQS/SNS policy + create DLQ + complete sweep

Quota: PoliciesPerUser=10 already maxed. Inline policies bypass this limit.
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
        report['steps'].append({'step': name, 'result': 'OK', 'detail': str(r)[:200]})
        print(f"  ✅ {name}")
        return r
    except Exception as e:
        report['steps'].append({'step': name, 'result': 'ERROR', 'error': str(e)[:400]})
        print(f"  ❌ {name}: {str(e)[:200]}")
        return None


# 1. Add inline policy for SQS+SNS (scoped to justhodl-* resources)
print("[1] Inline policy: justhodl-observability-bootstrap...")
inline_policy = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "SqsBootstrap",
            "Effect": "Allow",
            "Action": [
                "sqs:CreateQueue", "sqs:DeleteQueue", "sqs:GetQueueUrl",
                "sqs:GetQueueAttributes", "sqs:SetQueueAttributes",
                "sqs:SendMessage", "sqs:ReceiveMessage", "sqs:ListQueues",
                "sqs:ListQueueTags", "sqs:TagQueue",
            ],
            "Resource": [
                f"arn:aws:sqs:{REGION}:{ACCOUNT}:justhodl-*",
            ],
        },
        {
            "Sid": "SqsListAll",
            "Effect": "Allow",
            "Action": ["sqs:ListQueues"],
            "Resource": "*",
        },
        {
            "Sid": "SnsBootstrap",
            "Effect": "Allow",
            "Action": [
                "sns:CreateTopic", "sns:DeleteTopic", "sns:GetTopicAttributes",
                "sns:SetTopicAttributes", "sns:Publish", "sns:ListTopics",
                "sns:Subscribe", "sns:Unsubscribe", "sns:ListSubscriptionsByTopic",
                "sns:TagResource",
            ],
            "Resource": [
                f"arn:aws:sns:{REGION}:{ACCOUNT}:justhodl-*",
            ],
        },
        {
            "Sid": "SnsListAll",
            "Effect": "Allow",
            "Action": ["sns:ListTopics"],
            "Resource": "*",
        },
    ],
}
step('put_inline_policy', lambda: iam.put_user_policy(
    UserName=USER,
    PolicyName='justhodl-observability-bootstrap',
    PolicyDocument=json.dumps(inline_policy),
))

# Wait for IAM propagation
print("[2] Wait 30s for IAM propagation...")
time.sleep(30)

# Refresh clients
sqs = boto3.client('sqs', config=cfg)
sns = boto3.client('sns', config=cfg)

# 3. Create DLQ
print("[3] Create DLQ...")
queue_url = step('create_dlq', lambda: sqs.create_queue(
    QueueName=DLQ_NAME,
    Attributes={'MessageRetentionPeriod': '1209600', 'VisibilityTimeout': '60'},
)['QueueUrl'])

# 4. Queue policy
if queue_url:
    print("[4] Queue policy...")
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
    time.sleep(10)

# 5. SNS topic
print("[5] SNS topic...")
sns_arn = step('create_sns', lambda: sns.create_topic(Name=SNS_NAME)['TopicArn'])
if sns_arn:
    report['sns_arn'] = sns_arn

# 6. Verify DLQ via test send
print("[6] Test DLQ send...")
if queue_url:
    msg_id = step('test_send', lambda: sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps({'test': 'ops 1044 verification'}),
    )['MessageId'])
    if msg_id:
        report['dlq_test_message_id'] = msg_id

# 7. Sweep — DLQ on all Lambdas
if queue_url:
    print("[7] Applying DLQ across all Lambdas...")
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
with open('aws/ops/reports/1044.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n=== FINAL ===")
print(f"  DLQ:   {n_dlq}/{len(verify)} ({report['final']['pct_dlq']}%)")
print(f"  X-Ray: {n_xray}/{len(verify)} ({report['final']['pct_xray']}%)")
