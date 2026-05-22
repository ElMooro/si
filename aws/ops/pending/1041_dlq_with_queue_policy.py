#!/usr/bin/env python3
"""
ops 1041 — Set DLQ queue policy + retry observability sweep

Root cause of 1040 failures: UpdateFunctionConfiguration validates DLQ
access by sending a test message under the Lambda's OWN execution role.
Most Lambdas use family-specific roles (not just lambda-execution-role).
Solution: set a permissive queue policy on the DLQ itself.

This ops:
  1. Set SQS queue policy on justhodl-dlq-default allowing all roles
     in account 857687956942 to SendMessage
  2. Probe: discover all execution roles in use across the 399 Lambdas
  3. Wait 30s for IAM/SQS propagation
  4. Retry the sweep with adaptive retry
"""
import json, boto3, os, time
from datetime import datetime, timezone
from botocore.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter

REGION = 'us-east-1'
ACCOUNT = '857687956942'
DLQ_NAME = 'justhodl-dlq-default'
DLQ_ARN = f'arn:aws:sqs:{REGION}:{ACCOUNT}:{DLQ_NAME}'

cfg = Config(region_name=REGION, retries={'max_attempts': 10, 'mode': 'adaptive'},
             read_timeout=60, connect_timeout=10)
lam = boto3.client('lambda', config=cfg)
sqs = boto3.client('sqs', config=cfg)
sns = boto3.client('sns', config=cfg)

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# ---- Step 1: Get DLQ URL, set permissive queue policy ----
print("[1] Setting DLQ queue policy...")
try:
    queue_url = sqs.get_queue_url(QueueName=DLQ_NAME)['QueueUrl']
    queue_policy = {
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "AllowAccountLambdas",
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{ACCOUNT}:root"},
            "Action": ["sqs:SendMessage", "sqs:GetQueueAttributes", "sqs:GetQueueUrl"],
            "Resource": DLQ_ARN,
        }],
    }
    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={'Policy': json.dumps(queue_policy)},
    )
    report['queue_policy_set'] = True
except Exception as e:
    report['queue_policy_error'] = str(e)[:300]

# ---- Step 2: Discover roles in use ----
print("[2] Discovering execution roles in use...")
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    all_lambdas.extend(page['Functions'])

roles = Counter()
for fn in all_lambdas:
    role_arn = fn.get('Role', '')
    role_name = role_arn.split('/')[-1] if role_arn else 'unknown'
    roles[role_name] += 1
report['execution_roles_in_use'] = dict(roles.most_common(20))
report['n_unique_roles'] = len(roles)
print(f"  {len(roles)} unique roles in use:")
for r, c in roles.most_common(10):
    print(f"    {c:>3}× {r}")

# ---- Step 3: Wait for propagation ----
print("[3] Waiting 30s for queue policy propagation...")
time.sleep(30)

# ---- Step 4: Ensure SNS topic ----
print("[4] Ensuring SNS topic justhodl-fleet-alerts...")
try:
    resp = sns.create_topic(Name='justhodl-fleet-alerts')
    report['sns_arn'] = resp['TopicArn']
except Exception as e:
    report['sns_error'] = str(e)[:300]

# ---- Step 5: Sweep — apply DLQ + X-Ray ----
print(f"[5] Sweeping {len(all_lambdas)} Lambdas...")

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
        # Try just X-Ray if DLQ failed (some Lambdas may have restrictive roles)
        if needs_xray and needs_dlq:
            try:
                lam.update_function_configuration(
                    FunctionName=name,
                    TracingConfig={'Mode': 'Active'},
                )
                return name, 'XRAY_ONLY'
            except Exception as e2:
                pass
        return name, f'ERROR: {str(e)[:200]}'

results = {'UPDATED': [], 'SKIP': [], 'XRAY_ONLY': [], 'ERROR': []}
with ThreadPoolExecutor(max_workers=4) as ex:
    futures = [ex.submit(apply_config, fn) for fn in all_lambdas]
    done = 0
    for fut in as_completed(futures):
        name, status = fut.result()
        if status == 'UPDATED': results['UPDATED'].append(name)
        elif status == 'SKIP': results['SKIP'].append(name)
        elif status == 'XRAY_ONLY': results['XRAY_ONLY'].append(name)
        else: results['ERROR'].append({'lambda': name, 'msg': status})
        done += 1
        if done % 50 == 0:
            print(f"  {done}/{len(all_lambdas)} (updated={len(results['UPDATED'])}  xray_only={len(results['XRAY_ONLY'])}  err={len(results['ERROR'])})")

report['sweep_results'] = {
    'updated_full': len(results['UPDATED']),
    'xray_only': len(results['XRAY_ONLY']),
    'already_configured': len(results['SKIP']),
    'errors': len(results['ERROR']),
    'error_samples': results['ERROR'][:8],
}

# ---- Step 6: Final verification ----
print("[6] Verifying final state...")
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
with open('aws/ops/reports/1041.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n=== FINAL ===")
print(f"  DLQ:   {n_dlq}/{len(verify)} ({report['final']['pct_dlq']}%)")
print(f"  X-Ray: {n_xray}/{len(verify)} ({report['final']['pct_xray']}%)")
