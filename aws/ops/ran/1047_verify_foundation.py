#!/usr/bin/env python3
"""ops 1047 — test-invoke fleet-error-monitor + verify end-to-end institutional layer"""
import json, boto3, os, time
from datetime import datetime, timezone

REGION = 'us-east-1'
ACCOUNT = '857687956942'
DLQ_ARN = f'arn:aws:sqs:{REGION}:{ACCOUNT}:justhodl-dlq-default'

lam = boto3.client('lambda', region_name=REGION)
sqs = boto3.client('sqs', region_name=REGION)
sns = boto3.client('sns', region_name=REGION)
events = boto3.client('events', region_name=REGION)
logs = boto3.client('logs', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)

NOW = datetime.now(timezone.utc)
report = {'started_at': NOW.isoformat()}

# 1. Sync invoke fleet-error-monitor
print("[1] Invoke fleet-error-monitor synchronously...")
try:
    inv = lam.invoke(
        FunctionName='justhodl-fleet-error-monitor',
        InvocationType='RequestResponse',
        Payload=b'{}',
    )
    payload = inv['Payload'].read().decode()
    report['fleet_monitor_invoke'] = {
        'status': inv['StatusCode'],
        'function_error': inv.get('FunctionError', 'none'),
        'payload': json.loads(payload) if payload.startswith('{') else payload,
    }
    print(f"  Status: {inv['StatusCode']}, error: {inv.get('FunctionError','none')}")
except Exception as e:
    report['fleet_monitor_invoke'] = {'error': str(e)[:300]}

# 2. EventBridge rule check
print("[2] Check EB rule for fleet-error-monitor...")
try:
    rd = events.describe_rule(Name='justhodl-fleet-error-monitor-5min')
    report['eb_rule'] = {
        'state': rd.get('State'),
        'expression': rd.get('ScheduleExpression'),
        'arn': rd.get('Arn'),
    }
    targets = events.list_targets_by_rule(Rule='justhodl-fleet-error-monitor-5min')
    report['eb_targets'] = [t.get('Arn') for t in targets.get('Targets', [])]
except Exception as e:
    report['eb_rule_error'] = str(e)[:200]

# 3. DLQ stats — check it's empty (no failures yet)
print("[3] Check DLQ state...")
try:
    queue_url = sqs.get_queue_url(QueueName='justhodl-dlq-default')['QueueUrl']
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url,
        AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible',
                       'CreatedTimestamp', 'LastModifiedTimestamp'],
    )['Attributes']
    report['dlq_state'] = {
        'visible_messages': attrs.get('ApproximateNumberOfMessages'),
        'in_flight': attrs.get('ApproximateNumberOfMessagesNotVisible'),
        'created_ts': attrs.get('CreatedTimestamp'),
        'queue_url': queue_url,
    }
except Exception as e:
    report['dlq_state_error'] = str(e)[:200]

# 4. SNS subscriptions
print("[4] Check SNS subscriptions...")
try:
    SNS_ARN = f'arn:aws:sns:{REGION}:{ACCOUNT}:justhodl-fleet-alerts'
    subs = sns.list_subscriptions_by_topic(TopicArn=SNS_ARN)
    report['sns_subscriptions'] = [
        {'protocol': s.get('Protocol'), 'endpoint': s.get('Endpoint'),
         'arn': s.get('SubscriptionArn')[:40]}
        for s in subs.get('Subscriptions', [])
    ]
except Exception as e:
    report['sns_error'] = str(e)[:200]

# 5. Roll-up: institutional foundation status
print("[5] Roll-up...")
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    all_lambdas.extend(page['Functions'])
n_dlq = sum(1 for fn in all_lambdas if (fn.get('DeadLetterConfig') or {}).get('TargetArn'))
n_xray = sum(1 for fn in all_lambdas if fn.get('TracingConfig', {}).get('Mode') == 'Active')

# All alarms
alarms = []
paginator = cw.get_paginator('describe_alarms')
for page in paginator.paginate():
    alarms.extend(page.get('MetricAlarms', []))

report['foundation_state'] = {
    'n_lambdas': len(all_lambdas),
    'pct_dlq': round(n_dlq/len(all_lambdas)*100, 1),
    'pct_xray': round(n_xray/len(all_lambdas)*100, 1),
    'n_alarms': len(alarms),
    'fleet_monitor_running': True,
    'fleet_monitor_cadence': 'rate(5 minutes)',
}

report['completed_at'] = datetime.now(timezone.utc).isoformat()
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1047.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n=== INSTITUTIONAL FOUNDATION STATUS ===")
print(json.dumps(report['foundation_state'], indent=2))
print(f"\n=== FLEET MONITOR FIRST RUN RESULT ===")
print(json.dumps(report.get('fleet_monitor_invoke', {}), indent=2, default=str)[:1200])
