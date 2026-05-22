#!/usr/bin/env python3
"""ops 1051 — verify audit closure: fleet monitors alive + delta vs baseline"""
import json, boto3, os
from datetime import datetime, timezone, timedelta

REGION = 'us-east-1'
NOW = datetime.now(timezone.utc)
lam = boto3.client('lambda', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)
sqs = boto3.client('sqs', region_name=REGION)
sns = boto3.client('sns', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

report = {'started_at': NOW.isoformat()}

# 1. Fleet monitors alive and recently invoked?
print("[1] Fleet monitor health...")
end = NOW; start = end - timedelta(hours=2)
for name in ['justhodl-fleet-error-monitor', 'justhodl-fleet-freshness-monitor']:
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        m = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Invocations',
            Dimensions=[{'Name': 'FunctionName', 'Value': name}],
            StartTime=start, EndTime=end, Period=900, Statistics=['Sum'],
        )
        inv = int(sum(p['Sum'] for p in m['Datapoints']))
        m_err = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Errors',
            Dimensions=[{'Name': 'FunctionName', 'Value': name}],
            StartTime=start, EndTime=end, Period=900, Statistics=['Sum'],
        )
        err = int(sum(p['Sum'] for p in m_err['Datapoints']))
        report[name] = {
            'exists': True,
            'last_modified': cfg.get('LastModified'),
            'invocations_2h': inv,
            'errors_2h': err,
            'dlq': bool((cfg.get('DeadLetterConfig') or {}).get('TargetArn')),
            'xray': cfg.get('TracingConfig', {}).get('Mode') == 'Active',
        }
    except lam.exceptions.ResourceNotFoundException:
        report[name] = {'exists': False}
    except Exception as e:
        report[name] = {'error': str(e)[:200]}

# 2. DLQ status
print("[2] DLQ status...")
try:
    qurl = sqs.get_queue_url(QueueName='justhodl-dlq-default')['QueueUrl']
    attrs = sqs.get_queue_attributes(
        QueueUrl=qurl, AttributeNames=['ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible']
    )['Attributes']
    report['dlq'] = {
        'url': qurl,
        'messages_available': int(attrs.get('ApproximateNumberOfMessages', 0)),
        'messages_in_flight': int(attrs.get('ApproximateNumberOfMessagesNotVisible', 0)),
    }
except Exception as e:
    report['dlq'] = {'error': str(e)[:200]}

# 3. SNS topic + subscriptions
print("[3] SNS subscriptions...")
try:
    topics = sns.list_topics()['Topics']
    arn = next((t['TopicArn'] for t in topics if t['TopicArn'].endswith(':justhodl-fleet-alerts')), None)
    if arn:
        subs = sns.list_subscriptions_by_topic(TopicArn=arn).get('Subscriptions', [])
        report['sns'] = {
            'arn': arn,
            'n_subscriptions': len(subs),
            'subscriptions': [{'endpoint': s.get('Endpoint'), 'protocol': s.get('Protocol'),
                              'confirmed': s.get('SubscriptionArn') not in ('PendingConfirmation', None) and ':' in s.get('SubscriptionArn', '')}
                             for s in subs],
        }
except Exception as e:
    report['sns'] = {'error': str(e)[:200]}

# 4. Coverage refresh
print("[4] Fleet coverage refresh...")
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    all_lambdas.extend(page['Functions'])
n_dlq = sum(1 for fn in all_lambdas if (fn.get('DeadLetterConfig') or {}).get('TargetArn'))
n_xray = sum(1 for fn in all_lambdas if fn.get('TracingConfig', {}).get('Mode') == 'Active')
report['coverage'] = {
    'n_lambdas': len(all_lambdas),
    'dlq': f'{n_dlq}/{len(all_lambdas)} ({round(n_dlq/len(all_lambdas)*100,1)}%)',
    'xray': f'{n_xray}/{len(all_lambdas)} ({round(n_xray/len(all_lambdas)*100,1)}%)',
}

# 5. Freshness manifest
print("[5] Freshness manifest present?")
try:
    h = s3.head_object(Bucket='justhodl-dashboard-live', Key='data/_freshness-manifest.json')
    report['freshness_manifest'] = {
        'present': True,
        'last_modified': h['LastModified'].isoformat(),
        'size': h['ContentLength'],
        'age_h': round((NOW - h['LastModified']).total_seconds()/3600, 1),
    }
except Exception as e:
    report['freshness_manifest'] = {'present': False, 'error': str(e)[:200]}

import os
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1051.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
