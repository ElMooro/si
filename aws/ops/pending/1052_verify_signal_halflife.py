#!/usr/bin/env python3
"""ops 1052 — verify justhodl-signal-halflife deployed and works"""
import json, boto3, os
from datetime import datetime, timezone

lam = boto3.client('lambda', region_name='us-east-1')
events = boto3.client('events', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# 1. Lambda exists?
try:
    cfg = lam.get_function_configuration(FunctionName='justhodl-signal-halflife')
    report['lambda'] = {
        'exists': True,
        'last_modified': cfg.get('LastModified'),
        'runtime': cfg.get('Runtime'),
        'memory': cfg.get('MemorySize'),
        'timeout': cfg.get('Timeout'),
        'dlq': (cfg.get('DeadLetterConfig') or {}).get('TargetArn', '')[-30:],
        'xray': cfg.get('TracingConfig', {}).get('Mode'),
        'env_vars': sorted((cfg.get('Environment',{}) or {}).get('Variables', {}).keys()),
    }
except lam.exceptions.ResourceNotFoundException:
    report['lambda'] = {'exists': False}

# 2. EventBridge rule
try:
    rd = events.describe_rule(Name='signal-halflife-weekly')
    report['eb_rule'] = {
        'state': rd.get('State'),
        'expression': rd.get('ScheduleExpression'),
    }
except events.exceptions.ResourceNotFoundException:
    report['eb_rule'] = {'state': 'NOT_FOUND'}

# 3. Live invoke
if report.get('lambda', {}).get('exists'):
    try:
        r = lam.invoke(FunctionName='justhodl-signal-halflife', Payload=b'{}',
                       InvocationType='RequestResponse')
        body = r['Payload'].read().decode('utf-8', errors='replace')
        report['invoke'] = {
            'status': r['StatusCode'],
            'function_error': r.get('FunctionError', 'none'),
            'response_head': body[:800],
        }
    except Exception as e:
        report['invoke'] = {'error': str(e)[:300]}

# 4. S3 output written?
try:
    h = s3.head_object(Bucket='justhodl-dashboard-live', Key='data/signal-halflife.json')
    report['s3_output'] = {
        'exists': True,
        'size': h['ContentLength'],
        'last_modified': str(h['LastModified']),
    }
except Exception:
    report['s3_output'] = {'exists': False}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1052.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
