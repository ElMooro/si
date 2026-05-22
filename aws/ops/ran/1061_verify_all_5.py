#!/usr/bin/env python3
"""ops 1061 — verify all 5 next-wave exp engines deployed + first invokes"""
import json, boto3, os, time
from datetime import datetime, timezone

REGION = 'us-east-1'
lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

ENGINES = [
    ('justhodl-carry-surface', 'carry-surface-4h', 'data/carry-surface.json', '#1 Carry Surface'),
    ('justhodl-engine-contribution', 'engine-contribution-weekly', 'data/engine-contributions.json', '#2 Engine Contribution'),
    ('justhodl-cross-asset-confirm', 'cross-asset-confirm-3h', 'data/cross-asset-confirm.json', '#3 Cross-Asset Confirm'),
    ('justhodl-earnings-nlp', None, None, '#4 Earnings Linguistic (pre-existing)'),
    ('justhodl-engine-robustness', 'engine-robustness-weekly', 'data/engine-robustness.json', '#5 Engine Robustness'),
]

report = {'started_at': datetime.now(timezone.utc).isoformat()}

for fn_name, rule_name, out_key, label in ENGINES:
    info = {'label': label}
    
    # Lambda exists?
    try:
        cfg = lam.get_function_configuration(FunctionName=fn_name)
        info['lambda'] = {
            'exists': True,
            'last_modified': cfg.get('LastModified'),
            'memory': cfg.get('MemorySize'),
            'timeout': cfg.get('Timeout'),
            'dlq': bool((cfg.get('DeadLetterConfig') or {}).get('TargetArn')),
            'xray': cfg.get('TracingConfig', {}).get('Mode') == 'Active',
        }
    except Exception as e:
        info['lambda'] = {'exists': False, 'error': str(e)[:200]}
    
    # EB rule
    if rule_name:
        try:
            r = events.describe_rule(Name=rule_name)
            info['eb_rule'] = {'state': r.get('State'), 'cron': r.get('ScheduleExpression')}
        except Exception as e:
            info['eb_rule'] = {'error': str(e)[:120]}
    
    # Live invoke (if Lambda exists)
    if info['lambda'].get('exists'):
        try:
            inv = lam.invoke(FunctionName=fn_name, InvocationType='RequestResponse', Payload=b'{}')
            payload = inv['Payload'].read().decode()
            info['invoke'] = {
                'status': inv['StatusCode'],
                'fn_error': inv.get('FunctionError', 'none'),
                'response': payload[:500],
            }
        except Exception as e:
            info['invoke'] = {'error': str(e)[:200]}
    
    # S3 output
    if out_key and info['lambda'].get('exists'):
        try:
            head = s3.head_object(Bucket='justhodl-dashboard-live', Key=out_key)
            age_h = (datetime.now(timezone.utc) - head['LastModified']).total_seconds() / 3600
            info['s3_output'] = {
                'exists': True,
                'size': head['ContentLength'],
                'age_h': round(age_h, 2),
                'last_modified': head['LastModified'].isoformat(),
            }
        except Exception as e:
            info['s3_output'] = {'exists': False, 'error': str(e)[:120]}
    
    report[fn_name] = info

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1061.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(json.dumps(report, indent=2, default=str))
