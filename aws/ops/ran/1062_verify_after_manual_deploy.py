#!/usr/bin/env python3
"""ops 1062 — verify carry-surface + engine-contribution after manual deploy"""
import json, boto3, os, time
from datetime import datetime, timezone

REGION = 'us-east-1'
lam = boto3.client('lambda', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat()}

for fn_name, out_key in [
    ('justhodl-carry-surface', 'data/carry-surface.json'),
    ('justhodl-engine-contribution', 'data/engine-contributions.json'),
]:
    info = {}
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
        # Invoke
        inv = lam.invoke(FunctionName=fn_name, InvocationType='RequestResponse', Payload=b'{}')
        payload = inv['Payload'].read().decode()
        info['invoke'] = {
            'status': inv['StatusCode'],
            'fn_error': inv.get('FunctionError', 'none'),
            'response': payload[:600],
        }
        # Wait + check S3
        time.sleep(3)
        try:
            head = s3.head_object(Bucket='justhodl-dashboard-live', Key=out_key)
            age_h = (datetime.now(timezone.utc) - head['LastModified']).total_seconds() / 3600
            info['s3_output'] = {
                'exists': True, 'size': head['ContentLength'],
                'age_h': round(age_h, 2),
                'last_modified': head['LastModified'].isoformat(),
            }
        except Exception as e:
            info['s3_output'] = {'exists': False, 'error': str(e)[:120]}
    except Exception as e:
        info['lambda'] = {'exists': False, 'error': str(e)[:200]}
    
    report[fn_name] = info

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1062.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
