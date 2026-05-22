#!/usr/bin/env python3
"""ops 1059 — inspect partial implementations to determine BUILD/PATCH/SKIP"""
import json, boto3, os
from datetime import datetime, timezone

REGION = 'us-east-1'
lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# === EB rule: carry-surface-4h — what does it target? ===
try:
    r = events.describe_rule(Name='carry-surface-4h')
    targets = events.list_targets_by_rule(Rule='carry-surface-4h').get('Targets', [])
    report['carry_surface_4h_rule'] = {
        'state': r.get('State'),
        'expression': r.get('ScheduleExpression'),
        'targets': [{'arn': t['Arn'], 'id': t['Id']} for t in targets],
    }
except Exception as e:
    report['carry_surface_4h_rule'] = {'error': str(e)[:200]}

# === EB rule: engine-contribution-weekly ===
try:
    r = events.describe_rule(Name='engine-contribution-weekly')
    targets = events.list_targets_by_rule(Rule='engine-contribution-weekly').get('Targets', [])
    report['engine_contribution_rule'] = {
        'state': r.get('State'),
        'expression': r.get('ScheduleExpression'),
        'targets': [{'arn': t['Arn'], 'id': t['Id']} for t in targets],
    }
except Exception as e:
    report['engine_contribution_rule'] = {'error': str(e)[:200]}

# === Lambda: justhodl-yen-carry ===
try:
    cfg = lam.get_function_configuration(FunctionName='justhodl-yen-carry')
    report['yen_carry'] = {
        'description': cfg.get('Description'),
        'memory': cfg.get('MemorySize'),
        'timeout': cfg.get('Timeout'),
        'env_keys': sorted(list((cfg.get('Environment') or {}).get('Variables', {}).keys())),
        'last_modified': cfg.get('LastModified'),
    }
    # Read code top
    import urllib.request, io, zipfile
    full = lam.get_function(FunctionName='justhodl-yen-carry')
    code = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(full['Code']['Location'], timeout=30).read())).read('lambda_function.py').decode()
    report['yen_carry']['code_head'] = code[:1500]
except Exception as e:
    report['yen_carry'] = {'error': str(e)[:200]}

# === Lambda: justhodl-earnings-nlp ===
try:
    cfg = lam.get_function_configuration(FunctionName='justhodl-earnings-nlp')
    report['earnings_nlp'] = {
        'description': cfg.get('Description'),
        'memory': cfg.get('MemorySize'),
        'timeout': cfg.get('Timeout'),
        'env_keys': sorted(list((cfg.get('Environment') or {}).get('Variables', {}).keys())),
        'last_modified': cfg.get('LastModified'),
    }
    import urllib.request, io, zipfile
    full = lam.get_function(FunctionName='justhodl-earnings-nlp')
    code = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(full['Code']['Location'], timeout=30).read())).read('lambda_function.py').decode()
    report['earnings_nlp']['code_head'] = code[:1500]
    # check the OUT_KEY
    import re
    m = re.search(r'OUT_KEY\s*=\s*["\']([^"\']+)["\']', code)
    report['earnings_nlp']['out_key'] = m.group(1) if m else None
except Exception as e:
    report['earnings_nlp'] = {'error': str(e)[:200]}

# === Lambda: justhodl-buyback-yield-ranking ===
try:
    cfg = lam.get_function_configuration(FunctionName='justhodl-buyback-yield-ranking')
    report['buyback_yield_ranking'] = {
        'description': cfg.get('Description'),
        'env_keys': sorted(list((cfg.get('Environment') or {}).get('Variables', {}).keys())),
        'last_modified': cfg.get('LastModified'),
    }
except Exception as e:
    report['buyback_yield_ranking'] = {'error': str(e)[:200]}

# === Search S3 for any carry-related output beyond yen-carry ===
keys_with_carry = []
for page in s3.get_paginator('list_objects_v2').paginate(Bucket='justhodl-dashboard-live', Prefix='data/'):
    for obj in page.get('Contents', []):
        if 'carry' in obj['Key'].lower():
            keys_with_carry.append({
                'key': obj['Key'],
                'size': obj['Size'],
                'last_modified': obj['LastModified'].isoformat(),
            })
report['s3_carry_keys'] = keys_with_carry[:20]

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1059.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str)[:5000])
