#!/usr/bin/env python3
"""ops 1059 — inspect xccy-basis-agent + cb-injection + yen-carry data to design carry-surface"""
import json, boto3, os, urllib.request, io, zipfile
from datetime import datetime, timezone

REGION = 'us-east-1'
lam = boto3.client('lambda', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# Inspect each carry-adjacent Lambda's deployed code (top of file = imports + constants)
TARGETS = ['xccy-basis-agent', 'justhodl-cb-injection']
for fn_name in TARGETS:
    info = {}
    try:
        cfg = lam.get_function_configuration(FunctionName=fn_name)
        info['memory'] = cfg.get('MemorySize')
        info['timeout'] = cfg.get('Timeout')
        info['description'] = cfg.get('Description', '')
        info['env_keys'] = sorted(list((cfg.get('Environment') or {}).get('Variables', {}).keys()))
        info['last_modified'] = cfg.get('LastModified')
        
        # Pull deployed code
        cfg_full = lam.get_function(FunctionName=fn_name)
        loc = cfg_full['Code']['Location']
        z = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=30).read()))
        code = z.read('lambda_function.py').decode()
        info['code_size'] = len(code)
        # First 80 lines for context
        info['code_head'] = '\n'.join(code.split('\n')[:80])
        # Find OUT_KEY / S3_KEY / output keys
        import re
        outs = re.findall(r'["\']data/[^"\']+\.json["\']', code)
        info['s3_keys_referenced'] = list(set(outs))[:8]
    except Exception as e:
        info['error'] = str(e)[:300]
    report[fn_name] = info

# Inspect yen-carry.json content
try:
    obj = s3.get_object(Bucket='justhodl-dashboard-live', Key='data/yen-carry.json')
    raw = obj['Body'].read().decode()
    d = json.loads(raw)
    report['yen_carry_json'] = {
        'size': len(raw),
        'top_level_keys': list(d.keys()) if isinstance(d, dict) else 'not_dict',
        'last_modified': obj['LastModified'].isoformat(),
        'sample': {k: (str(v)[:200] if not isinstance(v, (dict, list)) else f'<{type(v).__name__} with {len(v)} items>') for k, v in (d.items() if isinstance(d, dict) else [])},
    }
except Exception as e:
    report['yen_carry_json'] = {'err': str(e)[:200]}

# Check for any other carry/yield/funding-related S3 keys
paginator = s3.get_paginator('list_objects_v2')
related_keys = []
for page in paginator.paginate(Bucket='justhodl-dashboard-live', Prefix='data/'):
    for obj in page.get('Contents', []):
        k = obj['Key'].lower()
        if any(x in k for x in ['carry', 'yield-', 'funding-rate', 'roll-', 'basis-', 'contango']):
            related_keys.append({'key': obj['Key'], 'size': obj['Size'], 'lm': obj['LastModified'].isoformat()})
report['related_s3_keys'] = related_keys

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1059.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(json.dumps(report, indent=2, default=str)[:5000])
