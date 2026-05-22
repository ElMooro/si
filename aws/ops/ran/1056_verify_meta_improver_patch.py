#!/usr/bin/env python3
"""ops 1056 — re-verify meta-improver patch deployed + heartbeat lands"""
import json, boto3, os, time
from datetime import datetime, timezone

REGION = 'us-east-1'
lam = boto3.client('lambda', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# Check meta-improver code SHA
cfg = lam.get_function_configuration(FunctionName='justhodl-meta-improver')
report['meta_improver_config'] = {
    'last_modified': cfg.get('LastModified'),
    'code_sha': cfg.get('CodeSha256', '')[:16],
    'code_size': cfg.get('CodeSize'),
}

# Download deployed code, check if patch is in it
try:
    cfg_full = lam.get_function(FunctionName='justhodl-meta-improver')
    loc = cfg_full['Code']['Location']
    import urllib.request, io, zipfile
    z = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=30).read()))
    code = z.read('lambda_function.py').decode()
    has_patch = '"last_run"' in code and '"reason": "no_decaying_engines_outside_cooldown"' in code
    report['deployed_has_patch'] = has_patch
    # Show snippet around the change
    idx = code.find('no_decaying_engines_outside_cooldown')
    report['snippet'] = code[max(0,idx-200):idx+200] if idx > 0 else 'pattern_not_found'
except Exception as e:
    report['inspect_error'] = str(e)[:300]

# Live invoke
inv = lam.invoke(FunctionName='justhodl-meta-improver',
                 InvocationType='RequestResponse', Payload=b'{}')
payload = inv['Payload'].read().decode()
report['invoke'] = {
    'status': inv['StatusCode'],
    'function_error': inv.get('FunctionError', 'none'),
    'response': payload[:500],
}

# Wait a moment + check S3 state
time.sleep(3)
try:
    head = s3.head_object(Bucket='justhodl-dashboard-live', Key='data/meta-improver-state.json')
    age_h = (datetime.now(timezone.utc) - head['LastModified']).total_seconds() / 3600
    report['state_key'] = {
        'exists': True,
        'size': head['ContentLength'],
        'last_modified': head['LastModified'].isoformat(),
        'age_h': round(age_h, 2),
    }
    # Read content
    obj = s3.get_object(Bucket='justhodl-dashboard-live', Key='data/meta-improver-state.json')
    content = json.loads(obj['Body'].read().decode())
    report['state_content_keys'] = list(content.keys()) if isinstance(content, dict) else None
    report['state_last_run'] = content.get('last_run') if isinstance(content, dict) else None
except Exception as e:
    report['state_key'] = {'exists': False, 'error': str(e)[:200]}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1056.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(json.dumps(report, indent=2, default=str)[:2500])
