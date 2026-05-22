#!/usr/bin/env python3
"""ops 1054b — verify ACTUAL OUT_KEY paths from each exp Lambda"""
import json, boto3, os, re
from datetime import datetime, timezone

s3 = boto3.client('s3', region_name='us-east-1')
lam = boto3.client('lambda', region_name='us-east-1')

# Targets: lambda → actual OUT_KEY from code inspection (ops 1053)
TARGETS = {
    'justhodl-premortem-engine': 'data/kill-theses.json',
    'justhodl-behavior-mirror': 'data/behavior-mirror.json',
    'justhodl-failure-library': 'data/pre-disaster-watchlist.json',
    'justhodl-causality-scanner': 'data/causality-discoveries.json',
    'justhodl-convexity-scorer': 'data/convexity-scores.json',
    'justhodl-chart-vision': 'data/chart-vision.json',
    'justhodl-signal-halflife': 'data/signal-halflife.json',
}

report = {'started_at': datetime.now(timezone.utc).isoformat(), 'lambdas': {}}

for fn_name, key in TARGETS.items():
    try:
        head = s3.head_object(Bucket='justhodl-dashboard-live', Key=key)
        age_h = (datetime.now(timezone.utc) - head['LastModified']).total_seconds() / 3600
        report['lambdas'][fn_name] = {
            'key': key, 'exists': True,
            'size': head['ContentLength'],
            'last_modified': head['LastModified'].isoformat(),
            'age_h': round(age_h, 2),
        }
    except Exception as e:
        report['lambdas'][fn_name] = {'key': key, 'exists': False, 'error': str(e)[:80]}

# Special case: meta-improver — find STATE_KEY value
fn = 'justhodl-meta-improver'
try:
    cfg = lam.get_function(FunctionName=fn)
    loc = cfg['Code']['Location']
    import urllib.request, io, zipfile
    z = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(loc, timeout=30).read()))
    code = z.read('lambda_function.py').decode()
    # Find STATE_KEY assignment
    m = re.search(r'STATE_KEY\s*=\s*["\']([^"\']+)["\']', code)
    state_key = m.group(1) if m else None
    report['meta_improver_state_key'] = state_key
    if state_key:
        try:
            head = s3.head_object(Bucket='justhodl-dashboard-live', Key=state_key)
            age_h = (datetime.now(timezone.utc) - head['LastModified']).total_seconds() / 3600
            report['lambdas'][fn] = {
                'key': state_key, 'exists': True,
                'size': head['ContentLength'],
                'last_modified': head['LastModified'].isoformat(),
                'age_h': round(age_h, 2),
            }
        except Exception as e:
            report['lambdas'][fn] = {'key': state_key, 'exists': False, 'error': str(e)[:80]}
    
    # Also: causality-scanner has 3 early returns. Check if any of them write a 'no_action' state
    cs_cfg = lam.get_function(FunctionName='justhodl-causality-scanner')
    cs_code = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(cs_cfg['Code']['Location'], timeout=30).read())).read('lambda_function.py').decode()
    # Find the early-return pattern (lines ~360-380 based on grep showing "insufficient_history")
    insufficient_match = re.search(r'(.{200})insufficient_history(.{200})', cs_code, re.DOTALL)
    report['causality_insufficient_handling'] = insufficient_match.group(0)[:600] if insufficient_match else 'not_found'
except Exception as e:
    report['probe_error'] = str(e)[:200]

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1054b.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(json.dumps(report, indent=2, default=str))
