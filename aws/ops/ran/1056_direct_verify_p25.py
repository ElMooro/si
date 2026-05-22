#!/usr/bin/env python3
"""ops 1054 — direct verify P2.5 EMF patches reached AWS by downloading deployed code"""
import json, boto3, os, urllib.request, zipfile, io
from datetime import datetime, timezone

REGION = 'us-east-1'
lam = boto3.client('lambda', region_name=REGION)

PATCHED = [
    'justhodl-portfolio-catalysts', 'justhodl-vrp', 'justhodl-options-flow',
    'justhodl-global-stress', 'justhodl-financial-secretary',
    'justhodl-daily-report-v3', 'justhodl-feedback', 'justhodl-dollar-radar',
    'justhodl-hedge-pnl', 'justhodl-fleet-error-monitor',
]

report = {'started_at': datetime.now(timezone.utc).isoformat(), 'results': {}}

for name in PATCHED:
    try:
        cfg = lam.get_function_configuration(FunctionName=name)
        url = lam.get_function(FunctionName=name)['Code']['Location']
        with urllib.request.urlopen(url, timeout=30) as r:
            zf = zipfile.ZipFile(io.BytesIO(r.read()))
            code = zf.read('lambda_function.py').decode('utf-8', errors='replace')
        
        has_marker = code.count('audit P2.5: emit EMF metric for silent put_object failure')
        has_emf_namespace = 'JustHodl/Reliability' in code
        has_s3_put_failure = 'S3PutFailure' in code
        
        # fleet-error-monitor specific
        is_monitor = name == 'justhodl-fleet-error-monitor'
        if is_monitor:
            has_silent_alerts = 'silent_alerts' in code
            has_helper_fn = 'def get_silent_failure_count' in code
        
        report['results'][name] = {
            'last_modified': cfg.get('LastModified'),
            'code_sha': cfg.get('CodeSha256', '')[:12],
            'marker_count': has_marker,
            'has_emf_namespace': has_emf_namespace,
            'has_s3_put_failure_metric': has_s3_put_failure,
            **({'has_silent_alerts_pass': has_silent_alerts,
                'has_helper_function': has_helper_fn} if is_monitor else {}),
        }
    except Exception as e:
        report['results'][name] = {'error': str(e)[:200]}

# Summary
ok_count = sum(1 for r in report['results'].values() 
               if not r.get('error') and r.get('marker_count', 0) > 0 and r.get('has_emf_namespace'))
fleet_ok = report['results'].get('justhodl-fleet-error-monitor', {}).get('has_silent_alerts_pass', False)

report['summary'] = {
    'lambdas_checked': len(PATCHED),
    'patched_correctly': ok_count,
    'fleet_monitor_extended': fleet_ok,
}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1054.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(json.dumps(report['summary'], indent=2))
print()
for name, r in report['results'].items():
    if 'error' in r:
        print(f"  ❌ {name}: {r['error'][:80]}")
    else:
        n = r.get('marker_count', 0)
        emf = '✓' if r.get('has_emf_namespace') else '✗'
        mark = '✅' if n > 0 and r.get('has_emf_namespace') else '❌'
        extra = ''
        if name == 'justhodl-fleet-error-monitor':
            extra = f"  silent_alerts={r.get('has_silent_alerts_pass')}  helper={r.get('has_helper_function')}"
        print(f"  {mark} {name}: markers={n} emf={emf}{extra}")
