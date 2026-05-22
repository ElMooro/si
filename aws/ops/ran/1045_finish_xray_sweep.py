#!/usr/bin/env python3
"""ops 1045 — finish X-Ray sweep (8 exp Lambdas missed in earlier passes)"""
import json, boto3, os
from datetime import datetime, timezone
from botocore.config import Config

REGION = 'us-east-1'
cfg = Config(region_name=REGION, retries={'max_attempts': 10, 'mode': 'adaptive'})
lam = boto3.client('lambda', config=cfg)

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# List all Lambdas
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    all_lambdas.extend(page['Functions'])

# Find ones missing X-Ray
missing_xray = [fn for fn in all_lambdas if fn.get('TracingConfig', {}).get('Mode') != 'Active']
print(f"Found {len(missing_xray)} Lambdas missing X-Ray")
report['missing_count_before'] = len(missing_xray)
report['missing_lambdas'] = [fn['FunctionName'] for fn in missing_xray]

# Sweep
results = {'UPDATED': [], 'ERROR': []}
for fn in missing_xray:
    name = fn['FunctionName']
    try:
        lam.update_function_configuration(
            FunctionName=name,
            TracingConfig={'Mode': 'Active'},
        )
        results['UPDATED'].append(name)
        print(f"  ✅ {name}")
    except Exception as e:
        results['ERROR'].append({'lambda': name, 'msg': str(e)[:200]})
        print(f"  ❌ {name}: {str(e)[:150]}")

report['updated'] = len(results['UPDATED'])
report['errors'] = results['ERROR']

# Final verify
verify = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    verify.extend(page['Functions'])
n_dlq = sum(1 for fn in verify if (fn.get('DeadLetterConfig') or {}).get('TargetArn'))
n_xray = sum(1 for fn in verify if fn.get('TracingConfig', {}).get('Mode') == 'Active')
report['final'] = {
    'n_lambdas': len(verify),
    'n_with_dlq': n_dlq,
    'pct_dlq': round(n_dlq/len(verify)*100, 1),
    'n_with_xray': n_xray,
    'pct_xray': round(n_xray/len(verify)*100, 1),
}

report['completed_at'] = datetime.now(timezone.utc).isoformat()
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1045.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n=== FINAL ===")
print(f"  DLQ:   {n_dlq}/{len(verify)} ({report['final']['pct_dlq']}%)")
print(f"  X-Ray: {n_xray}/{len(verify)} ({report['final']['pct_xray']}%)")
