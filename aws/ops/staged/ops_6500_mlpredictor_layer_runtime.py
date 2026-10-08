"""Finish ops 6499 for MLPredictor: swap AWSSDKPandas-Python310 -> AWSSDKPandas-Python312 and move to python3.12.

The runner's IAM user cannot call lambda:ListLayerVersions on the public 336392948345 account, so the
layer version is pinned from the published table (AWS SDK for pandas 3.17.1 docs, us-east-1, x86_64):
  arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:31
Configuration-only change; code is not redeployed and nothing is invoked.
"""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import time

import boto3

ROOT = Path(__file__).resolve().parents[3]
REPORT = ROOT / 'aws/ops/reports/latest/ops_6500_mlpredictor_layer_runtime.md'
FN = 'MLPredictor'
LAYER = 'arn:aws:lambda:us-east-1:336392948345:layer:AWSSDKPandas-Python312:31'
lam = boto3.client('lambda')
NOW = datetime.now(timezone.utc)
out = {'ran_at': NOW.isoformat()}
ok = True
try:
    c = lam.get_function_configuration(FunctionName=FN)
    out['before'] = {'runtime': c['Runtime'], 'layers': [l['Arn'] for l in c.get('Layers', [])]}
    if c['Runtime'] == 'python3.12':
        out['note'] = 'already python3.12'
    else:
        lam.update_function_configuration(FunctionName=FN, Runtime='python3.12', Layers=[LAYER])
        for _ in range(60):
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get('LastUpdateStatus') in (None, 'Successful'):
                break
            if c.get('LastUpdateStatus') == 'Failed':
                raise RuntimeError(c.get('LastUpdateStatusReason'))
            time.sleep(3)
    out['after'] = {'runtime': c['Runtime'], 'layers': [l['Arn'] for l in c.get('Layers', [])],
                    'last_modified': c['LastModified'], 'status': c.get('LastUpdateStatus')}
    ok = c['Runtime'] == 'python3.12'
except Exception as e:
    ok = False
    out['error'] = type(e).__name__ + ': ' + str(e)[:300]
remaining = []
for page in lam.get_paginator('list_functions').paginate():
    remaining += [(f['FunctionName'], f['Runtime']) for f in page['Functions']
                  if f.get('Runtime') in ('python3.9', 'python3.10', 'nodejs18.x')]
out['remaining_on_retired_runtimes'] = remaining
lines = ['# ops 6500 - MLPredictor layer + runtime', '', '```json', json.dumps(out, indent=1, default=str), '```', '',
         'VERDICT: %s' % ('PASS' if ok and not remaining else 'FAIL')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines))
if not (ok and not remaining):
    sys.exit(1)
