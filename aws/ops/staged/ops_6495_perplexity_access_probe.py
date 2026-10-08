"""Read-only access probe for the Perplexity Computer lane (2026-10-08).

Proves the runner's AWS identity and read access. No writes to AWS, no Lambda
invocation, no schedule changes. Writes only its own markdown report locally.
"""
from datetime import datetime, timezone
from pathlib import Path
import sys

import boto3

ROOT = Path(__file__).resolve().parents[3]
REPORT = ROOT / 'aws/ops/reports/latest/ops_6495_perplexity_access_probe.md'
BUCKET = 'justhodl-dashboard-live'
KEYS = ['data/khalid-risk.json', 'data/engine-fusion.json', 'data/market-tape.json',
        'config/engine-contracts.json', 'data/contract-violations.json']

lines = ['# ops 6495 - Perplexity access probe (read-only)', '',
         '- ran_at: %s' % datetime.now(timezone.utc).isoformat()]
ok = True
try:
    ident = boto3.client('sts').get_caller_identity()
    lines.append('- aws_account: %s' % ident['Account'])
    lines.append('- aws_arn: %s' % ident['Arn'])
except Exception as e:
    ok = False
    lines.append('- sts: FAIL %s' % e)

try:
    lam = boto3.client('lambda')
    fns = []
    for page in lam.get_paginator('list_functions').paginate():
        fns += page['Functions']
    lines.append('- lambda_function_count: %d' % len(fns))
    runtimes = {}
    for f in fns:
        runtimes[f.get('Runtime', 'image')] = runtimes.get(f.get('Runtime', 'image'), 0) + 1
    lines.append('- lambda_runtimes: %s' % dict(sorted(runtimes.items())))
except Exception as e:
    ok = False
    lines.append('- lambda: FAIL %s' % e)

lines += ['', '## S3 feed freshness (%s)' % BUCKET, '', '| key | bytes | last_modified | age_min |', '|---|---|---|---|']
try:
    s3 = boto3.client('s3')
    now = datetime.now(timezone.utc)
    for k in KEYS:
        try:
            h = s3.head_object(Bucket=BUCKET, Key=k)
            age = (now - h['LastModified']).total_seconds() / 60
            lines.append('| %s | %d | %s | %.0f |' % (k, h['ContentLength'], h['LastModified'].isoformat(), age))
        except Exception as e:
            lines.append('| %s | - | missing: %s | - |' % (k, str(e)[:80]))
except Exception as e:
    ok = False
    lines.append('s3: FAIL %s' % e)

lines += ['', 'VERDICT: %s' % ('PASS' if ok else 'FAIL')]
REPORT.parent.mkdir(parents=True, exist_ok=True)
REPORT.write_text('\n'.join(lines) + '\n', encoding='utf-8')
print('\n'.join(lines))
if not ok:
    sys.exit(1)
