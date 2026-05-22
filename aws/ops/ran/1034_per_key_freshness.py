#!/usr/bin/env python3
"""
ops 1034 — final verification: per-output-key freshness check

For each suspect Lambda, I now know the EXACT keys their scheduled
flow writes (vs admin-only writes). Verify the per-key freshness.
"""
import json, boto3
from datetime import datetime, timezone, timedelta

REGION = 'us-east-1'
NOW = datetime.now(timezone.utc)
BUCKET = 'justhodl-dashboard-live'

s3 = boto3.client('s3', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)

# Lambda -> { key: 'on scheduled run' | 'admin-only' }
SUSPECT_KEYS = {
    'justhodl-khalid-metrics': {
        'data/khalid-config.json': 'admin-only',
        'data/khalid-metrics.json': 'scheduled',
        'data/khalid-analysis.json': 'scheduled (needs AI success)',
    },
    'justhodl-ka-metrics': {
        'data/khalid-config.json': 'admin-only (dual-write)',
        'data/ka-config.json': 'admin-only (dual-write)',
        'data/khalid-metrics.json': 'scheduled (potential overwrite)',
        'data/khalid-analysis.json': 'scheduled (potential overwrite)',
    },
    'justhodl-bloomberg-v8': {
        'data/report.json': 'scheduled',
    },
    'justhodl-daily-report-v3': {
        'data/report.json': 'scheduled (race winner)',
    },
}

report = {'started_at': NOW.isoformat(), 'per_key': {}, 'per_lambda': {}}

# Get last-modified for each key
for lam_name, keys in SUSPECT_KEYS.items():
    report['per_lambda'][lam_name] = {}
    for key, kind in keys.items():
        try:
            h = s3.head_object(Bucket=BUCKET, Key=key)
            entry = {
                'kind': kind,
                'last_modified': h['LastModified'].isoformat(),
                'age_h': round((NOW - h['LastModified']).total_seconds()/3600, 2),
                'size': h['ContentLength'],
            }
        except s3.exceptions.NoSuchKey:
            entry = {'kind': kind, 'error': 'NOT_FOUND'}
        except Exception as e:
            entry = {'kind': kind, 'error': str(e)[:200]}
        report['per_key'][key] = entry
        report['per_lambda'][lam_name][key] = entry

# Also: get last invocation timestamp for these Lambdas via CW
end = NOW; start = end - timedelta(days=7)
for lam_name in SUSPECT_KEYS:
    try:
        m = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Invocations',
            Dimensions=[{'Name': 'FunctionName', 'Value': lam_name}],
            StartTime=start, EndTime=end, Period=3600, Statistics=['Sum'],
        )
        dps = sorted(m['Datapoints'], key=lambda p: p['Timestamp'], reverse=True)
        last_inv = None
        for p in dps:
            if p['Sum'] > 0:
                last_inv = {
                    'hour': p['Timestamp'].isoformat(),
                    'invocations_that_hour': int(p['Sum']),
                    'hours_ago': round((NOW - p['Timestamp']).total_seconds()/3600, 2),
                }
                break
        report['per_lambda'][lam_name]['last_invocation'] = last_inv
        # Total invocations 7d
        report['per_lambda'][lam_name]['invocations_7d'] = int(sum(p['Sum'] for p in m['Datapoints']))
        # Total errors 7d
        m_err = cw.get_metric_statistics(
            Namespace='AWS/Lambda', MetricName='Errors',
            Dimensions=[{'Name': 'FunctionName', 'Value': lam_name}],
            StartTime=start, EndTime=end, Period=86400, Statistics=['Sum'],
        )
        report['per_lambda'][lam_name]['errors_7d'] = int(sum(p['Sum'] for p in m_err['Datapoints']))
    except Exception as e:
        report['per_lambda'][lam_name]['cw_error'] = str(e)[:200]

# Write
import os
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1034.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

# Diagnostic per Lambda
print("="*70)
print("PER-LAMBDA SCHEDULED-OUTPUT FRESHNESS DIAGNOSIS")
print("="*70)
for lam_name, info in report['per_lambda'].items():
    print(f"\n  {lam_name}")
    print(f"    invocations_7d: {info.get('invocations_7d')}")
    print(f"    errors_7d: {info.get('errors_7d')}")
    print(f"    last_invocation: {info.get('last_invocation')}")
    print(f"    OUTPUTS:")
    for key, val in info.items():
        if not isinstance(val, dict) or 'kind' not in val:
            continue
        kind = val['kind']
        if 'error' in val:
            print(f"      {key} ({kind}): {val['error']}")
        else:
            age = val['age_h']
            is_admin = 'admin' in kind
            verdict = '✅ recent' if age < 26 else ('ℹ️ stale ok (admin)' if is_admin else f'🚨 STALE')
            print(f"      {key:50} ({kind})")
            print(f"          age={age}h  last_mod={val['last_modified']}  {verdict}")

print(f"\nReport: aws/ops/reports/1034.json")
