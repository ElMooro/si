#!/usr/bin/env python3
"""
ops 1068 — DATA PROTECTION FOUNDATION
======================================
Phase 1A: Enable DDB PITR on 11 disabled tables (cap risk on every table)
Phase 1B: Inspect 2 partial Lambdas to confirm build needed
Phase 1C: Audit S3 versioning + lifecycle to understand current backup posture

S3 CRR (Cross-Region Replication) — deferred to ops 1069 because it requires
creating a destination bucket in us-west-2 + an IAM replication role.
"""
import json, boto3, os
from datetime import datetime, timezone

ddb = boto3.client('dynamodb', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')
lam = boto3.client('lambda', region_name='us-east-1')

# === Phase 1A: Enable PITR on tables that need it ===
TABLES_TO_PROTECT = [
    'justhodl-portfolio',       # CAPITAL records
    'justhodl-trades',          # TRADE history
    'justhodl-alert-actions',   # behavior-mirror data
    'justhodl-backtest',
    'justhodl-feedback',
    'justhodl-history',
    'justhodl-push-subscriptions',
    'justhodl-api-rate',
    'justhodl-signal-registry',
    'WebSocketConnections',
]

pitr_results = {}
for t in TABLES_TO_PROTECT:
    try:
        # Check if already enabled
        r = ddb.describe_continuous_backups(TableName=t)
        current = r['ContinuousBackupsDescription']['PointInTimeRecoveryDescription']['PointInTimeRecoveryStatus']
        if current == 'ENABLED':
            pitr_results[t] = 'ALREADY_ENABLED'
            continue
        # Enable it
        ddb.update_continuous_backups(
            TableName=t,
            PointInTimeRecoverySpecification={'PointInTimeRecoveryEnabled': True},
        )
        pitr_results[t] = 'ENABLED_NOW'
    except ddb.exceptions.TableNotFoundException:
        pitr_results[t] = 'TABLE_MISSING'
    except Exception as e:
        pitr_results[t] = f'ERR: {str(e)[:120]}'

# === Phase 1B: Inspect partial Lambdas ===
import io, zipfile, urllib.request
partials = {}

# data/stress-scenarios.json — is there a Lambda producing it?
try:
    head = s3.head_object(Bucket='justhodl-dashboard-live', Key='data/stress-scenarios.json')
    obj = s3.get_object(Bucket='justhodl-dashboard-live', Key='data/stress-scenarios.json')
    content = json.loads(obj['Body'].read())
    age_h = (datetime.now(timezone.utc) - head['LastModified']).total_seconds() / 3600
    partials['stress-scenarios.json'] = {
        'exists': True,
        'size': head['ContentLength'],
        'age_h': round(age_h, 1),
        'top_keys': list(content.keys()) if isinstance(content, dict) else 'not_dict',
        'sample': json.dumps(content, default=str, indent=2)[:1500] if isinstance(content, dict) else None,
    }
except Exception as e:
    partials['stress-scenarios.json'] = {'error': str(e)[:200]}

# justhodl-spinoff-desk — what does it do?
try:
    cfg = lam.get_function_configuration(FunctionName='justhodl-spinoff-desk')
    partials['spinoff-desk'] = {
        'description': cfg.get('Description'),
        'env_keys': sorted(list((cfg.get('Environment') or {}).get('Variables', {}).keys())),
        'memory': cfg.get('MemorySize'),
        'last_modified': cfg.get('LastModified'),
    }
    # Code head
    full = lam.get_function(FunctionName='justhodl-spinoff-desk')
    code = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(full['Code']['Location'], timeout=30).read())).read('lambda_function.py').decode()
    partials['spinoff-desk']['code_head'] = code[:1200]
    partials['spinoff-desk']['code_lines'] = len(code.split('\n'))
    import re
    m = re.search(r'OUT_KEY\s*=\s*["\']([^"\']+)["\']', code)
    partials['spinoff-desk']['out_key'] = m.group(1) if m else None
except Exception as e:
    partials['spinoff-desk'] = {'error': str(e)[:200]}

# === Phase 1C: S3 backup posture ===
s3_posture = {}
try:
    v = s3.get_bucket_versioning(Bucket='justhodl-dashboard-live')
    s3_posture['versioning'] = v.get('Status')
    s3_posture['mfa_delete'] = v.get('MFADelete', 'Disabled')
except Exception as e:
    s3_posture['versioning_err'] = str(e)[:100]

try:
    lc = s3.get_bucket_lifecycle_configuration(Bucket='justhodl-dashboard-live')
    s3_posture['lifecycle_rules'] = [{'id': r.get('ID'), 'status': r.get('Status'),
                                       'prefix': (r.get('Filter') or {}).get('Prefix') or 'all'}
                                      for r in lc.get('Rules', [])]
except s3.exceptions.ClientError as e:
    s3_posture['lifecycle'] = 'NOT_CONFIGURED'

# Verify final PITR state
try:
    pitr_after = {}
    for table in TABLES_TO_PROTECT:
        try:
            r = ddb.describe_continuous_backups(TableName=table)
            pitr_after[table] = r['ContinuousBackupsDescription']['PointInTimeRecoveryDescription']['PointInTimeRecoveryStatus']
        except Exception:
            pitr_after[table] = 'UNKNOWN'
    s3_posture['ddb_pitr_after'] = pitr_after
except Exception as e:
    s3_posture['ddb_pitr_after_err'] = str(e)[:100]

report = {
    'started_at': datetime.now(timezone.utc).isoformat(),
    'phase1a_pitr_actions': pitr_results,
    'phase1b_partials': partials,
    'phase1c_s3_posture': s3_posture,
}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1068.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str)[:5000])
