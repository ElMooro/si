#!/usr/bin/env python3
"""ops 1070 — DEEP RECON of existing stress-* + status of prior pushes."""
import json, os, io, zipfile, urllib.request, re, boto3
from datetime import datetime, timezone

lam = boto3.client('lambda', region_name='us-east-1')
events = boto3.client('events', region_name='us-east-1')
s3 = boto3.client('s3', region_name='us-east-1')

EXISTING_STRESS = [
    'justhodl-bank-stress', 'justhodl-credit-stress', 'justhodl-eurodollar-stress',
    'justhodl-firm-stress', 'justhodl-global-stress', 'justhodl-sovereign-stress',
    'justhodl-stress-loadings', 'justhodl-stress-scenarios', 'justhodl-stress-simulator',
    'justhodl-systemic-stress',
]

# New ones we expect to exist after prior pushes
NEW_EXPECTED = [
    'justhodl-dr-snapshot', 'justhodl-cost-anomaly',
]

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# === Inspect all 10 stress-* Lambdas ===
stress_inspect = {}
for fn in EXISTING_STRESS:
    try:
        cfg = lam.get_function_configuration(FunctionName=fn)
        full = lam.get_function(FunctionName=fn)
        code = zipfile.ZipFile(io.BytesIO(
            urllib.request.urlopen(full['Code']['Location'], timeout=20).read()
        )).read('lambda_function.py').decode('utf-8', errors='replace')
        # Find OUT_KEY or similar S3 output marker
        out_key = None
        for pattern in [r'OUT_KEY\s*=\s*["\']([^"\']+)["\']',
                        r'OUTPUT_KEY\s*=\s*["\']([^"\']+)["\']',
                        r'Key\s*=\s*["\']data/([^"\']+)["\']']:
            m = re.search(pattern, code)
            if m:
                out_key = m.group(1) if 'data/' in pattern else f"data/{m.group(1)}" if not m.group(1).startswith('data/') else m.group(1)
                break
        # Schedule
        try:
            rules = []
            for page in events.get_paginator('list_rule_names_by_target').paginate(
                TargetArn=cfg.get('FunctionArn','')):
                rules.extend(page.get('RuleNames', []))
        except Exception:
            rules = []
        stress_inspect[fn] = {
            'description': (cfg.get('Description') or '')[:300],
            'memory': cfg.get('MemorySize'),
            'last_modified': cfg.get('LastModified'),
            'env_keys': sorted(list((cfg.get('Environment') or {}).get('Variables', {}).keys())),
            'code_lines': len(code.split('\n')),
            'out_key': out_key,
            'first_500_chars': code[:500],
        }
    except Exception as e:
        stress_inspect[fn] = {'error': str(e)[:200]}

# === Check our newly pushed Lambdas ===
new_status = {}
for fn in NEW_EXPECTED:
    try:
        cfg = lam.get_function_configuration(FunctionName=fn)
        new_status[fn] = {
            'exists': True,
            'last_modified': cfg.get('LastModified'),
            'memory': cfg.get('MemorySize'),
            'state': cfg.get('State'),
            'last_update_status': cfg.get('LastUpdateStatus'),
        }
    except lam.exceptions.ResourceNotFoundException:
        new_status[fn] = {'exists': False, 'note': 'NOT DEPLOYED - deploy-lambdas.yml may not have run for new dir'}
    except Exception as e:
        new_status[fn] = {'error': str(e)[:200]}

# === Check ops 1069 outcome (S3 CRR) ===
crr_status = {}
try:
    r = s3.get_bucket_replication(Bucket='justhodl-dashboard-live')
    crr_status['source_bucket'] = {
        'role': r['ReplicationConfiguration']['Role'],
        'n_rules': len(r['ReplicationConfiguration']['Rules']),
        'first_rule_status': r['ReplicationConfiguration']['Rules'][0]['Status'],
    }
except s3.exceptions.ClientError as e:
    crr_status['source_bucket'] = {'configured': False, 'error': str(e)[:200]}

# Check DR bucket exists
try:
    s3_west = boto3.client('s3', region_name='us-west-2')
    s3_west.head_bucket(Bucket='justhodl-dashboard-live-dr')
    crr_status['dr_bucket_exists'] = True
    # Check versioning
    v = s3_west.get_bucket_versioning(Bucket='justhodl-dashboard-live-dr')
    crr_status['dr_bucket_versioning'] = v.get('Status', 'NOT_ENABLED')
except Exception as e:
    crr_status['dr_bucket_exists'] = False
    crr_status['dr_bucket_err'] = str(e)[:200]

# === Check existing S3 outputs from stress-* Lambdas ===
stress_outputs = {}
for prefix in ['data/stress-', 'data/firm-stress', 'data/global-stress',
               'data/bank-stress', 'data/credit-stress', 'data/sovereign-stress',
               'data/systemic-', 'data/eurodollar', 'data/portfolio-stress']:
    try:
        r = s3.list_objects_v2(Bucket='justhodl-dashboard-live', Prefix=prefix, MaxKeys=20)
        for obj in r.get('Contents', []):
            stress_outputs[obj['Key']] = {
                'size': obj['Size'],
                'last_modified': obj['LastModified'].isoformat(),
                'age_h': round((datetime.now(timezone.utc) - obj['LastModified']).total_seconds() / 3600, 1),
            }
    except Exception:
        pass

report['stress_lambda_inspect'] = stress_inspect
report['newly_pushed_status'] = new_status
report['s3_crr_status'] = crr_status
report['existing_stress_outputs'] = stress_outputs

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1070.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

# Print compact summary
print('=== STRESS-* LAMBDA INSPECTION ===')
for fn, info in stress_inspect.items():
    if 'error' in info:
        print(f"  {fn}: ERR {info['error'][:80]}")
    else:
        print(f"  {fn:35s} out={info.get('out_key','?'):40s} lines={info.get('code_lines','?')}")
        print(f"      desc: {(info.get('description') or '')[:140]}")

print()
print('=== NEW LAMBDAS DEPLOYMENT STATUS ===')
for fn, s in new_status.items():
    print(f"  {fn}: {s}")

print()
print('=== S3 CRR STATUS ===')
print(json.dumps(crr_status, indent=2, default=str))

print()
print('=== EXISTING STRESS S3 OUTPUTS ===')
for k, v in sorted(stress_outputs.items()):
    print(f"  {k}  ({v['size']/1024:.0f}KB, {v['age_h']}h ago)")
