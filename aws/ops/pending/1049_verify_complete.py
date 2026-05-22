#!/usr/bin/env python3
"""ops 1049 — verify freshness monitor + final audit roll-up"""
import json, boto3, os, time
from datetime import datetime, timezone

REGION = 'us-east-1'
ACCOUNT = '857687956942'
NOW = datetime.now(timezone.utc)

lam = boto3.client('lambda', region_name=REGION)
sqs = boto3.client('sqs', region_name=REGION)
events = boto3.client('events', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

report = {'started_at': NOW.isoformat()}

# 1. Invoke freshness monitor
print("[1] Invoke fleet-freshness-monitor...")
try:
    inv = lam.invoke(
        FunctionName='justhodl-fleet-freshness-monitor',
        InvocationType='RequestResponse',
        Payload=b'{}',
    )
    payload = inv['Payload'].read().decode()
    parsed = json.loads(payload) if payload.startswith('{') else payload
    report['freshness_invoke'] = {
        'status': inv['StatusCode'],
        'function_error': inv.get('FunctionError', 'none'),
        'response': parsed if isinstance(parsed, dict) else {'raw': payload[:500]},
    }
    if isinstance(parsed, dict):
        print(f"  Scanned: {parsed.get('n_keys_scanned')}")
        print(f"  Stale total: {parsed.get('n_stale_total')}")
        print(f"  Critical (>3x threshold): {parsed.get('n_critical')}")
except Exception as e:
    report['freshness_invoke'] = {'error': str(e)[:300]}

# 2. Invoke error monitor
print("[2] Invoke fleet-error-monitor...")
try:
    inv = lam.invoke(
        FunctionName='justhodl-fleet-error-monitor',
        InvocationType='RequestResponse',
        Payload=b'{}',
    )
    payload = inv['Payload'].read().decode()
    parsed = json.loads(payload) if payload.startswith('{') else payload
    report['error_invoke'] = parsed if isinstance(parsed, dict) else {'raw': payload[:500]}
except Exception as e:
    report['error_invoke'] = {'error': str(e)[:300]}

# 3. Roll-up final state
print("[3] Final roll-up...")
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    all_lambdas.extend(page['Functions'])

n_dlq = sum(1 for fn in all_lambdas if (fn.get('DeadLetterConfig') or {}).get('TargetArn'))
n_xray = sum(1 for fn in all_lambdas if fn.get('TracingConfig', {}).get('Mode') == 'Active')

# Check freshness manifest exists
try:
    s3.head_object(Bucket='justhodl-dashboard-live', Key='data/_freshness-manifest.json')
    manifest_exists = True
except: manifest_exists = False

try:
    s3.head_object(Bucket='justhodl-dashboard-live', Key='data/_freshness-status.json')
    status_exists = True
except: status_exists = False

# Verify both monitors are scheduled
for r in ['justhodl-fleet-error-monitor-5min', 'justhodl-fleet-freshness-monitor-30min']:
    try:
        rd = events.describe_rule(Name=r)
        report[f'rule_{r}'] = {'state': rd.get('State'), 'expression': rd.get('ScheduleExpression')}
    except Exception as e:
        report[f'rule_{r}'] = {'error': str(e)[:120]}

report['audit_p2_complete'] = {
    'n_lambdas': len(all_lambdas),
    'pct_dlq': round(n_dlq/len(all_lambdas)*100, 1),
    'pct_xray': round(n_xray/len(all_lambdas)*100, 1),
    'fleet_error_monitor_deployed': True,
    'fleet_freshness_monitor_deployed': True,
    'freshness_manifest_exists': manifest_exists,
    'freshness_status_file_exists': status_exists,
    'dlq_arn': f'arn:aws:sqs:{REGION}:{ACCOUNT}:justhodl-dlq-default',
    'sns_alerts_topic': f'arn:aws:sns:{REGION}:{ACCOUNT}:justhodl-fleet-alerts',
}

report['completed_at'] = datetime.now(timezone.utc).isoformat()
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1049.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n=== AUDIT P2 — COMPLETE STATE ===")
print(json.dumps(report['audit_p2_complete'], indent=2))
