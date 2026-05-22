#!/usr/bin/env python3
"""ops 1047 — verify fleet-freshness-monitor deployed + bootstrap manifest"""
import json, boto3, os, time
from datetime import datetime, timezone

REGION = 'us-east-1'
lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)
logs = boto3.client('logs', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# 1. Verify both monitors
for fn_name, rule_name in [
    ('justhodl-fleet-error-monitor', 'fleet-error-monitor-5min'),
    ('justhodl-fleet-freshness-monitor', 'fleet-freshness-monitor-30min'),
]:
    info = {}
    try:
        cfg = lam.get_function_configuration(FunctionName=fn_name)
        info['lambda'] = {
            'exists': True,
            'last_modified': cfg.get('LastModified'),
            'dlq': bool((cfg.get('DeadLetterConfig') or {}).get('TargetArn')),
            'xray': cfg.get('TracingConfig', {}).get('Mode') == 'Active',
            'memory': cfg.get('MemorySize'),
            'timeout': cfg.get('Timeout'),
        }
    except Exception as e:
        info['lambda'] = {'exists': False, 'error': str(e)[:200]}
    try:
        r = events.describe_rule(Name=rule_name)
        info['eb_rule'] = {'state': r.get('State'), 'cron': r.get('ScheduleExpression')}
    except Exception as e:
        info['eb_rule'] = {'error': str(e)[:200]}
    report[fn_name] = info

# 2. Invoke freshness-monitor (bootstrap)
print("[2] Invoking freshness-monitor (1st run = bootstrap)...")
try:
    inv = lam.invoke(
        FunctionName='justhodl-fleet-freshness-monitor',
        InvocationType='RequestResponse',
        Payload=b'{}',
    )
    payload = inv['Payload'].read().decode()
    report['freshness_bootstrap'] = {
        'status': inv['StatusCode'],
        'function_error': inv.get('FunctionError', 'none'),
        'response': payload[:1000],
    }
    print(f"  status={inv['StatusCode']}  fn_error={inv.get('FunctionError','none')}")
    print(f"  resp: {payload[:300]}")
except Exception as e:
    report['freshness_bootstrap'] = {'error': str(e)[:300]}

# 3. Check manifest written
try:
    obj = s3.head_object(Bucket='justhodl-dashboard-live', Key='data/_freshness-manifest.json')
    report['manifest'] = {
        'size': obj['ContentLength'],
        'last_modified': obj['LastModified'].isoformat(),
    }
    # Read it to count entries
    obj = s3.get_object(Bucket='justhodl-dashboard-live', Key='data/_freshness-manifest.json')
    manifest = json.loads(obj['Body'].read().decode())
    report['manifest']['n_keys'] = len(manifest)
    # Sample some
    report['manifest']['sample'] = list(manifest.keys())[:10]
except Exception as e:
    report['manifest'] = {'error': str(e)[:200]}

# 4. Second invoke (now with manifest — should actually check)
print("[4] 2nd invoke (with manifest)...")
try:
    inv = lam.invoke(
        FunctionName='justhodl-fleet-freshness-monitor',
        InvocationType='RequestResponse',
        Payload=b'{}',
    )
    payload = inv['Payload'].read().decode()
    report['freshness_run2'] = {
        'status': inv['StatusCode'],
        'function_error': inv.get('FunctionError', 'none'),
        'response': payload[:1500],
    }
    print(f"  resp: {payload[:500]}")
except Exception as e:
    report['freshness_run2'] = {'error': str(e)[:300]}

# 5. Read run state
try:
    obj = s3.get_object(Bucket='justhodl-dashboard-live', Key='data/_freshness-monitor.json')
    state = json.loads(obj['Body'].read().decode())
    report['s3_state'] = {
        'n_keys_tracked': state.get('n_keys_tracked'),
        'counts': state.get('counts'),
        'alerts_raised': state.get('alerts_raised'),
        'top_5_stale': state.get('stale', [])[:5],
        'missing': state.get('missing', [])[:5],
    }
except Exception as e:
    report['s3_state'] = {'error': str(e)[:200]}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1047.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(json.dumps(report, indent=2, default=str)[:3000])
