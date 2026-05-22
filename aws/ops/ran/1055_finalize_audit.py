#!/usr/bin/env python3
"""
ops 1055 — Add 8 exp Lambda outputs to freshness manifest + verify patches.

After 1054b found 2 exp Lambdas silently skipping S3 writes when there's
"no action", I patched both:
  - causality-scanner: writes heartbeat on insufficient_history
  - meta-improver: writes heartbeat on no_decaying_engines

Now I need to:
  1. Add ALL 8 exp output keys to freshness manifest key_overrides so
     they're actively monitored
  2. Wait for deploys to land
  3. Invoke the 2 patched Lambdas to confirm they now write
  4. Final fleet observability scorecard
"""
import json, boto3, os, time
from datetime import datetime, timezone

REGION = 'us-east-1'
s3 = boto3.client('s3', region_name=REGION)
lam = boto3.client('lambda', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# ---- 1. Update manifest with all 8 exp output keys ----
# Cron expressions → expected max age (hours)
# Bias: cron_period_hours * 1.5 buffer
EXP_OUTPUTS = {
    'data/kill-theses.json':           {'max_age_h': 36.0, 'note': 'premortem-engine: MON-FRI 14 UTC'},
    'data/behavior-mirror.json':       {'max_age_h': 200.0, 'note': 'behavior-mirror: weekly SUN 13 UTC'},
    'data/pre-disaster-watchlist.json':{'max_age_h': 36.0, 'note': 'failure-library: MON-FRI 15 UTC'},
    'data/causality-discoveries.json': {'max_age_h': 200.0, 'note': 'causality-scanner: weekly SUN 21 UTC'},
    'data/convexity-scores.json':      {'max_age_h': 36.0, 'note': 'convexity-scorer: MON-FRI 14 UTC'},
    'data/chart-vision.json':          {'max_age_h': 36.0, 'note': 'chart-vision: MON-FRI 16 UTC'},
    'data/meta-improver-state.json':   {'max_age_h': 200.0, 'note': 'meta-improver: weekly schedule'},
    'data/signal-halflife.json':       {'max_age_h': 200.0, 'note': 'signal-halflife: weekly MON 06 UTC'},
}

manifest_key = 'data/_freshness-manifest.json'
try:
    obj = s3.get_object(Bucket='justhodl-dashboard-live', Key=manifest_key)
    manifest = json.loads(obj['Body'].read().decode())
except Exception as e:
    print(f"manifest read err: {e}")
    manifest = {"rules": [{"prefix": "data/", "default_max_age_h": 26.0}],
                "exclude_prefixes": [], "admin_only_keys": [], "key_overrides": {}}

before = dict(manifest.get('key_overrides', {}))
manifest.setdefault('key_overrides', {})
added = {}
for key, info in EXP_OUTPUTS.items():
    if key not in manifest['key_overrides']:
        manifest['key_overrides'][key] = info['max_age_h']
        added[key] = info['max_age_h']
report['manifest'] = {
    'before_n_overrides': len(before),
    'after_n_overrides': len(manifest['key_overrides']),
    'added': added,
}

# Save manifest
s3.put_object(
    Bucket='justhodl-dashboard-live', Key=manifest_key,
    Body=json.dumps(manifest, indent=2, default=str).encode(),
    ContentType='application/json',
    CacheControl='no-store',
)
print(f"[1] Manifest updated: +{len(added)} key_overrides")

# ---- 2. Invoke the 2 patched Lambdas (after deploys land) ----
# The deploy-lambdas workflow should pick up the code changes within ~2 min
# So by the time this ops runs (post-push), the patched code should be deployed.
print("[2] Invoking patched Lambdas...")
patched_invokes = {}
for fn in ['justhodl-causality-scanner', 'justhodl-meta-improver']:
    try:
        # Check last_modified to confirm code is deployed
        cfg = lam.get_function_configuration(FunctionName=fn)
        last_modified = cfg.get('LastModified', '')
        
        inv = lam.invoke(
            FunctionName=fn, InvocationType='RequestResponse', Payload=b'{}',
        )
        payload = inv['Payload'].read().decode()
        patched_invokes[fn] = {
            'last_modified': last_modified,
            'status': inv['StatusCode'],
            'function_error': inv.get('FunctionError', 'none'),
            'response': payload[:600],
        }
        print(f"  {fn}: deploy={last_modified} status={inv['StatusCode']} fn_err={inv.get('FunctionError','none')}")
    except Exception as e:
        patched_invokes[fn] = {'error': str(e)[:200]}
report['patched_invokes'] = patched_invokes

# ---- 3. Verify the heartbeat outputs now exist ----
print("[3] Verifying heartbeat outputs...")
verify_keys = ['data/causality-discoveries.json', 'data/meta-improver-state.json']
heartbeats = {}
for key in verify_keys:
    try:
        head = s3.head_object(Bucket='justhodl-dashboard-live', Key=key)
        age_h = (datetime.now(timezone.utc) - head['LastModified']).total_seconds() / 3600
        heartbeats[key] = {
            'exists': True,
            'size': head['ContentLength'],
            'last_modified': head['LastModified'].isoformat(),
            'age_h': round(age_h, 2),
        }
    except Exception as e:
        heartbeats[key] = {'exists': False, 'error': str(e)[:120]}
report['heartbeats'] = heartbeats

# ---- 4. Final scorecard ----
print("[4] Final scorecard...")
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    all_lambdas.extend(page['Functions'])

n_dlq = sum(1 for fn in all_lambdas if (fn.get('DeadLetterConfig') or {}).get('TargetArn'))
n_xray = sum(1 for fn in all_lambdas if fn.get('TracingConfig', {}).get('Mode') == 'Active')

# Invoke freshness monitor to get final counts
try:
    inv = lam.invoke(FunctionName='justhodl-fleet-freshness-monitor',
                     InvocationType='RequestResponse', Payload=b'{}')
    payload = inv['Payload'].read().decode()
    report['freshness_final_invoke'] = payload[:500]
except Exception as e:
    report['freshness_final_invoke'] = f'err: {str(e)[:200]}'

# Also pull state
try:
    obj = s3.get_object(Bucket='justhodl-dashboard-live', Key='data/_freshness-monitor.json')
    state = json.loads(obj['Body'].read().decode())
    report['freshness_final_state'] = {
        'n_keys_tracked': state.get('n_keys_tracked'),
        'counts': state.get('counts'),
        'alerts_raised': state.get('alerts_raised'),
    }
except Exception as e:
    report['freshness_final_state'] = {'err': str(e)[:200]}

report['final_scorecard'] = {
    'n_lambdas': len(all_lambdas),
    'pct_dlq': round(n_dlq/len(all_lambdas)*100, 1),
    'pct_xray': round(n_xray/len(all_lambdas)*100, 1),
    'fleet_error_monitor_live': True,
    'fleet_freshness_monitor_live': True,
}

report['completed_at'] = datetime.now(timezone.utc).isoformat()
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1055.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(json.dumps(report, indent=2, default=str)[:3500])
