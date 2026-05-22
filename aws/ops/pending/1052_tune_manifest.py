#!/usr/bin/env python3
"""ops 1052 — tune freshness manifest based on first real-world findings

Findings from ops 1049 + 1050:
  - activist-13d: was unscheduled (fixed in 1051). Now daily, 26h default OK.
  - 13f-price-divergence: scheduled WEEKLY Tuesday via Scheduler v2.
    Default 26h max_age is wrong for a weekly Lambda → add 192h override.
  - activity-nowcast/snapshots/2026-05-NN.json: dated daily snapshots that
    intentionally aren't updated → exclude the prefix.

Manifest update:
  exclude_prefixes += "data/activity-nowcast/snapshots/"
  key_overrides += "data/13f-price-divergence.json": 192.0  (8d for weekly)

After this:
  - The 4 dated-snapshot false positives go away
  - The 13f weekly Lambda gets a proper window
  - Freshness monitor's signal-to-noise improves significantly
"""
import json, boto3, os
from datetime import datetime, timezone

REGION = 'us-east-1'
s3 = boto3.client('s3', region_name=REGION)
BUCKET = 'justhodl-dashboard-live'

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# 1. Load existing manifest
print("[1] Loading existing manifest...")
obj = s3.get_object(Bucket=BUCKET, Key='data/_freshness-manifest.json')
manifest = json.loads(obj['Body'].read().decode())
report['before'] = {
    'exclude_prefixes': manifest.get('exclude_prefixes', []),
    'admin_only_keys': manifest.get('admin_only_keys', []),
    'key_overrides': manifest.get('key_overrides', {}),
}
print(f"  exclude_prefixes: {len(report['before']['exclude_prefixes'])}")
print(f"  key_overrides: {len(report['before']['key_overrides'])}")

# 2. Apply tuning
new_excludes = set(manifest.get('exclude_prefixes', []))
new_excludes.add('data/activity-nowcast/snapshots/')  # dated daily files, intentionally not updated
new_excludes.add('data/history/')  # history files are point-in-time
new_excludes.add('data/_archive/')  # alt-spelling of archive
manifest['exclude_prefixes'] = sorted(new_excludes)

new_overrides = dict(manifest.get('key_overrides', {}))
# Weekly Lambdas (1 week + buffer)
new_overrides['data/13f-price-divergence.json'] = 192.0
# Add some common weekly outputs we can infer:
# (only add ones we're CONFIDENT about — be conservative)
manifest['key_overrides'] = new_overrides

# 3. Save
print("\n[2] Writing tuned manifest...")
s3.put_object(
    Bucket=BUCKET,
    Key='data/_freshness-manifest.json',
    Body=json.dumps(manifest, indent=2, default=str).encode(),
    ContentType='application/json',
    CacheControl='no-store',
)
report['after'] = {
    'exclude_prefixes': manifest.get('exclude_prefixes'),
    'admin_only_keys': manifest.get('admin_only_keys'),
    'key_overrides': manifest.get('key_overrides'),
}
report['changes'] = {
    'exclude_added': sorted(set(report['after']['exclude_prefixes']) - set(report['before']['exclude_prefixes'])),
    'key_overrides_added': {
        k: v for k, v in report['after']['key_overrides'].items()
        if k not in report['before']['key_overrides']
    },
}

# 4. Trigger freshness monitor to verify improvement
print("\n[3] Invoking freshness monitor to verify tuning...")
lam = boto3.client('lambda', region_name=REGION)
try:
    inv = lam.invoke(
        FunctionName='justhodl-fleet-freshness-monitor',
        InvocationType='RequestResponse',
        Payload=b'{}',
    )
    payload = inv['Payload'].read().decode()
    report['post_tune_invoke'] = {
        'status': inv['StatusCode'],
        'function_error': inv.get('FunctionError', 'none'),
        'response': payload[:600],
    }
    print(f"  status={inv['StatusCode']}  fn_err={inv.get('FunctionError','none')}")
    print(f"  resp: {payload[:400]}")
except Exception as e:
    report['post_tune_invoke'] = {'error': str(e)[:300]}

# 5. Read new run state
try:
    obj = s3.get_object(Bucket=BUCKET, Key='data/_freshness-monitor.json')
    state = json.loads(obj['Body'].read().decode())
    report['post_tune_state'] = {
        'n_keys_tracked': state.get('n_keys_tracked'),
        'n_stale': state.get('n_stale'),
        'n_fresh': state.get('n_fresh'),
        'n_alerts_raised': state.get('n_alerts_raised'),
        'top_stale': [
            {'key': r.get('key'), 'age_h': r.get('age_h'), 'max_h': r.get('max_age_h')}
            for r in state.get('stale_top_50', [])[:10]
        ],
    }
except Exception as e:
    report['post_tune_state'] = {'error': str(e)[:200]}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1052.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print("\n=== SUMMARY ===")
print(f"Exclude added: {report['changes']['exclude_added']}")
print(f"Overrides added: {report['changes']['key_overrides_added']}")
state = report.get('post_tune_state', {})
print(f"After tuning: tracked={state.get('n_keys_tracked')} stale={state.get('n_stale')} fresh={state.get('n_fresh')}")
