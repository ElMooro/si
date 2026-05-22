#!/usr/bin/env python3
"""ops 1066 — add 4 new exp engine outputs to freshness manifest"""
import json, boto3, os
from datetime import datetime, timezone

s3 = boto3.client('s3', region_name='us-east-1')
KEY = 'data/_freshness-manifest.json'

# Pull current manifest
try:
    obj = s3.get_object(Bucket='justhodl-dashboard-live', Key=KEY)
    manifest = json.loads(obj['Body'].read())
except Exception as e:
    print(f"manifest load failed: {e}")
    manifest = {'rules': [], 'exclude_prefixes': [], 'key_overrides': {}}

before = {
    'n_overrides': len(manifest.get('key_overrides', {})),
    'has_carry_surface': 'data/carry-surface.json' in manifest.get('key_overrides', {}),
    'has_engine_contrib': 'data/engine-contributions.json' in manifest.get('key_overrides', {}),
    'has_cross_asset': 'data/cross-asset-confirm.json' in manifest.get('key_overrides', {}),
    'has_engine_robust': 'data/engine-robustness.json' in manifest.get('key_overrides', {}),
}

# Add overrides for the 4 new outputs
manifest.setdefault('key_overrides', {})
new_overrides = {
    'data/carry-surface.json': {
        'max_age_h': 6,
        'description': 'Universal Carry Surface — refreshes every 4h, alert if >6h stale',
    },
    'data/engine-contributions.json': {
        'max_age_h': 200,
        'description': 'Counterfactual Engine Contribution — weekly Sunday 02 UTC, alert if >200h stale',
    },
    'data/cross-asset-confirm.json': {
        'max_age_h': 5,
        'description': 'Cross-Asset Confirmation Filter — refreshes every 3h, alert if >5h stale',
    },
    'data/engine-robustness.json': {
        'max_age_h': 200,
        'description': 'Engine Health CT-Scan — weekly Tuesday 04 UTC, alert if >200h stale',
    },
    'data/earnings-nlp.json': {
        'max_age_h': 36,
        'description': 'Earnings Call NLP — daily, alert if >36h stale',
    },
}
manifest['key_overrides'].update(new_overrides)

# Write back
s3.put_object(
    Bucket='justhodl-dashboard-live',
    Key=KEY,
    Body=json.dumps(manifest, indent=2, default=str).encode(),
    ContentType='application/json',
)

after = {
    'n_overrides': len(manifest.get('key_overrides', {})),
    'added_keys': list(new_overrides.keys()),
}

# Run a dry-check by invoking the freshness monitor
lam = boto3.client('lambda', region_name='us-east-1')
try:
    inv = lam.invoke(
        FunctionName='justhodl-fleet-freshness-monitor',
        InvocationType='RequestResponse',
        Payload=b'{}',
    )
    response = inv['Payload'].read().decode()
except Exception as e:
    response = f"invoke error: {e}"

report = {
    'started_at': datetime.now(timezone.utc).isoformat(),
    'before': before,
    'after': after,
    'freshness_monitor_invoke': response[:1500],
}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1066.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
