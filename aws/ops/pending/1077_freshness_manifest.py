#!/usr/bin/env python3
"""ops 1077 — add 6 new critical Lambda outputs to freshness manifest

Pattern from ops 1066. Adds key_overrides for each new output so the
fleet-freshness-monitor knows when these are stale.

Schedules:
  data/dr-snapshot-latest.json       daily 06UTC      → max_age_h = 36
  data/cost-anomaly.json             daily 09UTC      → max_age_h = 36
  data/macro-calendar.json           daily 11UTC      → max_age_h = 36
  data/fed-nlp.json                  every 6h         → max_age_h = 12
  data/news-wire.json                every 15min      → max_age_h = 2
  data/concentration-liquidity.json  daily 14UTC      → max_age_h = 36
"""
import json, boto3, os
from datetime import datetime, timezone

s3 = boto3.client('s3', region_name='us-east-1')
KEY = 'data/_freshness-manifest.json'
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())

try:
    obj = s3.get_object(Bucket='justhodl-dashboard-live', Key=KEY)
    manifest = json.loads(obj['Body'].read())
except Exception as e:
    print(f"manifest load failed: {e}")
    manifest = {'rules': [], 'exclude_prefixes': [], 'key_overrides': {}}

before_count = len(manifest.get('key_overrides', {}))

manifest.setdefault('key_overrides', {})

new_overrides = {
    'data/dr-snapshot-latest.json': {
        'max_age_h': 36,
        'description': 'Disaster Recovery snapshot manifest — daily 06UTC, alert if >36h stale',
    },
    'data/cost-anomaly.json': {
        'max_age_h': 36,
        'description': 'AWS+Anthropic cost anomaly detector — daily 09UTC, alert if >36h stale',
    },
    'data/macro-calendar.json': {
        'max_age_h': 36,
        'description': 'Macro events calendar with portfolio sensitivity — daily 11UTC, alert if >36h stale',
    },
    'data/fed-nlp.json': {
        'max_age_h': 12,
        'description': 'Fed communications hawkish/dovish NLP drift — every 6h, alert if >12h stale',
    },
    'data/news-wire.json': {
        'max_age_h': 2,
        'description': 'Real-time news wire with portfolio impact scoring — every 15min, alert if >2h stale',
    },
    'data/concentration-liquidity.json': {
        'max_age_h': 36,
        'description': 'Position concentration, sector/factor, liquidity (days-to-exit 20% ADV) — daily 14UTC, alert if >36h stale',
    },
}
manifest['key_overrides'].update(new_overrides)

manifest['_last_updated'] = datetime.now(timezone.utc).isoformat()
manifest['_last_updater'] = 'ops/1077'

s3.put_object(
    Bucket='justhodl-dashboard-live',
    Key=KEY,
    Body=json.dumps(manifest, indent=2).encode(),
    ContentType='application/json',
)

after_count = len(manifest['key_overrides'])

report = {
    'started_at': datetime.now(timezone.utc).isoformat(),
    'before_overrides': before_count,
    'after_overrides': after_count,
    'added': list(new_overrides.keys()),
    'manifest_uri': f"s3://justhodl-dashboard-live/{KEY}",
}

# Trigger freshness monitor to rebuild
try:
    lam = boto3.client('lambda', region_name='us-east-1')
    inv = lam.invoke(FunctionName='justhodl-fleet-freshness-monitor', InvocationType='Event')
    report['monitor_kicked'] = inv.get('StatusCode')
except Exception as e:
    report['monitor_kick_err'] = str(e)[:200]

out_path = os.path.join(REPO_ROOT, 'aws/ops/reports/1077.json')
os.makedirs(os.path.dirname(out_path), exist_ok=True)
with open(out_path, 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
