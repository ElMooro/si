#!/usr/bin/env python3
"""ops 1080 — add data/forward-returns.json to freshness manifest.

Weekly schedule (Sun 03 UTC) so set max_age_h to 180h (7.5 days)
to allow one missed run before alerting.
"""
import json, os, boto3
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

before = len(manifest.get('key_overrides', {}))

manifest.setdefault('key_overrides', {})
manifest['key_overrides']['data/forward-returns.json'] = {
    'max_age_h': 180,
    'description': 'Capital Compass forward 10y expected returns (Damodaran/AQR/GMO methodology) — weekly Sun 03 UTC, alert if >180h stale (one missed run)',
}
manifest['_last_updated'] = datetime.now(timezone.utc).isoformat()
manifest['_last_updater'] = 'ops/1080'

s3.put_object(
    Bucket='justhodl-dashboard-live',
    Key=KEY,
    Body=json.dumps(manifest, indent=2).encode(),
    ContentType='application/json',
)

# Kick monitor
report = {
    'started_at': datetime.now(timezone.utc).isoformat(),
    'before_overrides': before,
    'after_overrides': len(manifest['key_overrides']),
    'added_key': 'data/forward-returns.json',
}
try:
    lam = boto3.client('lambda', region_name='us-east-1')
    inv = lam.invoke(FunctionName='justhodl-fleet-freshness-monitor', InvocationType='Event')
    report['monitor_kicked'] = inv.get('StatusCode')
except Exception as e:
    report['monitor_kick_err'] = str(e)[:200]

out = os.path.join(REPO_ROOT, 'aws/ops/reports/1080.json')
os.makedirs(os.path.dirname(out), exist_ok=True)
with open(out, 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
