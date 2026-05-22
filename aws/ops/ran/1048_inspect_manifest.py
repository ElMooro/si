#!/usr/bin/env python3
"""ops 1048 — inspect existing freshness manifest schema"""
import json, boto3, os
from datetime import datetime, timezone

REGION = 'us-east-1'
s3 = boto3.client('s3', region_name=REGION)

report = {'started_at': datetime.now(timezone.utc).isoformat()}

try:
    obj = s3.get_object(Bucket='justhodl-dashboard-live', Key='data/_freshness-manifest.json')
    raw = obj['Body'].read().decode()
    d = json.loads(raw)
    report['size_bytes'] = len(raw)
    report['top_level_keys'] = list(d.keys()) if isinstance(d, dict) else 'not_dict'
    report['full'] = d
except Exception as e:
    report['error'] = str(e)[:300]

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1048.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str)[:3000])
