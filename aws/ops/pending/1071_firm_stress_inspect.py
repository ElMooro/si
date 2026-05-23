#!/usr/bin/env python3
"""ops 1071 — Inspect firm-stress.json to confirm SKIP of historical-replay build."""
import json, os, boto3
from datetime import datetime, timezone
s3 = boto3.client('s3', region_name='us-east-1')

report = {}
try:
    obj = s3.get_object(Bucket='justhodl-dashboard-live', Key='data/firm-stress.json')
    content = json.loads(obj['Body'].read())
    report['top_keys'] = list(content.keys()) if isinstance(content, dict) else 'not-dict'
    if isinstance(content, dict):
        report['n_scenarios'] = len(content.get('scenarios', []))
        scens = content.get('scenarios', [])[:3]
        report['scenario_sample'] = [{'name': s.get('name'), 'type': s.get('type'),
                                       'pnl': s.get('pnl_pct', s.get('pnl_usd', s.get('total_pnl')))}
                                      for s in scens]
        # Look for historical-replay scenario keys
        for s in content.get('scenarios', []):
            n = (s.get('name','') + s.get('key','')).lower()
            if any(t in n for t in ['1987','black monday','lehman','2008','covid','yen','volmageddon','ltcm']):
                report.setdefault('historical_replays_found', []).append(s.get('name'))
    report['sample'] = json.dumps(content, default=str, indent=2)[:2500]
except Exception as e:
    report['error'] = str(e)

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1071.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str)[:3500])
