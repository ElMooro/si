#!/usr/bin/env python3
"""ops 1064 — read carry-surface.json to design HTML around real schema"""
import json, boto3, os
s3 = boto3.client('s3', region_name='us-east-1')
obj = s3.get_object(Bucket='justhodl-dashboard-live', Key='data/carry-surface.json')
d = json.loads(obj['Body'].read())
# Top-level keys + sample of each
schema = {
    'top_keys': list(d.keys()),
    'regime_keys': list((d.get('regime') or {}).keys())[:20],
    'asset_classes_keys': list((d.get('asset_classes') or {}).keys()),
    'sample_top_carry_to_vol': (d.get('global_top_carry_to_vol') or d.get('top_assets') or [])[:3],
    'sample_unwind_alert': (d.get('global_unwind_alerts') or d.get('unwind_alerts') or [])[:2],
    'first_asset_class_sample': None,
}
ac = d.get('asset_classes') or {}
if ac:
    first_cls = list(ac.keys())[0]
    first_data = ac[first_cls]
    if isinstance(first_data, dict):
        schema['first_asset_class_sample'] = {
            'class': first_cls,
            'data_keys': list(first_data.keys()),
            'first_asset_sample': (first_data.get('assets') or first_data.get('rows') or [])[:1],
        }
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1064.json','w') as f:
    json.dump({'schema': schema, 'regime': d.get('regime'), 
               'top_5_carry_to_vol': (d.get('global_top_carry_to_vol') or [])[:5]},
              f, indent=2, default=str)
print(json.dumps(schema, indent=2, default=str)[:2500])
