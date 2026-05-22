#!/usr/bin/env python3
"""ops 1065 — deep schema inspection of carry-surface + engine-contribution"""
import json, boto3, os
s3 = boto3.client('s3', region_name='us-east-1')

# Carry surface
obj = s3.get_object(Bucket='justhodl-dashboard-live', Key='data/carry-surface.json')
carry = json.loads(obj['Body'].read())

# Engine contribution
obj = s3.get_object(Bucket='justhodl-dashboard-live', Key='data/engine-contributions.json')
ec = json.loads(obj['Body'].read())

# Cross-asset confirm
obj = s3.get_object(Bucket='justhodl-dashboard-live', Key='data/cross-asset-confirm.json')
cac = json.loads(obj['Body'].read())

report = {
    'carry_surface': {
        'top_keys': list(carry.keys()),
        'regime_summary': carry.get('regime_summary'),
        'financing_rate_pct': carry.get('financing_rate_pct'),
        'n_assets': carry.get('n_assets'),
        'by_class_keys': list((carry.get('by_class') or {}).keys()),
        'by_class_sample': {
            cls: {
                'keys': list(v.keys()) if isinstance(v, dict) else 'not_dict',
                'sample_asset': (v.get('assets') or v.get('rows') or [])[:1] if isinstance(v, dict) else None,
            }
            for cls, v in list((carry.get('by_class') or {}).items())[:5]
        },
        'cross_asset_top_first3': (carry.get('cross_asset_top') or [])[:3],
        'cross_asset_bottom_first3': (carry.get('cross_asset_bottom') or [])[:3],
        'risk_adjusted_leaders_first3': (carry.get('risk_adjusted_leaders') or [])[:3],
    },
    'engine_contribution': {
        'top_keys': list(ec.keys()),
        'sample_engine': (ec.get('engines') or [])[:2],
        'leader': ec.get('leader'),
        'laggard': ec.get('laggard'),
        'verdict_counts': ec.get('verdict_counts'),
    },
    'cross_asset_confirm': {
        'top_keys': list(cac.keys()),
        'regime_state': cac.get('regime_state'),
        'components': cac.get('components'),
        'sample_overlay': (cac.get('signal_overlays') or [])[:2],
    },
}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1065.json','w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str)[:5000])
