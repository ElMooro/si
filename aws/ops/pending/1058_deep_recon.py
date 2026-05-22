#!/usr/bin/env python3
"""ops 1058 — DEEPER verification before building 5 new exp engines"""
import json, boto3, os
from datetime import datetime, timezone

REGION = 'us-east-1'
lam = boto3.client('lambda', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)
events = boto3.client('events', region_name=REGION)

CANDIDATES = {
    "1_carry_surface": {
        "lambda_patterns": ["carry", "yield-rank", "dividend-rank", "roll-yield", "funding-rank"],
        "s3_patterns": ["carry", "carry-surface", "carry-rank"],
        "rule_patterns": ["carry"],
    },
    "2_counterfactual": {
        "lambda_patterns": ["counterfactual", "engine-contrib", "ablation", "leave-one-out", "marginal-engine"],
        "s3_patterns": ["counterfactual", "engine-contrib", "ablation"],
        "rule_patterns": ["counterfactual", "engine-contrib"],
    },
    "3_cross_asset_confirm": {
        "lambda_patterns": ["cross-asset-confirm", "confirmation-filter", "macro-confirm", "regime-confirm-filter", "confirm-filter"],
        "s3_patterns": ["cross-asset-confirm", "confirmation-filter", "macro-confirm"],
        "rule_patterns": ["cross-asset-confirm", "confirmation-filter"],
    },
    "4_earnings_linguistic": {
        "lambda_patterns": ["earnings-linguistic", "earnings-nlp", "transcript-nlp", "call-language", "hedging-language", "call-nlp", "earnings-text"],
        "s3_patterns": ["earnings-linguistic", "earnings-language", "transcript-nlp"],
        "rule_patterns": ["earnings-linguistic", "earnings-language"],
    },
    "5_engine_robustness": {
        "lambda_patterns": ["robustness", "perturbation", "engine-health", "stability-test", "ct-scan", "noise-test"],
        "s3_patterns": ["robustness", "engine-health", "engine-stability", "perturbation"],
        "rule_patterns": ["robustness", "engine-health"],
    },
}

report = {'started_at': datetime.now(timezone.utc).isoformat()}

all_lambdas = []
for page in lam.get_paginator('list_functions').paginate():
    all_lambdas.extend([fn['FunctionName'] for fn in page['Functions']])
report['n_lambdas'] = len(all_lambdas)

all_keys = []
for page in s3.get_paginator('list_objects_v2').paginate(Bucket='justhodl-dashboard-live', Prefix='data/'):
    for obj in page.get('Contents', []):
        all_keys.append(obj['Key'])

all_rules = []
for page in events.get_paginator('list_rules').paginate():
    all_rules.extend([r['Name'] for r in page['Rules']])

def lc(s): return s.lower()
ll_lc = [lc(x) for x in all_lambdas]
ak_lc = [lc(x) for x in all_keys]
ar_lc = [lc(x) for x in all_rules]

results = {}
for idea, patterns in CANDIDATES.items():
    lambda_hits = sorted(set([all_lambdas[i] for i in range(len(all_lambdas))
                              for kw in patterns['lambda_patterns']
                              if lc(kw) in ll_lc[i]]))
    s3_hits = sorted(set([all_keys[i] for i in range(len(all_keys))
                          for kw in patterns['s3_patterns']
                          if lc(kw) in ak_lc[i]]))
    rule_hits = sorted(set([all_rules[i] for i in range(len(all_rules))
                            for kw in patterns['rule_patterns']
                            if lc(kw) in ar_lc[i]]))
    
    if not lambda_hits and not s3_hits and not rule_hits:
        verdict = "BUILD"
    elif lambda_hits and s3_hits:
        verdict = "FULL_IMPL_EXISTS"
    else:
        verdict = "PARTIAL"
    
    results[idea] = {
        'verdict': verdict,
        'lambda_hits': lambda_hits[:15],
        's3_hits': s3_hits[:15],
        'rule_hits': rule_hits[:10],
    }

report['results'] = results
report['n_s3_keys'] = len(all_keys)
report['n_eb_rules'] = len(all_rules)

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1058.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(json.dumps(report, indent=2, default=str))
