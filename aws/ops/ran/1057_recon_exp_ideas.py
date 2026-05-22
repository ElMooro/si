#!/usr/bin/env python3
"""
ops 1057 — RECON for next-wave exponential improvements

Before proposing any new feature, audit what already exists. For each
candidate idea, search Lambda names + S3 prefixes for matching keywords.
Returns: per-idea evidence ('not_found' = green light, otherwise lists hits).
"""
import json, boto3, os, re
from datetime import datetime, timezone

REGION = 'us-east-1'
lam = boto3.client('lambda', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

# Candidate ideas with search keywords
CANDIDATES = {
    "regime_switcher": [
        "regime-switch", "regime-conditional", "engine-activation",
        "regime-overlay", "regime-router", "regime-matrix"
    ],
    "pnl_attribution": [
        "pnl-attribution", "pnl-decomp", "performance-attribution",
        "factor-pnl", "attribution-engine", "return-attribution"
    ],
    "implied_probability": [
        "implied-prob", "implied-distribution", "options-implied",
        "vol-surface", "implied-skew", "rnd-recover"
    ],
    "counterfactual_backtest": [
        "counterfactual", "engine-contribution", "marginal-pnl",
        "leave-one-out", "ablation"
    ],
    "decision_journal_rlhf": [
        "decision-journal", "rlhf", "khalid-trained", "personal-llm",
        "trade-log", "decision-log"
    ],
    "dealer_gamma": [
        "dealer-gamma", "gamma-exposure", "gex", "vanna-charm",
        "options-positioning", "dealer-positioning"
    ],
    "cross_asset_confirm": [
        "cross-asset-confirm", "regime-confirm", "macro-confirm",
        "multi-asset-filter"
    ],
    "capacity_sizing": [
        "capacity", "market-impact", "adv-aware", "signal-capacity",
        "size-decay"
    ],
    "news_novelty": [
        "news-novelty", "news-embedding", "news-dedupe",
        "story-novelty", "headline-dedupe"
    ],
    "earnings_linguistic": [
        "earnings-linguistic", "call-nlp", "call-transcript-nlp",
        "earnings-language", "hedging-language", "ceo-language"
    ],
    "adversarial_robustness": [
        "robustness-test", "perturbation", "adversarial-test",
        "signal-stability", "noise-injection"
    ],
    "synthetic_assets": [
        "synthetic-asset", "synthetic-vix", "synthetic-credit",
        "constructed-instrument"
    ],
    "carry_surface": [
        "carry-surface", "universal-carry", "carry-cross-asset",
        "carry-engine"
    ],
    "hedge_effectiveness": [
        "hedge-effectiveness", "hedge-drift", "hedge-correlation",
        "hedge-quality"
    ],
    "behavioral_bias_intervention": [
        "behavioral-bias", "bias-mirror-active", "bias-alert",
        "anti-fomo", "anti-anchor"
    ],
    "sector_rotation_leadlag": [
        "sector-rotation", "sector-leadlag", "sector-cycle",
        "rotation-engine"
    ],
    "smart_money_crowding": [
        "crowding", "crowding-score", "vip-portfolio",
        "hedge-fund-vip", "smart-money-crowding"
    ],
    "macro_surprise_index": [
        "macro-surprise", "surprise-index", "citi-surprise"
    ],
    "vrp_tracker": [
        "vrp", "vol-risk-premium", "rv-iv", "vol-premium"
    ],
    "stop_loss_optimizer": [
        "stop-loss", "stop-target", "exit-optimizer", "kelly-sizing"
    ],
}

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# List all Lambdas
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    all_lambdas.extend(page['Functions'])
lambda_names_lower = [fn['FunctionName'].lower() for fn in all_lambdas]
report['n_lambdas'] = len(all_lambdas)

# List all S3 keys under data/ (just names, not contents)
all_s3_keys = []
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket='justhodl-dashboard-live', Prefix='data/'):
    for obj in page.get('Contents', []):
        all_s3_keys.append(obj['Key'].lower())
report['n_s3_keys'] = len(all_s3_keys)

# Search each candidate
results = {}
for idea, keywords in CANDIDATES.items():
    lambda_hits = []
    s3_hits = []
    for kw in keywords:
        kw_lower = kw.lower()
        for name in lambda_names_lower:
            if kw_lower in name:
                lambda_hits.append({'kw': kw, 'lambda': name})
        for key in all_s3_keys:
            if kw_lower in key:
                s3_hits.append({'kw': kw, 's3_key': key})
    results[idea] = {
        'verdict': 'EXISTS' if (lambda_hits or s3_hits) else 'NOT_FOUND',
        'lambda_hits': lambda_hits[:5],
        's3_hits': s3_hits[:5],
    }

report['candidates'] = results

# Summary
not_found = [k for k, v in results.items() if v['verdict'] == 'NOT_FOUND']
exists = [k for k, v in results.items() if v['verdict'] == 'EXISTS']
report['summary'] = {
    'truly_novel': not_found,
    'already_built_or_partial': exists,
}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1057.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(json.dumps(report, indent=2, default=str))
