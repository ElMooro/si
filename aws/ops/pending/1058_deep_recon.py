#!/usr/bin/env python3
"""
ops 1058 — DEEP RECON before building 5 new engines

Beyond keyword matching, this:
  1. Lists Lambda NAMES + descriptions matching expanded keyword sets
  2. Lists S3 keys + sizes matching same sets
  3. Inspects the description/code-fingerprint of any partial matches
  4. Confirms each candidate is genuinely buildable (not partial-built)
"""
import json, boto3, os, re, urllib.request, io, zipfile
from datetime import datetime, timezone

REGION = 'us-east-1'
lam = boto3.client('lambda', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)

# Expanded keyword sets per candidate
CHECKS = {
    "carry_surface": {
        "lambda_kws": ["carry", "yield-vs", "net-yield", "holding-cost",
                       "cost-of-carry", "xccy", "rate-diff", "roll-yield",
                       "contango", "backwardation", "funding-rate"],
        "s3_kws": ["carry", "rate-diff", "roll-yield", "funding-rate", "contango"],
    },
    "engine_contribution": {
        "lambda_kws": ["contribution", "marginal", "leave-one-out", "ablation",
                       "counterfactual", "pnl-delta", "engine-pnl", "alpha-attribution",
                       "engine-importance", "shapley"],
        "s3_kws": ["contribution", "marginal", "ablation", "counterfactual",
                   "engine-importance"],
    },
    "cross_asset_confirm": {
        "lambda_kws": ["cross-asset-confirm", "confirm-filter", "regime-veto",
                       "structure-filter", "multi-asset-veto", "signal-validator"],
        "s3_kws": ["cross-asset-confirm", "regime-veto", "signal-validator"],
    },
    "earnings_linguistic": {
        "lambda_kws": ["linguistic", "transcript-nlp", "call-language",
                       "earnings-language", "hedging-words", "evasion-detect",
                       "transcript-score", "call-analysis"],
        "s3_kws": ["linguistic", "transcript", "call-language", "hedging-words"],
    },
    "engine_robustness": {
        "lambda_kws": ["robustness", "perturbation", "stability-test",
                       "noise-test", "stress-test-engine", "engine-stability",
                       "ablation-test", "rank-stability"],
        "s3_kws": ["robustness", "stability", "engine-health"],
    },
}

report = {'started_at': datetime.now(timezone.utc).isoformat()}

# Pull all Lambdas WITH descriptions this time
print("[1] Listing Lambdas with descriptions...")
all_lambdas = []
paginator = lam.get_paginator('list_functions')
for page in paginator.paginate():
    for fn in page['Functions']:
        all_lambdas.append({
            'name': fn['FunctionName'],
            'desc': fn.get('Description', ''),
            'last_modified': fn.get('LastModified', ''),
        })
report['n_lambdas'] = len(all_lambdas)

# Pull all S3 keys with sizes
print("[2] Listing S3 keys...")
all_s3 = []
paginator = s3.get_paginator('list_objects_v2')
for page in paginator.paginate(Bucket='justhodl-dashboard-live', Prefix='data/'):
    for obj in page.get('Contents', []):
        all_s3.append({'key': obj['Key'], 'size': obj['Size']})
report['n_s3_keys'] = len(all_s3)

# Per-candidate deep search
print("[3] Per-candidate deep search...")
for candidate, cfg in CHECKS.items():
    lambda_hits = []
    for fn in all_lambdas:
        for kw in cfg['lambda_kws']:
            blob = (fn['name'] + ' ' + fn['desc']).lower()
            if kw.lower() in blob:
                lambda_hits.append({
                    'kw': kw, 'name': fn['name'], 'desc': fn['desc'][:120],
                    'last_modified': fn['last_modified'],
                })
                break  # one hit per Lambda
    s3_hits = []
    for obj in all_s3:
        for kw in cfg['s3_kws']:
            if kw.lower() in obj['key'].lower():
                s3_hits.append({'kw': kw, 'key': obj['key'], 'size': obj['size']})
                break
    report[candidate] = {
        'verdict': 'EXISTS' if (lambda_hits or s3_hits) else 'NOVEL',
        'lambda_hits': lambda_hits[:8],
        's3_hits': s3_hits[:8],
    }

# Also pull a directory listing of aws/lambdas/ to cross-check what's in repo vs deployed
print("[4] Repo Lambda dirs...")
local_lambdas_dir = '/home/runner/work/si/si/aws/lambdas'  # path on runner
try:
    if os.path.exists(local_lambdas_dir):
        repo_dirs = sorted(os.listdir(local_lambdas_dir))
        report['repo_lambda_dirs_n'] = len(repo_dirs)
        report['repo_lambda_dirs_carry'] = [d for d in repo_dirs if 'carry' in d.lower()]
    else:
        report['repo_path_missing'] = local_lambdas_dir
except Exception as e:
    report['repo_scan_err'] = str(e)[:120]

# Summary
report['summary'] = {
    'novel': [k for k, v in report.items() if isinstance(v, dict) and v.get('verdict') == 'NOVEL'],
    'exists': [k for k, v in report.items() if isinstance(v, dict) and v.get('verdict') == 'EXISTS'],
}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1058.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(json.dumps(report['summary'], indent=2))
for k in CHECKS:
    r = report[k]
    print(f"\n{'='*70}\n{k}: {r['verdict']}")
    if r.get('lambda_hits'):
        for h in r['lambda_hits'][:5]:
            print(f"  Lambda: {h['name']}")
            print(f"    desc: {h['desc']}")
    if r.get('s3_hits'):
        for h in r['s3_hits'][:5]:
            print(f"  S3: {h['key']} ({h['size']}b)")
