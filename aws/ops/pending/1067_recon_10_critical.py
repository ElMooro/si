#!/usr/bin/env python3
"""ops 1067 — RECON for 10 critical Tier-0/1/2 improvements"""
import json, boto3, os
from datetime import datetime, timezone

REGION = 'us-east-1'
lam = boto3.client('lambda', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)
events = boto3.client('events', region_name=REGION)
ddb = boto3.client('dynamodb', region_name=REGION)

CANDIDATES = {
    "1_disaster_recovery": {
        "lambda_patterns": ["backup", "disaster", "dr-", "replication", "snapshot-all", "cross-region", "restore"],
        "s3_patterns": ["disaster-recovery", "backup/", "snapshot/lambdas", "snapshot-codes"],
        "rule_patterns": ["backup", "disaster", "snapshot-lambdas"],
    },
    "2_cost_anomaly": {
        "lambda_patterns": ["cost", "billing", "spend", "burn-rate", "cost-anomaly", "anthropic-spend"],
        "s3_patterns": ["cost-anomaly", "billing", "spend-tracker"],
        "rule_patterns": ["cost", "billing", "spend"],
    },
    "3_macro_calendar": {
        "lambda_patterns": ["macro-event", "fomc-schedule", "event-calendar", "macro-calendar", "release-calendar"],
        "s3_patterns": ["macro-events", "fomc-calendar", "release-calendar", "macro-calendar"],
        "rule_patterns": ["macro-event", "fomc-schedule"],
    },
    "4_fed_nlp": {
        "lambda_patterns": ["fed-nlp", "fomc-nlp", "fed-speech", "fed-language", "powell-nlp", "hawkish", "dovish", "fed-comms"],
        "s3_patterns": ["fed-nlp", "fomc-nlp", "fed-speech", "fed-comms", "fed-language"],
        "rule_patterns": ["fed-nlp", "fomc-nlp", "fed-speech"],
    },
    "5_black_swan_stress": {
        "lambda_patterns": ["stress-test", "black-swan", "blackswan", "scenario-replay", "historical-replay", "scenario-stress"],
        "s3_patterns": ["stress-test", "black-swan", "scenario-replay", "scenarios"],
        "rule_patterns": ["stress-test", "black-swan"],
    },
    "6_news_wire": {
        "lambda_patterns": ["news-wire", "newswire", "news-feed", "headline-feed", "headlines-watcher", "news-impact"],
        "s3_patterns": ["news-wire", "newswire", "headlines", "news-impact"],
        "rule_patterns": ["news-wire", "newswire"],
    },
    "7_trade_journal_active": {
        "lambda_patterns": ["trade-journal", "decision-capture", "decision-log", "trade-postmortem", "post-mortem", "decision-quality"],
        "s3_patterns": ["trade-journal", "decision-log", "post-mortem"],
        "rule_patterns": ["trade-journal", "post-mortem", "decision-capture"],
    },
    "8_capital_markets_cal": {
        "lambda_patterns": ["ipo-calendar", "split-calendar", "spinoff", "spin-off", "indexing-event", "sp500-changes", "corporate-action"],
        "s3_patterns": ["ipo-calendar", "splits", "spinoffs", "indexing-events", "corporate-actions"],
        "rule_patterns": ["ipo", "split", "spinoff", "indexing"],
    },
    "9_concentration_liquidity": {
        "lambda_patterns": ["concentration", "position-liquidity", "exit-time", "adv-exposure", "size-decay", "liquidity-risk", "position-risk"],
        "s3_patterns": ["concentration", "position-liquidity", "liquidity-risk"],
        "rule_patterns": ["concentration", "liquidity-risk"],
    },
    "10_whisper_numbers": {
        "lambda_patterns": ["whisper", "estimize", "buy-side-estimate", "consensus-vs", "whispers"],
        "s3_patterns": ["whisper", "estimize", "buy-side"],
        "rule_patterns": ["whisper", "estimize"],
    },
}

# List all Lambdas
all_lambdas = []
for page in lam.get_paginator('list_functions').paginate():
    all_lambdas.extend([fn['FunctionName'] for fn in page['Functions']])

# List S3 keys
all_keys = []
for page in s3.get_paginator('list_objects_v2').paginate(Bucket='justhodl-dashboard-live', Prefix='data/'):
    for obj in page.get('Contents', []):
        all_keys.append(obj['Key'])
# Also check 'backup/', 'snapshot/', 'disaster/' prefixes outside data/
for prefix in ('backup/', 'snapshot/', 'disaster/', 'archive/'):
    for page in s3.get_paginator('list_objects_v2').paginate(Bucket='justhodl-dashboard-live', Prefix=prefix):
        for obj in page.get('Contents', []):
            all_keys.append(obj['Key'])

# List EB rules
all_rules = []
for page in events.get_paginator('list_rules').paginate():
    all_rules.extend([r['Name'] for r in page['Rules']])

# DDB tables (for trade journal etc.)
ddb_tables = []
try:
    pg = ddb.get_paginator('list_tables')
    for p in pg.paginate():
        ddb_tables.extend(p.get('TableNames', []))
except Exception as e:
    ddb_tables = [f"err:{e}"]

# Check S3 cross-region replication
crr_status = None
try:
    r = s3.get_bucket_replication(Bucket='justhodl-dashboard-live')
    crr_status = r.get('ReplicationConfiguration', {})
except s3.exceptions.ClientError as e:
    crr_status = {'configured': False, 'error': str(e)[:100]}

# Check S3 versioning
versioning = None
try:
    v = s3.get_bucket_versioning(Bucket='justhodl-dashboard-live')
    versioning = v.get('Status', 'NOT_ENABLED')
except Exception as e:
    versioning = f"err:{e}"

# Check DDB PITR for each table
ddb_pitr = {}
for table in ddb_tables:
    if isinstance(table, str) and not table.startswith('err:'):
        try:
            r = ddb.describe_continuous_backups(TableName=table)
            ddb_pitr[table] = r['ContinuousBackupsDescription']['PointInTimeRecoveryDescription']['PointInTimeRecoveryStatus']
        except Exception:
            ddb_pitr[table] = 'UNKNOWN'

# Match candidates
def lc(s): return s.lower()
ll_lc = [lc(x) for x in all_lambdas]
ak_lc = [lc(x) for x in all_keys]
ar_lc = [lc(x) for x in all_rules]

results = {}
for idea, p in CANDIDATES.items():
    lh = sorted(set([all_lambdas[i] for i in range(len(all_lambdas))
                     for kw in p['lambda_patterns'] if lc(kw) in ll_lc[i]]))
    sh = sorted(set([all_keys[i] for i in range(len(all_keys))
                     for kw in p['s3_patterns'] if lc(kw) in ak_lc[i]]))
    rh = sorted(set([all_rules[i] for i in range(len(all_rules))
                     for kw in p['rule_patterns'] if lc(kw) in ar_lc[i]]))
    if not lh and not sh and not rh:
        v = "BUILD"
    elif lh and (sh or rh):
        v = "FULL_IMPL_EXISTS"
    else:
        v = "PARTIAL"
    results[idea] = {
        'verdict': v,
        'lambda_hits': lh[:8],
        's3_hits': sh[:8],
        'rule_hits': rh[:8],
    }

# Special inspection: portfolio Lambda (for stress test feasibility)
portfolio_state = {}
for table in ['justhodl-portfolio', 'justhodl-signals', 'justhodl-outcomes', 'justhodl-alert-actions']:
    try:
        d = ddb.describe_table(TableName=table)
        portfolio_state[table] = {
            'item_count': d['Table'].get('ItemCount'),
            'size_bytes': d['Table'].get('TableSizeBytes'),
        }
    except Exception as e:
        portfolio_state[table] = {'error': str(e)[:100]}

report = {
    'started_at': datetime.now(timezone.utc).isoformat(),
    'n_lambdas': len(all_lambdas),
    'n_s3_keys': len(all_keys),
    'n_eb_rules': len(all_rules),
    'n_ddb_tables': len([t for t in ddb_tables if isinstance(t, str) and not t.startswith('err')]),
    'ddb_tables': ddb_tables,
    'ddb_pitr_status': ddb_pitr,
    'bucket_crr': crr_status,
    'bucket_versioning': versioning,
    'portfolio_state': portfolio_state,
    'candidates': results,
}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1067.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str)[:6000])
