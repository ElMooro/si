#!/usr/bin/env python3
"""ops 1054 — comprehensive verify of all 8 exponential Lambdas."""
import json, boto3, os
from datetime import datetime, timezone

lam = boto3.client('lambda', region_name='us-east-1')
events = boto3.client('events', region_name='us-east-1')
ddb = boto3.client('dynamodb', region_name='us-east-1')

LAMBDAS = [
    ("justhodl-signal-halflife", "signal-halflife-weekly", "data/signal-halflife.json", "#2"),
    ("justhodl-premortem-engine", "premortem-engine-daily", "data/kill-theses.json", "#1"),
    ("justhodl-behavior-mirror", "behavior-mirror-weekly", "data/behavior-mirror.json", "#4"),
    ("justhodl-failure-library", "failure-library-daily", "data/pre-disaster-watchlist.json", "#5"),
    ("justhodl-causality-scanner", "causality-scanner-weekly", "data/causality-discoveries.json", "#3"),
    ("justhodl-convexity-scorer", "convexity-scorer-daily", "data/convexity-scores.json", "#7"),
    ("justhodl-chart-vision", "chart-vision-daily", "data/chart-vision.json", "#6"),
    ("justhodl-meta-improver", "meta-improver-weekly", "data/meta-improver-state.json", "#8"),
]

report = {
    'started_at': datetime.now(timezone.utc).isoformat(),
    'summary': {'lambdas_deployed': 0, 'lambdas_missing': 0,
                'eb_rules_active': 0, 'eb_rules_missing': 0,
                'dlq_attached': 0, 'xray_enabled': 0},
    'lambdas': [],
}

s3 = boto3.client('s3', region_name='us-east-1')

for fn_name, rule_name, output_key, idea_id in LAMBDAS:
    entry = {'idea': idea_id, 'fn_name': fn_name}
    # Lambda check
    try:
        cfg = lam.get_function_configuration(FunctionName=fn_name)
        entry['lambda'] = {
            'exists': True,
            'last_modified': cfg.get('LastModified'),
            'memory': cfg.get('MemorySize'),
            'timeout': cfg.get('Timeout'),
            'dlq_attached': bool((cfg.get('DeadLetterConfig') or {}).get('TargetArn')),
            'xray': cfg.get('TracingConfig', {}).get('Mode') == 'Active',
            'env_keys': sorted((cfg.get('Environment',{}) or {}).get('Variables', {}).keys()),
        }
        report['summary']['lambdas_deployed'] += 1
        if entry['lambda']['dlq_attached']:
            report['summary']['dlq_attached'] += 1
        if entry['lambda']['xray']:
            report['summary']['xray_enabled'] += 1
    except lam.exceptions.ResourceNotFoundException:
        entry['lambda'] = {'exists': False}
        report['summary']['lambdas_missing'] += 1

    # EventBridge rule check
    try:
        rd = events.describe_rule(Name=rule_name)
        entry['eb_rule'] = {
            'state': rd.get('State'),
            'cron': rd.get('ScheduleExpression'),
        }
        if rd.get('State') == 'ENABLED':
            report['summary']['eb_rules_active'] += 1
    except events.exceptions.ResourceNotFoundException:
        entry['eb_rule'] = {'state': 'NOT_FOUND'}
        report['summary']['eb_rules_missing'] += 1

    # S3 output check (some haven't run yet)
    try:
        h = s3.head_object(Bucket='justhodl-dashboard-live', Key=output_key)
        entry['s3_output'] = {
            'exists': True,
            'size': h['ContentLength'],
            'last_modified': str(h['LastModified']),
        }
    except Exception:
        entry['s3_output'] = {'exists': False, 'note': 'not_yet_invoked'}

    report['lambdas'].append(entry)

# DDB tables created for #4
try:
    desc = ddb.describe_table(TableName='justhodl-alert-actions')
    report['ddb_alert_actions'] = {
        'exists': True,
        'status': desc['Table']['TableStatus'],
        'billing': desc['Table'].get('BillingModeSummary', {}).get('BillingMode'),
    }
except Exception:
    report['ddb_alert_actions'] = {'exists': False}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1054.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)
print(json.dumps(report, indent=2, default=str))
print()
print(f"=== Summary: {report['summary']['lambdas_deployed']}/8 Lambdas deployed, "
      f"{report['summary']['eb_rules_active']}/8 EB rules active, "
      f"DLQ={report['summary']['dlq_attached']}/8, X-Ray={report['summary']['xray_enabled']}/8 ===")
