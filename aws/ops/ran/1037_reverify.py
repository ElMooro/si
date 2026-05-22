#!/usr/bin/env python3
"""ops 1037 — final re-verify after real portfolio-catalysts fix."""
import json, boto3, os, base64
from datetime import datetime, timezone

REGION = 'us-east-1'
NOW = datetime.now(timezone.utc)
lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)

report = {'started_at': NOW.isoformat()}

# Re-invoke portfolio-catalysts
print("[1] Invoking justhodl-portfolio-catalysts after REAL fix...")
try:
    r = lam.invoke(FunctionName='justhodl-portfolio-catalysts',
                   InvocationType='RequestResponse', LogType='Tail')
    payload = r['Payload'].read().decode('utf-8', errors='replace')
    report['portfolio_catalysts'] = {
        'status': r['StatusCode'],
        'function_error': r.get('FunctionError', 'none'),
        'response_head': payload[:400],
    }
    if r.get('LogResult'):
        log_tail = base64.b64decode(r['LogResult']).decode('utf-8', errors='replace')
        report['portfolio_catalysts']['log_has_KeyError'] = "KeyError: 'T-0'" in log_tail
        report['portfolio_catalysts']['log_tail_400'] = log_tail[-400:]
except Exception as e:
    report['portfolio_catalysts'] = {'error': str(e)[:300]}

# Re-confirm forced-selling-bounce (should still be fine)
print("[2] Re-invoking justhodl-forced-selling-bounce...")
try:
    r = lam.invoke(FunctionName='justhodl-forced-selling-bounce',
                   InvocationType='RequestResponse', LogType='Tail')
    payload = r['Payload'].read().decode('utf-8', errors='replace')
    report['forced_selling_bounce'] = {
        'status': r['StatusCode'],
        'function_error': r.get('FunctionError', 'none'),
        'response_head': payload[:400],
    }
    if r.get('LogResult'):
        log_tail = base64.b64decode(r['LogResult']).decode('utf-8', errors='replace')
        report['forced_selling_bounce']['log_has_TypeError'] = (
            "'<' not supported between instances of 'dict' and 'int'" in log_tail)
except Exception as e:
    report['forced_selling_bounce'] = {'error': str(e)[:300]}

# Reconfirm rules are still disabled
for rule_name in ['autonomous-ai-schedule', 'justhodl-8am']:
    try:
        r = events.describe_rule(Name=rule_name)
        report[f'rule_{rule_name}_state'] = r.get('State')
    except Exception as e:
        report[f'rule_{rule_name}_state'] = f'error: {str(e)[:100]}'

# Summary
report['summary'] = {
    'portfolio_catalysts_fixed': (
        report.get('portfolio_catalysts', {}).get('function_error') == 'none'
        and not report.get('portfolio_catalysts', {}).get('log_has_KeyError', True)
    ),
    'forced_selling_bounce_clean': (
        report.get('forced_selling_bounce', {}).get('function_error') == 'none'
        and not report.get('forced_selling_bounce', {}).get('log_has_TypeError', True)
    ),
    'rules_still_disabled': all(
        report.get(f'rule_{r}_state') == 'DISABLED'
        for r in ('autonomous-ai-schedule', 'justhodl-8am')
    ),
}

os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1037.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print("\n=== OPS 1037 SUMMARY ===")
print(json.dumps(report['summary'], indent=2))
print()
print("=== portfolio-catalysts response ===")
print(json.dumps(report['portfolio_catalysts'], indent=2, default=str)[:1200])
