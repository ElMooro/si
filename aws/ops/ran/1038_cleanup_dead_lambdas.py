#!/usr/bin/env python3
"""
ops 1038 — CLEANUP DEAD LAMBDAS + VERIFY NOBRAINER

Deletes confirmed-dead Lambdas + their EB rules:
  Truly orphan (no triggers, 0 invocations):
    - justhodl-cdn-diag-temp
    - justhodl-tmp-433
    - justhodl-tmp-454
    - justhodl-tmp-458
    - justhodl-tmp-force-refresh
  
  Retired (rules DISABLED, dead Lambda code):
    - macro-report-api  (rule macro-api-daily-update)
    - justhodl-daily-macro-report  (rule DailyMacroReportRule)
  
  Already disabled but Lambda kept for now:
    - autonomous-ai-processor  (rule disabled, but Lambda has function URL)
    - justhodl-email-reports v1  (rule disabled, but v2 may reference v1's S3 data)

Also live-invokes nobrainer-rationale to confirm the fix is working.

Writes: aws/ops/reports/1038.json
"""
import json, boto3, base64
from datetime import datetime, timezone

REGION = 'us-east-1'
NOW = datetime.now(timezone.utc)
lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)

# Targets to delete
DELETE_LAMBDAS = [
    'justhodl-cdn-diag-temp',
    'justhodl-tmp-433',
    'justhodl-tmp-454',
    'justhodl-tmp-458',
    'justhodl-tmp-force-refresh',
    'macro-report-api',
    'justhodl-daily-macro-report',
]
DELETE_RULES = [
    'macro-api-daily-update',
    'DailyMacroReportRule',
]

report = {'started_at': NOW.isoformat(), 'actions': []}

# ---- A. Live verify nobrainer-rationale ----
print("[A] Live-verify nobrainer-rationale...")
try:
    inv = lam.invoke(
        FunctionName='justhodl-nobrainer-rationale',
        InvocationType='RequestResponse',
        Payload=b'{}',
    )
    payload = inv['Payload'].read().decode()
    report['nobrainer_invoke'] = {
        'status': inv['StatusCode'],
        'function_error': inv.get('FunctionError', 'none'),
        'response_head': payload[:500],
    }
    print(f"  status={inv['StatusCode']}  function_error={inv.get('FunctionError','none')}")
except Exception as e:
    report['nobrainer_invoke'] = {'error': str(e)[:300]}
    print(f"  ERROR: {e}")

# ---- B. Delete EB rules ----
print("[B] Deleting EB rules...")
for rule in DELETE_RULES:
    action = {'type': 'delete_rule', 'rule': rule}
    try:
        rd = events.describe_rule(Name=rule)
        action['before_state'] = rd.get('State')
        # Remove all targets first (required before deleting rule)
        try:
            targets = events.list_targets_by_rule(Rule=rule).get('Targets', [])
            if targets:
                target_ids = [t['Id'] for t in targets]
                events.remove_targets(Rule=rule, Ids=target_ids)
                action['removed_target_ids'] = target_ids
        except Exception as e:
            action['target_removal_error'] = str(e)[:200]
        events.delete_rule(Name=rule)
        action['result'] = 'DELETED'
    except events.exceptions.ResourceNotFoundException:
        action['result'] = 'NOT_FOUND (already deleted)'
    except Exception as e:
        action['error'] = str(e)[:200]
    report['actions'].append(action)

# ---- C. Delete Lambdas ----
print("[C] Deleting Lambdas...")
for lname in DELETE_LAMBDAS:
    action = {'type': 'delete_lambda', 'lambda': lname}
    try:
        cfg = lam.get_function_configuration(FunctionName=lname)
        action['before'] = {
            'last_modified': cfg.get('LastModified'),
            'code_sha': cfg.get('CodeSha256', '')[:12],
            'runtime': cfg.get('Runtime'),
        }
        # Delete function URL if exists (precaution)
        try:
            lam.delete_function_url_config(FunctionName=lname)
            action['url_deleted'] = True
        except lam.exceptions.ResourceNotFoundException:
            pass
        # Delete the function
        lam.delete_function(FunctionName=lname)
        action['result'] = 'DELETED'
    except lam.exceptions.ResourceNotFoundException:
        action['result'] = 'NOT_FOUND (already deleted)'
    except Exception as e:
        action['error'] = str(e)[:200]
    report['actions'].append(action)

# Summary
report['summary'] = {
    'lambdas_deleted': sum(1 for a in report['actions'] if a.get('type')=='delete_lambda' and a.get('result')=='DELETED'),
    'rules_deleted': sum(1 for a in report['actions'] if a.get('type')=='delete_rule' and a.get('result')=='DELETED'),
    'lambdas_not_found': sum(1 for a in report['actions'] if a.get('type')=='delete_lambda' and 'NOT_FOUND' in a.get('result','')),
    'rules_not_found': sum(1 for a in report['actions'] if a.get('type')=='delete_rule' and 'NOT_FOUND' in a.get('result','')),
    'nobrainer_healthy': report.get('nobrainer_invoke', {}).get('function_error') == 'none',
}

import os
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1038.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

print(f"\n=== SUMMARY ===")
print(json.dumps(report['summary'], indent=2))
