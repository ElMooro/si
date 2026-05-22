#!/usr/bin/env python3
"""
ops 1035 — DISABLE 2 zombie EB rules (audit P0 items 1 & 2)

Audit ops 1029-1033 verified:
  - autonomous-ai-processor: rate(5min) ENABLED rule, 2016 FailedInvocations/7d
    Root cause: resource policy only allows lambda:InvokeFunctionUrl,
    missing events.amazonaws.com InvokeFunction. Lambda has been dead 226 days.
  - justhodl-email-reports (v1): cron(0 13 * * ?) ENABLED rule, 7 FailedInvocations/7d
    Root cause: NO resource policy at all. v2 is the active version.

Safest action: DISABLE the rules (don't delete). Stops the 2,023
failed invocations per week and the corresponding CloudWatch noise.
The Lambdas + rules remain in AWS for Khalid to delete after review.

This is reversible — re-enable via:
  aws events enable-rule --name <RuleName>
"""
import json, boto3, os
from datetime import datetime, timezone, timedelta

REGION = 'us-east-1'
NOW = datetime.now(timezone.utc)
events = boto3.client('events', region_name=REGION)
cw = boto3.client('cloudwatch', region_name=REGION)

report = {'started_at': NOW.isoformat(), 'actions': []}

ZOMBIE_RULES = [
    ('autonomous-ai-schedule', 'autonomous-ai-processor'),
    ('justhodl-8am',           'justhodl-email-reports'),
]

for rule_name, lambda_name in ZOMBIE_RULES:
    action = {'rule': rule_name, 'lambda': lambda_name}
    try:
        # Capture before-state for audit trail
        before = events.describe_rule(Name=rule_name)
        action['before'] = {
            'state': before.get('State'),
            'expression': before.get('ScheduleExpression'),
            'description': before.get('Description'),
        }
        # Get FailedInvocations 7d to confirm we are doing the right thing
        end = NOW; start = end - timedelta(days=7)
        try:
            m = cw.get_metric_statistics(
                Namespace='AWS/Events', MetricName='FailedInvocations',
                Dimensions=[{'Name': 'RuleName', 'Value': rule_name}],
                StartTime=start, EndTime=end, Period=86400, Statistics=['Sum'])
            action['failed_invocations_7d'] = int(sum(p['Sum'] for p in m['Datapoints']))
        except Exception as e:
            action['cw_metric_error'] = str(e)[:200]
        
        # Disable the rule
        if before.get('State') == 'ENABLED':
            events.disable_rule(Name=rule_name)
            after = events.describe_rule(Name=rule_name)
            action['after_state'] = after.get('State')
            action['result'] = 'DISABLED'
        else:
            action['result'] = f'already {before.get("State")}; no change'
    except events.exceptions.ResourceNotFoundException:
        action['result'] = 'rule not found'
    except Exception as e:
        action['result'] = f'error: {str(e)[:200]}'
    
    report['actions'].append(action)

# Write report
os.makedirs('aws/ops/reports', exist_ok=True)
with open('aws/ops/reports/1035.json', 'w') as f:
    json.dump(report, f, indent=2, default=str)

# Console summary
print("="*60)
print("OPS 1035 — Zombie EB rule disable")
print("="*60)
for a in report['actions']:
    print(f"\n  Rule: {a['rule']}  (Lambda: {a['lambda']})")
    print(f"    Failed invocations 7d: {a.get('failed_invocations_7d', 'n/a')}")
    print(f"    Before state: {a.get('before', {}).get('state')}")
    print(f"    Result: {a.get('result')}")

print(f"\nReport: aws/ops/reports/1035.json")
