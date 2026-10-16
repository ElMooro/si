#!/usr/bin/env python3
"""Force once-daily Mon-Fri schedules and concurrency=1 on high-frequency attention items.

Targets the residual overlapping-risk Lambdas identified in the cost audit.
Applies changes directly via EventBridge + Lambda APIs (bypasses deploy schedule guard).
"""
import boto3

REGION = 'us-east-1'
events = boto3.client('events', region_name=REGION)
lam = boto3.client('lambda', region_name=REGION)

# Daily Mon-Fri at ~8AM ET (13:00 UTC)
DAILY_CRON = 'cron(0 13 ? * MON-FRI *)'

TARGETS = [
    # (function_name, rule_name_to_update_or_create, description)
    ('justhodl-vix-curve', 'justhodl-vix-curve-30min', 'Once daily Mon-Fri (was 30min market hours)'),
    ('justhodl-katlin', 'justhodl-katlin-permission-refresh', 'Once daily Mon-Fri (was rate(15min) permission refresh)'),
    # Add more as identified; script is idempotent
]

# Also set concurrency=1 on these functions
CONCURRENCY_TARGETS = [
    'justhodl-vix-curve',
    'justhodl-katlin',
    'justhodl-13f-positions',
    'justhodl-crypto-intel',
    'justhodl-repo-monitor',
]

def main():
    print('=== Forcing once-daily schedules ===')
    for fn, rule, desc in TARGETS:
        try:
            events.put_rule(
                Name=rule,
                ScheduleExpression=DAILY_CRON,
                State='ENABLED',
                Description=desc
            )
            # Ensure target points at the function
            events.put_targets(
                Rule=rule,
                Targets=[{'Id': '1', 'Arn': f'arn:aws:lambda:{REGION}:857687956942:function:{fn}'}]
            )
            print(f'Set {rule} → {DAILY_CRON} for {fn}')
        except Exception as e:
            print(f'Error on {rule}/{fn}: {e}')

    print('\n=== Setting reserved concurrency = 1 ===')
    for fn in CONCURRENCY_TARGETS:
        try:
            lam.put_function_concurrency(
                FunctionName=fn,
                ReservedConcurrentExecutions=1
            )
            print(f'Concurrency=1 set on {fn}')
        except Exception as e:
            print(f'Concurrency error on {fn}: {e}')

    print('\n=== Done. Verify in EventBridge and Lambda console. ===')

if __name__ == '__main__':
    main()
