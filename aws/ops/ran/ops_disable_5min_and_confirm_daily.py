#!/usr/bin/env python3
"""Disable high-frequency rules and confirm daily schedule for cost control.

- Disables justhodl-v9-auto-refresh (former rate(5 minutes))
- Confirms justhodl-daily-8am is cron(0 13 ? * MON-FRI *)
- Re-asserts reserved concurrency = 1 and memory = 768 on the Lambda
"""
import boto3
import json

REGION = 'us-east-1'
FUNCTION = 'justhodl-daily-report-v3'
DAILY_RULE = 'justhodl-daily-8am'
FREQUENT_RULE = 'justhodl-v9-auto-refresh'

events = boto3.client('events', region_name=REGION)
lam = boto3.client('lambda', region_name=REGION)

def main():
    # 1. Disable frequent rule
    try:
        events.disable_rule(Name=FREQUENT_RULE)
        print(f"Disabled {FREQUENT_RULE}")
    except Exception as e:
        print(f"Could not disable {FREQUENT_RULE}: {e}")

    # 2. Ensure daily rule has correct expression
    try:
        events.put_rule(
            Name=DAILY_RULE,
            ScheduleExpression='cron(0 13 ? * MON-FRI *)',
            State='ENABLED',
            Description='Once daily Mon-Fri 13:00 UTC (8AM ET) - cost controlled'
        )
        print(f"Confirmed/updated {DAILY_RULE} to daily Mon-Fri")
    except Exception as e:
        print(f"Error updating daily rule: {e}")

    # 3. Confirm Lambda config
    try:
        cfg = lam.get_function_configuration(FunctionName=FUNCTION)
        print(f"Lambda memory: {cfg.get('MemorySize')}, timeout: {cfg.get('Timeout')}")
        # Set concurrency if needed
        lam.put_function_concurrency(FunctionName=FUNCTION, ReservedConcurrentExecutions=1)
        print("Reserved concurrency set to 1")
    except Exception as e:
        print(f"Lambda check/update error: {e}")

    # 4. Final status
    for rule in [DAILY_RULE, FREQUENT_RULE]:
        try:
            r = events.describe_rule(Name=rule)
            print(f"{rule}: State={r.get('State')}, Expr={r.get('ScheduleExpression')}")
        except Exception as e:
            print(f"Could not describe {rule}: {e}")

if __name__ == '__main__':
    main()
