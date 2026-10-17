#!/usr/bin/env python3
"""EMERGENCY HALT for Cost Anomaly.

Disables non-essential EventBridge rules and sets reserved concurrency=0
on high-memory / high-impact Lambdas to stop further GB-Second and S3 accrual.
System remains halted until manually restored.
"""
import boto3

REGION = 'us-east-1'
events = boto3.client('events', region_name=REGION)
lam = boto3.client('lambda', region_name=REGION)

# High-cost functions to fully halt (concurrency=0)
HALT_FUNCTIONS = [
    'justhodl-daily-report-v3',
    'justhodl-katlin',
    'justhodl-symdir',
    'justhodl-13f-positions',
    'justhodl-crypto-intel',
    'justhodl-vix-curve',
    'justhodl-repo-monitor',
    'justhodl-dex-scanner',
    'justhodl-sec-8k',
    'justhodl-tv-bars',
]

# Rules to disable (frequent or high-impact)
DISABLE_RULES = [
    'justhodl-daily-8am',
    'justhodl-vix-curve-30min',
    'justhodl-katlin-permission-refresh',
    'justhodl-crypto-15min',
    'justhodl-repo-30min',
    'justhodl-dex-scanner-15min',
    'justhodl-sec-8k-30min',
    'justhodl-tv-bars-hourly',
]

def main():
    print('=== EMERGENCY HALT: stopping high-cost activity ===')

    print('\n--- Disabling EventBridge rules ---')
    for rule in DISABLE_RULES:
        try:
            events.disable_rule(Name=rule)
            print(f'Disabled rule: {rule}')
        except Exception as e:
            print(f'Rule {rule}: {e}')

    print('\n--- Setting concurrency=0 on high-cost Lambdas ---')
    for fn in HALT_FUNCTIONS:
        try:
            lam.put_function_concurrency(
                FunctionName=fn,
                ReservedConcurrentExecutions=0
            )
            print(f'Concurrency=0 set on {fn} (halted)')
        except Exception as e:
            print(f'Concurrency error on {fn}: {e}')

    print('\n=== HALT COMPLETE. High-cost scheduled runs are stopped. ===')
    print('System is halted for investigation. Manually re-enable rules and raise concurrency after fixes.')

if __name__ == '__main__':
    main()
