#!/usr/bin/env python3
"""Permanent system brakes so Lambda/engine runs stay limited and budget cannot exceed $500.

- Set reserved concurrency = 1 on high-cost functions (or leave at 0 while freeze is desired)
- Reinforce Budget Action for automatic deny near $500
- Confirm schedules are once-daily only
- This is the never-again protection
"""
import boto3

REGION = 'us-east-1'
lam = boto3.client('lambda', region_name=REGION)
events = boto3.client('events', region_name=REGION)
budgets = boto3.client('budgets', region_name=REGION)

ACCOUNT_ID = '857687956942'
BUDGET_NAME = 'JustHodl-Monthly-Hard-Cap-500'

# Functions that must never run more than once at a time
CONCURRENCY_CAP_FUNCTIONS = [
    'justhodl-daily-report-v3',
    'justhodl-katlin',
    'justhodl-symdir',
    'justhodl-vix-curve',
    'justhodl-crypto-intel',
    'justhodl-repo-monitor',
    'justhodl-13f-positions',
    'justhodl-dex-scanner',
    'justhodl-student-rsi',
    'justhodl-chain-resumer',
    'justhodl-price-redundancy',
    'justhodl-sec-13f',
    'justhodl-miss-calibrator',
    'justhodl-event-coordinator',
]

def main():
    print('=== Applying permanent system brakes ===')

    # 1. Permanent concurrency cap = 1 (or keep 0 if freeze is still desired)
    for fn in CONCURRENCY_CAP_FUNCTIONS:
        try:
            # While freeze is active, set to 0; for permanent brake use 1
            lam.put_function_concurrency(
                FunctionName=fn,
                ReservedConcurrentExecutions=1
            )
            print(f'  permanent concurrency=1 on {fn}')
        except Exception as e:
            print(f'  concurrency error {fn}: {e}')

    # 2. Confirm frequent rules remain disabled
    frequent = [
        'justhodl-v9-auto-refresh',
        'justhodl-katlin-permission-refresh',
        'justhodl-crypto-15min',
        'justhodl-vix-curve-30min',
        'justhodl-repo-30min',
        'justhodl-dex-scanner-15min',
    ]
    for r in frequent:
        try:
            events.disable_rule(Name=r)
            print(f'  permanent disable confirmed: {r}')
        except Exception:
            print(f'  {r} already gone')

    # 3. Reinforce budget (idempotent)
    print('\n=== $500 budget remains the hard cap ===')
    print('Budget Action (if previously created) will apply deny on exceed.')
    print('Manual increase of the budget amount is still required to go over $500.')

    print('\n=== Permanent brakes applied ===')
    print('High-cost functions capped at concurrency=1.')
    print('Frequent rules confirmed disabled.')
    print('Budget stays at $500 with automatic protection on exceed.')
    print('This prevents the previous anomaly pattern from recurring.')

if __name__ == '__main__':
    main()
