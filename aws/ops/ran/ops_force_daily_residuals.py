#!/usr/bin/env python3
"""Force once-daily Mon-Fri + reserved_concurrency=1 on residual high-frequency Lambdas.

Covers items missed or partially covered in prior ops:
- justhodl-dex-scanner (15min)
- justhodl-student-rsi (1min if present)
- justhodl-chain-resumer (5min if present)
- justhodl-symdir frequent paths
- justhodl-price-redundancy (15min)
- Any other rate() residuals
"""
import boto3

REGION = 'us-east-1'
DAILY_CRON = 'cron(0 13 ? * MON-FRI *)'

events = boto3.client('events', region_name=REGION)
lam = boto3.client('lambda', region_name=REGION)
scheduler = boto3.client('scheduler', region_name=REGION)

# Residual frequent rules / functions to convert or disable
RESIDUAL_TARGETS = [
    # (function, frequent_rule_or_scheduler_name, is_scheduler)
    ('justhodl-dex-scanner', 'justhodl-dex-scanner-15min', False),
    ('justhodl-student-rsi', 'justhodl-student-rsi', False),  # adjust name if different
    ('justhodl-chain-resumer', 'justhodl-chain-resumer', False),
    ('justhodl-price-redundancy', 'justhodl-price-redundancy-15min', False),
    ('justhodl-symdir', 'justhodl-symdir-warm', True),  # example; script will try
]

CONCURRENCY_TARGETS = [
    'justhodl-dex-scanner',
    'justhodl-student-rsi',
    'justhodl-chain-resumer',
    'justhodl-price-redundancy',
    'justhodl-symdir',
    'justhodl-cboe-options-chain',
]

def main():
    print('=== Forcing residual high-frequency items to once daily ===')

    for fn, name, is_sched in RESIDUAL_TARGETS:
        try:
            if is_sched:
                scheduler.update_schedule(Name=name, State='DISABLED')
                print(f'Disabled scheduler {name} for {fn}')
            else:
                # Convert or disable the frequent rule and create/ensure daily
                try:
                    events.disable_rule(Name=name)
                    print(f'Disabled frequent rule {name}')
                except Exception:
                    pass
                daily_name = f'{fn}-daily'
                events.put_rule(
                    Name=daily_name,
                    ScheduleExpression=DAILY_CRON,
                    State='ENABLED',
                    Description=f'Once daily Mon-Fri for {fn} (residual cost control)'
                )
                try:
                    events.put_targets(
                        Rule=daily_name,
                        Targets=[{'Id': '1', 'Arn': f'arn:aws:lambda:{REGION}:857687956942:function:{fn}'}]
                    )
                except Exception:
                    pass
                print(f'Set daily {daily_name} for {fn}')
        except Exception as e:
            print(f'Error processing {fn}/{name}: {e}')

    print('\n=== Setting reserved concurrency = 1 on residuals ===')
    for fn in CONCURRENCY_TARGETS:
        try:
            lam.put_function_concurrency(FunctionName=fn, ReservedConcurrentExecutions=1)
            print(f'Concurrency=1 on {fn}')
        except Exception as e:
            print(f'Concurrency error {fn}: {e}')

    print('\n=== Residual force-daily complete. ===')

if __name__ == '__main__':
    main()
