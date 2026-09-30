
**Status:** failure  
**Duration:** 2.5s  
**Finished:** 2026-09-30T05:57:50+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6359_portfolio_accounting_runtime_acceptance.py", line 52, in main
    validate(before,expected,baseline[function] if baseline else None)
  File "/home/runner/work/si/si/aws/ops/staged/ops_6359_portfolio_accounting_runtime_acceptance.py", line 27, in validate
    if function not in FUNCTIONS or actual.get('receipt',{}).get('status')!='matched':raise ValueError('Exact reviewed native function and receipt required')
                                                                                      ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ValueError: Exact reviewed native function and receipt required

```

## Data

| actual_runtime | expected_commit | function |
|---|---|---|
| {'code_sha256': 'WkXXjoo1LN8v71JfiNmMgNs9XofEKASafYauC584FhY=', 'source_files_checked': 3, 'handler_bytes': 36196, 'timeout': 180, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '99b780fbbaae0a6a6b6c1a7a16049f7884788b5f'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-portfolio-snapshot-hourly', 'state': 'ENABLED', 'expression': 'cron(40 * * * ? *)', 'native_targets': 1, 'qualified_targets': ['arn:aws:lambda:us-east-1:857687956942:function:justhodl-portfolio-snapshot:live']}, {'kind': 'EventBridge Scheduler', 'name': 'portfolio-snapshot-sched', 'state': 'ENABLED', 'expression': 'cron(40 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default', 'target_qualifier': 'live'}], 'function_name': 'justhodl-portfolio-snapshot', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'active_alias': {'alias': 'live', 'version': '6', 'code_sha256': 'WkXXjoo1LN8v71JfiNmMgNs9XofEKASafYauC584FhY='}} | None | justhodl-portfolio-snapshot |
| {'code_sha256': 'tA2TVIR0t+z4pwkA7wkE0VEw/3gFFSXR2LCj26LrOdI=', 'source_files_checked': 1, 'handler_bytes': 15490, 'timeout': 30, 'memory_mb': 256, 'receipt': {'status': 'missing_predecessor_receipt'}, 'schedules': [], 'function_name': 'justhodl-portfolio-admin', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | None | justhodl-portfolio-admin |

## Log

