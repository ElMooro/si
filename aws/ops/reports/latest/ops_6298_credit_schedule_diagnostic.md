
**Status:** success  
**Duration:** 4.0s  
**Finished:** 2026-09-28T17:04:59+00:00  

## Data

| actual_runtime | baseline_accepted | credential_reads | declared_schedule | history_writes | licensed_original_reads | native_invocations | private_account_reads | provider_requests | public_writes | retained_originals_replayed | schedule_changes | scope | strict_baseline_matches | strict_baseline_refusal |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'code_sha256': 's1FMKIb7M5KpzfU1dqn/4O8l3K+B6TshMI5nG7d7OPw=', 'source_files_checked': 9, 'handler_bytes': 1684, 'timeout': 300, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '9cb4a62b116865bbf9695493bcff820c87da9277'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-credit-stress-cadence', 'state': 'ENABLED', 'expression': 'cron(10 22 ? * MON-FRI *)', 'native_targets': 1}, {'kind': 'EventBridge Scheduler', 'name': 'credit-stress-sched', 'state': 'ENABLED', 'expression': 'cron(0 20 ? * MON-FRI *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-credit-stress', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | False | 0 | cron(10 22 ? * MON-FRI *) | 0 | 0 | 0 | 0 | 0 | 0 | False | 0 | Runtime/schedule observation only. The strict acceptance requirement remains unchanged. | False | One original weekday 22:10 UTC schedule required |

## Log

