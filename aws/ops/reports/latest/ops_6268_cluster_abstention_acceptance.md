
**Status:** success  
**Duration:** 1.1s  
**Finished:** 2026-09-28T04:23:23+00:00  

## Data

| acceptance | account_reads | actual_package | consumer_output_reads | expected_commit | learning_log_reads | native_invocations | private_state_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|
| {'status': 'exact_package_and_original_schedule_verified', 'synthetic_behaviour_verified': True, 'native_output_verified': False, 'consumer_qualification': False, 'investment_authority': False} | 0 | {'code_sha256': 'aA54O7M0qH4mBf2EZiNJQuzd6gqV2qraRHMqGyWl3m4=', 'source_files_checked': 2, 'handler_bytes': 25781, 'timeout': 180, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '0495e963e680f3e3e02fe359803a9c492c8ef922'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-catalyst-clusters-daily', 'state': 'ENABLED', 'expression': 'cron(15 14 * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-catalyst-clusters', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 0495e963e680f3e3e02fe359803a9c492c8ef922 | 0 | 0 | 0 | 0 | 0 | 0 | Exact complete package, original runtime/schedule and synthetic regression only. No native output or portfolio qualification. |

## Log

