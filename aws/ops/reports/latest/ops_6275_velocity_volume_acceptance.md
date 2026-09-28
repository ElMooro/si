
**Status:** success  
**Duration:** 6.0s  
**Finished:** 2026-09-28T07:19:19+00:00  

## Data

| acceptance | account_reads | actual_package | consumer_output_reads | expected_commit | learning_log_reads | model_requests | native_invocations | private_context_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'status': 'exact_package_and_original_schedule_verified', 'native_output_verified': False, 'consumer_decisions_qualified': False, 'investment_authority': False} | 0 | {'code_sha256': 'T8wboVW1nwbzjTh5Z0nFceo5JJUknQa6xWMuOSaYAK4=', 'source_files_checked': 6, 'handler_bytes': 42658, 'timeout': 300, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '56dbedef8d805be2a3c7a36b95abb4ca0cab8fa2'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-velocity-acceleration-hourly', 'state': 'ENABLED', 'expression': 'cron(30 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-velocity-acceleration', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 56dbedef8d805be2a3c7a36b95abb4ca0cab8fa2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Complete Velocity package, original runtime/schedules and synthetic replay; no private or downstream consumer-output qualification. |

## Log

