
**Status:** success  
**Duration:** 3.8s  
**Finished:** 2026-09-28T05:11:49+00:00  

## Data

| acceptance | account_reads | actual_package | consumer_output_reads | expected_commit | learning_log_reads | model_requests | native_invocations | private_context_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'status': 'exact_package_and_original_schedule_verified', 'synthetic_brief_context_verified': True, 'native_output_verified': False, 'downstream_decisions_qualified': False, 'investment_authority': False} | 0 | {'code_sha256': 'wMIUOPmjmtVMvygjxzJQem9hfwHeUDdU7n6Y0+xRUKo=', 'source_files_checked': 3, 'handler_bytes': 39748, 'timeout': 300, 'memory_mb': 768, 'receipt': {'status': 'matched', 'commit': 'fab95fae1a8edad07e9b01c85d7d094af2f1a35f'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-pump-radar-brief-daily', 'state': 'ENABLED', 'expression': 'cron(30 13 * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-pump-radar-brief', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | fab95fae1a8edad07e9b01c85d7d094af2f1a35f | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Exact three-source package and original runtime/schedule only; synthetic source and compatibility tests. No native output or portfolio qualification. |

## Log

