
**Status:** success  
**Duration:** 2.6s  
**Finished:** 2026-09-28T04:49:22+00:00  

## Data

| acceptance | account_reads | actual_package | consumer_output_reads | expected_commit | learning_log_reads | model_requests | native_invocations | private_context_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'status': 'exact_package_and_original_schedule_verified', 'synthetic_source_context_verified': True, 'native_output_verified': False, 'downstream_decisions_qualified': False, 'investment_authority': False} | 0 | {'code_sha256': 'BPmSyNwlWQJz+4HLdYs6lQtlVhO0PfsS4mpTohTr8Fw=', 'source_files_checked': 2, 'handler_bytes': 32277, 'timeout': 300, 'memory_mb': 768, 'receipt': {'status': 'matched', 'commit': '45592bc9d232b9be69d526a1ee7d8fcf43f10fa0'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-catalyst-classifier-daily', 'state': 'ENABLED', 'expression': 'cron(0 14 * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-catalyst-classifier', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 45592bc9d232b9be69d526a1ee7d8fcf43f10fa0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Exact two-source package and original runtime/schedule only; synthetic source and compatibility tests. No native output or portfolio qualification. |

## Log

