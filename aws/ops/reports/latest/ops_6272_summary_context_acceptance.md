
**Status:** success  
**Duration:** 4.6s  
**Finished:** 2026-09-28T05:33:26+00:00  

## Data

| account_reads | consumer_output_reads | learning_log_reads | model_requests | native_invocations | packages | private_context_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0 | 0 | 0 | 0 | {'justhodl-prepump-summary': {'expected_commit': 'c59bf6521b7b1ca52d8b52aeae0f522d78549b51', 'actual_package': {'code_sha256': 'PAjUiLzPe+OTfx6FL4En/PrhcYu1FX5BL/mF25LREXM=', 'source_files_checked': 3, 'handler_bytes': 14639, 'timeout': 60, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': 'c59bf6521b7b1ca52d8b52aeae0f522d78549b51'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-prepump-summary-30min', 'state': 'ENABLED', 'expression': 'cron(12,42 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-prepump-summary', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'acceptance': {'status': 'exact_package_and_original_schedule_verified', 'native_output_verified': False, 'consumer_decisions_qualified': False, 'investment_authority': False}}, 'justhodl-pump-radar-brief': {'expected_commit': 'c59bf6521b7b1ca52d8b52aeae0f522d78549b51', 'actual_package': {'code_sha256': '0OfrZGplPXr9O1BpQTzmvC945cGRRMleQ+Gir3rZ2fU=', 'source_files_checked': 3, 'handler_bytes': 39748, 'timeout': 300, 'memory_mb': 768, 'receipt': {'status': 'matched', 'commit': 'c59bf6521b7b1ca52d8b52aeae0f522d78549b51'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-pump-radar-brief-daily', 'state': 'ENABLED', 'expression': 'cron(30 13 * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-pump-radar-brief', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'acceptance': {'status': 'exact_package_and_original_schedule_verified', 'native_output_verified': False, 'consumer_decisions_qualified': False, 'investment_authority': False}}} | 0 | 0 | 0 | 0 | Complete Summary and Brief packages, original runtime/schedules and synthetic replay only. No native output or portfolio qualification. |

## Log

