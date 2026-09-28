
**Status:** success  
**Duration:** 3.2s  
**Finished:** 2026-09-28T06:09:10+00:00  

## Data

| account_reads | consumer_output_reads | learning_log_reads | model_requests | native_invocations | packages | private_context_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0 | 0 | 0 | 0 | {'justhodl-prepump-summary': {'expected_commit': '37883ce96ff5614e7872db3a0c06f9c8ead3d607', 'actual_package': {'code_sha256': 'b2pxE3l8qB3tp/h/b3ttp+5/DmrSgA8lOgUgwdGbB/M=', 'source_files_checked': 3, 'handler_bytes': 15251, 'timeout': 60, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': '37883ce96ff5614e7872db3a0c06f9c8ead3d607'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-prepump-summary-30min', 'state': 'ENABLED', 'expression': 'cron(12,42 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-prepump-summary', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'acceptance': {'status': 'exact_package_and_original_schedule_verified', 'native_output_verified': False, 'consumer_decisions_qualified': False, 'investment_authority': False}}, 'justhodl-pump-radar-brief': {'expected_commit': '37883ce96ff5614e7872db3a0c06f9c8ead3d607', 'actual_package': {'code_sha256': 'AuIAY+OsEJEeYVXrM+vqIGvOklC96i1txwHj0bw7oSE=', 'source_files_checked': 3, 'handler_bytes': 40018, 'timeout': 300, 'memory_mb': 768, 'receipt': {'status': 'matched', 'commit': '37883ce96ff5614e7872db3a0c06f9c8ead3d607'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-pump-radar-brief-daily', 'state': 'ENABLED', 'expression': 'cron(30 13 * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-pump-radar-brief', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'acceptance': {'status': 'exact_package_and_original_schedule_verified', 'native_output_verified': False, 'consumer_decisions_qualified': False, 'investment_authority': False}}} | 0 | 0 | 0 | 0 | Complete Summary and Brief packages, original runtime/schedules and synthetic replay only. No native output or portfolio qualification. |

## Log

