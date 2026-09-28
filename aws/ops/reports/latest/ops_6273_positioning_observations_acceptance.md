
**Status:** success  
**Duration:** 3.0s  
**Finished:** 2026-09-28T06:22:44+00:00  

## Data

| account_reads | consumer_output_reads | learning_log_reads | model_requests | native_invocations | packages | private_context_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0 | 0 | 0 | 0 | {'justhodl-pump-positioning': {'expected_commit': 'a3033de3f1bdb06e2f242318bf880a450a6d0dc6', 'actual_package': {'code_sha256': '0CZIeD00EM0dVe0mpu2sGvh4KiwWl7UJP8M4Y1C35FA=', 'source_files_checked': 4, 'handler_bytes': 51425, 'timeout': 300, 'memory_mb': 768, 'receipt': {'status': 'matched', 'commit': 'a3033de3f1bdb06e2f242318bf880a450a6d0dc6'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-pump-positioning-hourly', 'state': 'ENABLED', 'expression': 'cron(10 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-pump-positioning', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'acceptance': {'status': 'exact_package_and_original_schedule_verified', 'native_output_verified': False, 'consumer_decisions_qualified': False, 'investment_authority': False}}, 'justhodl-apex-fusion': {'expected_commit': 'a3033de3f1bdb06e2f242318bf880a450a6d0dc6', 'actual_package': {'code_sha256': 'VBjPLKNhJ1G/6GhaEBzUYtAFfn3gfMj+JO4ycY3wIsk=', 'source_files_checked': 2, 'handler_bytes': 13091, 'timeout': 120, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': 'a3033de3f1bdb06e2f242318bf880a450a6d0dc6'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-apex-fusion-3h', 'state': 'ENABLED', 'expression': 'cron(21 12 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-apex-fusion', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'acceptance': {'status': 'exact_package_and_original_schedule_verified', 'native_output_verified': False, 'consumer_decisions_qualified': False, 'investment_authority': False}}} | 0 | 0 | 0 | 0 | Complete Positioning and Apex packages, original runtime/schedules, synthetic observations and narrow input-exclusion regression. No consumer-output, portfolio or broader Apex qualification. |

## Log

