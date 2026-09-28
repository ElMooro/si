
**Status:** success  
**Duration:** 3.6s  
**Finished:** 2026-09-28T03:58:22+00:00  

## Data

| account_reads | actual_package | consumer_output_reads | current_archive_verified | downstream_consumer_qualification | expected_commit | history_writes | learning_log_reads | native_invocations | native_publication | private_state_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | {'code_sha256': '3XWD63+saQGm1NfxESsmoUc3ROLa7j/RrOS+Q0lTm6E=', 'source_files_checked': 4, 'handler_bytes': 33195, 'timeout': 300, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '2ec7f109d714f8cb1d7848b89af17cacb20c6141'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-momentum-leaders-hourly', 'state': 'ENABLED', 'expression': 'cron(25 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-momentum-leaders', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 0 | {'code_sha256': '3XWD63+saQGm1NfxESsmoUc3ROLa7j/RrOS+Q0lTm6E=', 'source_files_checked': 4, 'handler_bytes': 33195, 'timeout': 300, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '2ec7f109d714f8cb1d7848b89af17cacb20c6141'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-momentum-leaders-hourly', 'state': 'ENABLED', 'expression': 'cron(25 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-momentum-leaders', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | False | False | 2ec7f109d714f8cb1d7848b89af17cacb20c6141 | 0 | 0 | 0 | {'status': 'pending_original_schedule_publication', 'bytes': 133, 'sha256': '570e85d58ee7ebd8eaeabbb196916432c26cb4f194ef5af9c88faa4b52501a48', 'generated_at': '2026-09-28T03:25:18.034183+00:00', 'version': None} | 0 | 0 | 0 | 0 | Exact native producer package and original runtime/schedule only; whole declared public producer packet and its own source/history graph. No consumer output read, native invocation or predictive qualification. |

## Log

