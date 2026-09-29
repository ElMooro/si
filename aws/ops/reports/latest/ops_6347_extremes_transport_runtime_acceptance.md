
**Status:** success  
**Duration:** 6.5s  
**Finished:** 2026-09-29T21:19:40+00:00  

## Data

| actual_runtimes | all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_packages_verified | expected_commit | native_invocations | native_publication_verified | original_resources_and_bindings_recorded | phase | private_reads | provider_requests | schedule_changes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'justhodl-capitulation': {'code_sha256': '9pfHudlydRJbztkUEi1yk0Vz7uss8Ju1ox34ugw27W8=', 'source_files_checked': 15, 'handler_bytes': 1712, 'timeout': 60, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': '006d75a9dadd858be56af142a4f12cf140a6f310'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'capitulation-3h', 'state': 'ENABLED', 'expression': 'cron(45 */3 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-capitulation', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'justhodl-market-extremes': {'code_sha256': 'clmw3CIkecDVF2DhdjIYQ/QUK/xQxnUKAx6sgs4QUfM=', 'source_files_checked': 14, 'handler_bytes': 1715, 'timeout': 60, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': '006d75a9dadd858be56af142a4f12cf140a6f310'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-market-extremes-daily', 'state': 'ENABLED', 'expression': 'cron(0 23 * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-market-extremes', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}} |  |  |  |  | 006d75a9dadd858be56af142a4f12cf140a6f310 |  |  |  | post_change |  |  |  |
|  | True | 0 | 0 | True |  | 0 | False | True |  | 0 | 0 | 0 |

## Log

