
**Status:** success  
**Duration:** 4.3s  
**Finished:** 2026-09-29T23:16:29+00:00  

## Data

| actual_runtime | all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_package_verified | expected_commit | native_invocations | native_publication_verified | original_resources_and_bindings_recorded | phase | private_reads | provider_requests | schedule_changes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'code_sha256': 'OFBgexxl46hDXyQLT2BmKgByZpUlmzdgAbP9TGU6ejo=', 'source_files_checked': 6, 'handler_bytes': 11155, 'timeout': 300, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '9f78ef21a30c1d4dd0d556f4be0c3bd6ee10a72a'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-portfolio-risk-hourly', 'state': 'ENABLED', 'expression': 'cron(43 * * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge Scheduler', 'name': 'portfolio-risk-sched', 'state': 'ENABLED', 'expression': 'cron(43 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-portfolio-risk', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} |  |  |  |  | None |  |  |  | predecessor_baseline |  |  |  |
|  | None | 0 | 0 | True |  | 0 | False | True |  | 0 | 0 | 0 |

## Log

