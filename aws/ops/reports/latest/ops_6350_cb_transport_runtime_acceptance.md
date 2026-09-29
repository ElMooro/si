
**Status:** success  
**Duration:** 4.1s  
**Finished:** 2026-09-29T21:27:04+00:00  

## Data

| actual_runtime | all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_package_verified | expected_commit | native_invocations | native_publication_verified | original_resources_and_bindings_recorded | phase | private_reads | provider_requests | schedule_changes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'code_sha256': '2Dsxn4VlyVSpArKDkUG+e9ToW+N/EiGYeggulyP+wfU=', 'source_files_checked': 11, 'handler_bytes': 15053, 'timeout': 240, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': 'cc666e9a53fdf407e5bbe9a2f81602309291e81d'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'cb-injection-daily', 'state': 'ENABLED', 'expression': 'cron(0 13 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-cb-injection', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} |  |  |  |  | None |  |  |  | predecessor_baseline |  |  |  |
|  | None | 0 | 0 | True |  | 0 | False | True |  | 0 | 0 | 0 |

## Log

