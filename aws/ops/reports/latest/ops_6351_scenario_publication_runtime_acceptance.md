
**Status:** success  
**Duration:** 4.0s  
**Finished:** 2026-09-29T22:32:53+00:00  

## Data

| actual_runtime | all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_package_verified | expected_commit | native_invocations | native_publication_verified | original_resources_and_bindings_recorded | phase | private_reads | provider_requests | schedule_changes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'code_sha256': 'UOdO4qBsqvzYsz5tZKe49R7DKIC7iAsr6lTe1OhM+1o=', 'source_files_checked': 3, 'handler_bytes': 639, 'timeout': 60, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': 'cf99a7bfc308a750ac3b5722b9f84ae9d6b8b447'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-position-sizer-6h', 'state': 'ENABLED', 'expression': 'rate(6 hours)', 'native_targets': 1}], 'function_name': 'justhodl-position-sizer', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} |  |  |  |  | None |  |  |  | predecessor_baseline |  |  |  |
|  | None | 0 | 0 | True |  | 0 | False | True |  | 0 | 0 | 0 |

## Log

