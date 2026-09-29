
**Status:** success  
**Duration:** 4.3s  
**Finished:** 2026-09-29T17:50:24+00:00  

## Data

| actual_runtimes | all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_packages_verified | expected_commit | native_invocations | native_publication_verified | original_resources_and_bindings_recorded | phase | private_reads | provider_requests | schedule_changes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'justhodl-capitulation': {'code_sha256': '0/EOL/UpceHeVk7qPGg+EEwS5IMYFUWSH8IHS88Z2dA=', 'source_files_checked': 15, 'handler_bytes': 1712, 'timeout': 60, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': '0900c1f81c8d7745e2783adcd01034cbd3d033f0'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'capitulation-3h', 'state': 'ENABLED', 'expression': 'cron(45 */3 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-capitulation', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'justhodl-market-extremes': {'code_sha256': 'DTK6FOS3TB/4Mur0C/32kzQZUTuzleWCjLbJwIkuFZk=', 'source_files_checked': 14, 'handler_bytes': 1715, 'timeout': 60, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': '0900c1f81c8d7745e2783adcd01034cbd3d033f0'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-market-extremes-daily', 'state': 'ENABLED', 'expression': 'cron(0 23 * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-market-extremes', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}} |  |  |  |  | 0900c1f81c8d7745e2783adcd01034cbd3d033f0 |  |  |  | post_change |  |  |  |
|  | True | 0 | 0 | True |  | 0 | False | True |  | 0 | 0 | 0 |

## Log

