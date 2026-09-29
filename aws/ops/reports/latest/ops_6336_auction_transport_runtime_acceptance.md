
**Status:** success  
**Duration:** 3.7s  
**Finished:** 2026-09-29T08:03:45+00:00  

## Data

| actual_runtimes | all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_packages_verified | expected_commit | native_invocations | native_publication_verified | private_reads | provider_requests | schedule_changes |
|---|---|---|---|---|---|---|---|---|---|---|
| {'justhodl-auction-crisis-detector': {'code_sha256': 'JHMYVJb3XOYXCaRsTf8pTgogRbB9M3pNNd9hn+7v7xQ=', 'source_files_checked': 17, 'handler_bytes': 43225, 'timeout': 240, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '98581fee88f1c432b81377aca13135d11215aecd'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-auction-crisis-active', 'state': 'ENABLED', 'expression': 'cron(50 19 * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-auction-crisis-backstop', 'state': 'ENABLED', 'expression': 'cron(5 13 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-auction-crisis-detector', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}} |  |  |  |  | 98581fee88f1c432b81377aca13135d11215aecd |  |  |  |  |  |
|  | True | 0 | 0 | True |  | 0 | False | 0 | 0 | 0 |

## Log

