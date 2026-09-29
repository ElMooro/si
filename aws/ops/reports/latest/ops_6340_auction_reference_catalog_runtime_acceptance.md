
**Status:** success  
**Duration:** 1.7s  
**Finished:** 2026-09-29T10:44:15+00:00  

## Data

| actual_runtimes | all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_packages_verified | expected_commit | native_invocations | native_publication_verified | private_reads | provider_requests | schedule_changes |
|---|---|---|---|---|---|---|---|---|---|---|
| {'justhodl-auction-crisis-detector': {'code_sha256': 'uIjN7CYAI2KW34NOK2WOPP9CK6dvoNTn89n8Q+g00LM=', 'source_files_checked': 22, 'handler_bytes': 43541, 'timeout': 240, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '3cc8aeb6d6b850565a27f2ce800ba3f5d24c609b'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-auction-crisis-active', 'state': 'ENABLED', 'expression': 'cron(50 19 * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-auction-crisis-backstop', 'state': 'ENABLED', 'expression': 'cron(5 13 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-auction-crisis-detector', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}} |  |  |  |  | 3cc8aeb6d6b850565a27f2ce800ba3f5d24c609b |  |  |  |  |  |
|  | True | 0 | 0 | True |  | 0 | False | 0 | 0 | 0 |

## Log

