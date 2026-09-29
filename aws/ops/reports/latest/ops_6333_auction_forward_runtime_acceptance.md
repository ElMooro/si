
**Status:** success  
**Duration:** 1.7s  
**Finished:** 2026-09-29T06:19:23+00:00  

## Data

| actual_runtimes | all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_packages_verified | expected_commit | native_invocations | native_publication_verified | private_reads | provider_requests | schedule_changes |
|---|---|---|---|---|---|---|---|---|---|---|
| {'justhodl-auction-crisis-detector': {'code_sha256': 'YRaJBAZ0ohKzQP+a/Im6RvNxoNbm9G95VsJJr68Y5wY=', 'source_files_checked': 13, 'handler_bytes': 41902, 'timeout': 240, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '0e4ad391fde50b7adbfccccb09632647369c3e3f'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-auction-crisis-active', 'state': 'ENABLED', 'expression': 'cron(50 19 * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-auction-crisis-backstop', 'state': 'ENABLED', 'expression': 'cron(5 13 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-auction-crisis-detector', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}} |  |  |  |  | 0e4ad391fde50b7adbfccccb09632647369c3e3f |  |  |  |  |  |
|  | True | 0 | 0 | True |  | 0 | False | 0 | 0 | 0 |

## Log

