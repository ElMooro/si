
**Status:** success  
**Duration:** 4.9s  
**Finished:** 2026-09-29T08:47:48+00:00  

## Data

| actual_runtimes | all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_packages_verified | expected_commit | native_invocations | native_publication_verified | private_reads | provider_requests | schedule_changes |
|---|---|---|---|---|---|---|---|---|---|---|
| {'justhodl-auction-crisis-detector': {'code_sha256': '1oE5RI1zhlog3Q73esdcp6RL/FhAGdIFR4o2uOMiGZU=', 'source_files_checked': 19, 'handler_bytes': 43830, 'timeout': 240, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': 'e8407646ca22e5395efcff44e3364e877ae984ed'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-auction-crisis-active', 'state': 'ENABLED', 'expression': 'cron(50 19 * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-auction-crisis-backstop', 'state': 'ENABLED', 'expression': 'cron(5 13 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-auction-crisis-detector', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}} |  |  |  |  | e8407646ca22e5395efcff44e3364e877ae984ed |  |  |  |  |  |
|  | True | 0 | 0 | True |  | 0 | False | 0 | 0 | 0 |

## Log

