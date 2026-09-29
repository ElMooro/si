
**Status:** success  
**Duration:** 5.3s  
**Finished:** 2026-09-29T11:23:32+00:00  

## Data

| actual_runtimes | all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_packages_verified | expected_commit | native_invocations | native_publication_verified | private_reads | provider_requests | schedule_changes |
|---|---|---|---|---|---|---|---|---|---|---|
| {'justhodl-auction-crisis-detector': {'code_sha256': 'K7c8UxwKjUbvRkaG0FpoiniPf38DxRom/n7q3CnvCoA=', 'source_files_checked': 23, 'handler_bytes': 42864, 'timeout': 240, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': 'b8443bb5c21056af5045cd931641343dbc17de6a'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-auction-crisis-active', 'state': 'ENABLED', 'expression': 'cron(50 19 * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-auction-crisis-backstop', 'state': 'ENABLED', 'expression': 'cron(5 13 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-auction-crisis-detector', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'justhodl-auction-desk': {'code_sha256': 'rDoUGkyRWxcXa75cZ+VbNwsFTrLackvQ8Chz6VAweQI=', 'source_files_checked': 7, 'handler_bytes': 66268, 'timeout': 300, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': 'b8443bb5c21056af5045cd931641343dbc17de6a'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-auction-desk-results', 'state': 'ENABLED', 'expression': 'cron(40 13 ? * MON-FRI *)', 'timezone': 'America/New_York', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-auction-desk-late', 'state': 'ENABLED', 'expression': 'cron(35 16 ? * MON-FRI *)', 'timezone': 'America/New_York', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-auction-desk-buyback', 'state': 'ENABLED', 'expression': 'cron(10 12 ? * MON-FRI *)', 'timezone': 'America/New_York', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-auction-desk-morning', 'state': 'ENABLED', 'expression': 'cron(15 9 ? * MON-FRI *)', 'timezone': 'America/New_York', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-auction-desk', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}} |  |  |  |  | b8443bb5c21056af5045cd931641343dbc17de6a |  |  |  |  |  |
|  | True | 0 | 0 | True |  | 0 | False | 0 | 0 | 0 |

## Log

