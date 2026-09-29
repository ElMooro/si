
**Status:** success  
**Duration:** 1.4s  
**Finished:** 2026-09-29T13:12:20+00:00  

## Data

| actual_runtimes | all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_packages_verified | expected_commit | native_invocations | native_publication_verified | private_reads | provider_requests | schedule_changes |
|---|---|---|---|---|---|---|---|---|---|---|
| {'justhodl-auction-desk': {'code_sha256': '5T4CD4sxUNWMcpzaEXyFskeuREx67AG0UkABgn//S+A=', 'source_files_checked': 10, 'handler_bytes': 65184, 'timeout': 300, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '6dbcd06802f14534f72176fe3499c82b9df88fb2'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-auction-desk-results', 'state': 'ENABLED', 'expression': 'cron(40 13 ? * MON-FRI *)', 'timezone': 'America/New_York', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-auction-desk-late', 'state': 'ENABLED', 'expression': 'cron(35 16 ? * MON-FRI *)', 'timezone': 'America/New_York', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-auction-desk-buyback', 'state': 'ENABLED', 'expression': 'cron(10 12 ? * MON-FRI *)', 'timezone': 'America/New_York', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-auction-desk-morning', 'state': 'ENABLED', 'expression': 'cron(15 9 ? * MON-FRI *)', 'timezone': 'America/New_York', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-auction-desk', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}} |  |  |  |  | 6dbcd06802f14534f72176fe3499c82b9df88fb2 |  |  |  |  |  |
|  | True | 0 | 0 | True |  | 0 | False | 0 | 0 | 0 |

## Log

