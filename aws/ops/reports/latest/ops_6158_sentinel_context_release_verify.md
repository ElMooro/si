
**Status:** success  
**Duration:** 7.7s  
**Finished:** 2026-09-26T14:46:02+00:00  

## Data

| code_and_receipt_verified | consumer_invocations | notifications_sent | packages | private_account_reads | producer_invocations | provider_requests | public_writes | schedules_changed | scope |
|---|---|---|---|---|---|---|---|---|---|
| True | 0 | 0 | [{'function': 'justhodl-us10y-sentinel', 'expected_commit': '039de5d680aec427484c37139e2f7f64957197fb', 'runtime': {'code_sha256': 'YNWZbXlH9RZKaTZPwnM+ZVxTD5NGDkARoMDWCMD6Btk=', 'source_files_checked': 2, 'handler_bytes': 16601, 'timeout': 300, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '039de5d680aec427484c37139e2f7f64957197fb'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-us10y-sentinel-5x', 'state': 'ENABLED', 'expression': 'cron(20 0,6,12,16,20 * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-us10y-sentinel', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'current_publication_clock': '2026-09-26T12:20:38.515279+00:00', 'current_schema': '2.0', 'new_output_verified': False}, {'function': 'justhodl-master-allocator', 'expected_commit': '039de5d680aec427484c37139e2f7f64957197fb', 'runtime': {'code_sha256': 'jrcfDsZbl6wACVjrcKZHOazdATK7tvaBxtUP9fCrUC8=', 'source_files_checked': 12, 'handler_bytes': 36688, 'timeout': 180, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '039de5d680aec427484c37139e2f7f64957197fb'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-master-allocator-3h', 'state': 'ENABLED', 'expression': 'cron(20 0,3,6,9,12,15,18,21 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-master-allocator', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'current_publication_clock': '2026-09-26T12:20:29.848603+00:00', 'current_schema': None, 'new_output_verified': False}] | 0 | 0 | 0 | 0 | 0 | Exact deployed source proof only; normal new-code outputs and original-source Sentinel replay remain separate. |

## Log

