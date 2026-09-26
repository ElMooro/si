
**Status:** success  
**Duration:** 5.5s  
**Finished:** 2026-09-26T13:30:27+00:00  

## Data

| code_and_receipt_verified | consumer_invocations | expected_commit | normal_publication_verified | notifications_sent | original_provider_replay_performed | packages | private_account_reads | producer_invocations | provider_requests | public_writes | schedules_changed | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| True | 0 | 110bcd711e1dd17f6dc7f76c79abe4acc34013c9 | False | 0 | False | [{'runtime': {'code_sha256': 'hguxfOfQemAwN8kXV4xxq7KnG7f1PXutThR2JBNDVwk=', 'source_files_checked': 9, 'handler_bytes': 573, 'timeout': 300, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '110bcd711e1dd17f6dc7f76c79abe4acc34013c9'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'liquidity-pulse-6h', 'state': 'ENABLED', 'expression': 'cron(3 16 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-liquidity-pulse', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'trigger_inventory': {'schedule_groups_scanned': 1, 'schedules_scanned': 408, 'matching_schedules': [], 'event_source_mappings': [], 'direct_bucket_notifications': [], 'indirect_lambda_stepfunction_and_eventbus_callers_verified': False}, 'reviewed_publication_timing_matches': True}, {'runtime': {'code_sha256': 'QpSAubLk5pb0MA4mLmppZVMH8kz2CLQZU7gMQTzYfNQ=', 'source_files_checked': 10, 'handler_bytes': 1311, 'timeout': 600, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '110bcd711e1dd17f6dc7f76c79abe4acc34013c9'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-yield-curve-6h', 'state': 'ENABLED', 'expression': 'cron(52 13 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-yield-curve', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'trigger_inventory': {'schedule_groups_scanned': 1, 'schedules_scanned': 408, 'matching_schedules': [], 'event_source_mappings': [], 'direct_bucket_notifications': [], 'indirect_lambda_stepfunction_and_eventbus_callers_verified': False}, 'reviewed_publication_timing_matches': True}] | 0 | 0 | 0 | 0 | 0 | Actual code, receipt and normal publication timing only. The next scheduled output still requires complete source replay. |

## Log

