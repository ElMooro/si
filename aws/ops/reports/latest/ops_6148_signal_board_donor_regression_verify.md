
**Status:** success  
**Duration:** 2.2s  
**Finished:** 2026-09-26T13:07:20+00:00  

## Data

| code_and_receipt_verified | consumer_invocations | expected_commit | normal_publication_verified | notifications_sent | private_account_reads | producer_invocations | provider_requests | public_writes | runtime | schedules_changed | scope | trigger_bindings_preserved | trigger_inventory |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| True | 0 | 958de3d210f4d037767095512a603c4f07e6a30e | False | 0 | 0 | 0 | 0 | 0 | {'code_sha256': 'T/W5eK1FcxvmMNiVaHmN0x4Gt7XYW6wMQ6cHD3cq9as=', 'source_files_checked': 5, 'handler_bytes': 1179, 'timeout': 600, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '958de3d210f4d037767095512a603c4f07e6a30e'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'signal-board-3h', 'state': 'ENABLED', 'expression': 'cron(15 0/6 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-signal-board', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | Exact deployed compiler/source package and preserved direct bindings; normal publication and complete public replay still required. | True | {'schedule_groups_scanned': 1, 'schedules_scanned': 408, 'matching_schedules': [], 'event_source_mappings': [], 'direct_bucket_notifications': [], 'indirect_lambda_stepfunction_and_eventbus_callers_verified': False} |

## Log

