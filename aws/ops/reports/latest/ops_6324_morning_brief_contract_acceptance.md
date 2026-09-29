
**Status:** success  
**Duration:** 3.8s  
**Finished:** 2026-09-29T01:02:51+00:00  

## Data

| actual_runtime | current_consumer_reads | exact_package_accepted | expected_commit | learning_ledger_reads | messages_sent | native_invocations | native_publication_verified | private_account_reads | provider_requests | public_writes | recipient_reads | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'code_sha256': 'LSsqkljjdvDUOV25jPZkX3mw5vpl4a3+8h3xvoVafhM=', 'source_files_checked': 26, 'handler_bytes': 99109, 'timeout': 120, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': 'def9b766832a4d9873fbb196e0903dbaae328b5f'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-morning-brief-daily', 'state': 'ENABLED', 'expression': 'rate(1 day)', 'native_targets': 1}], 'function_name': 'justhodl-morning-intelligence', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['arm64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} |  |  | def9b766832a4d9873fbb196e0903dbaae328b5f |  |  |  |  |  |  |  |  |  |  |
|  | 0 | True |  | 0 | 0 | 0 | False | 0 | 0 | 0 | 0 | 0 | Actual whole ZIP, exact release receipt and stable existing runtime only. No delivery or native publication is claimed. |

## Log

