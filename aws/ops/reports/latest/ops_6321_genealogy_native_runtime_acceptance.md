
**Status:** success  
**Duration:** 1.5s  
**Finished:** 2026-09-28T22:56:24+00:00  

## Data

| actual_native_capacity_verified | actual_runtime | downstream_output_reads | exact_package_accepted | expected_commit | history_writes | learning_ledger_reads | native_invocations | native_publication_verified | next_original_schedule_utc | original_cadence_preserved | private_account_reads | provider_requests | public_writes | research_head_reads | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | {'code_sha256': '3YqVuEhIXd5H17acR/T3apSiOuvrzzENd50OoRS7dqA=', 'source_files_checked': 17, 'handler_bytes': 1779, 'timeout': 600, 'memory_mb': 2048, 'receipt': {'status': 'matched', 'commit': '46f241c0b3895304039196ea03bed38d8efad778'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'signal-genealogy-daily', 'state': 'ENABLED', 'expression': 'cron(40 6 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-signal-genealogy', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} |  |  | 46f241c0b3895304039196ea03bed38d8efad778 |  |  |  |  |  |  |  |  |  |  |  |  |
| False |  | 0 | True |  | 0 | 0 | 0 | False | 2026-09-29T06:40:00+00:00 | True | 0 | 0 | 0 | 0 | 0 | Exact complete replacement package, reviewed 2048 MB/600-second envelope and original 06:40 schedule only. Actual native publication, durable transport and runtime capacity await the ordinary schedule. No old private-ledger-derived result is read. |

## Log

