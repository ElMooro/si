
**Status:** success  
**Duration:** 2.3s  
**Finished:** 2026-09-28T21:54:54+00:00  

## Data

| actual_runtime | current_publication | downstream_live_head_reads | expected_commit | learning_ledger_reads | native_invocations | next_original_schedule_utc | private_account_reads | private_journal_reads | provider_requests | public_writes | repaired_normal_publication_verified | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'code_sha256': 'zU/OjHmAvfo6UQwz/3mWnOcuk1GXagJ5xXNJlSWHbBw=', 'source_files_checked': 14, 'handler_bytes': 1496, 'timeout': 900, 'memory_mb': 2048, 'receipt': {'status': 'matched', 'commit': '3be085cd8e35efe29869483ebb485e8d215ad495'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-fifx-vol-daily', 'state': 'ENABLED', 'expression': 'cron(20 21 ? * MON-FRI *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-fifx-vol-migration', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | {'sha256': 'aabac40452da9461ea80b7edff11550e71b1e6e53a02bec789e11bef767805af', 'bytes': 31928, 'generated_at': '2026-09-28T21:21:57.335781+00:00', 'quality': {'current_sources': 0, 'history_rows': 0, 'original_rows': 500, 'status': 'degraded', 'total_sources': 18}, 'matches_accepted_predecessor': True} | 0 | 3be085cd8e35efe29869483ebb485e8d215ad495 | 0 | 0 | 2026-09-29T21:20:00+00:00 | 0 | 0 | 0 | 0 | False | 0 | Actual repaired code and exact release only, with unchanged complete operating settings and original schedule. The current predecessor packet remains historical evidence; source recovery requires the next ordinary native run and complete replay. |

## Log

