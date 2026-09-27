
**Status:** success  
**Duration:** 5.3s  
**Finished:** 2026-09-27T09:36:33+00:00  

## Data

| account_reads | actual_runtime | actual_runtime_before_validation | all_denied | anonymous_origins_checked | archive_writes | attempt_outcome_counts | compiler_sha256 | expected_commit | failures | native_invocations | native_publication | protected_paths_checked | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  | {'code_sha256': 'lmpjy45N8YRSVKh78WlU30R+JgWcWIMVYlvB0N/cqhw=', 'source_files_checked': 3, 'handler_bytes': 9335, 'timeout': 180, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '0303dd0ed41e97da7bc735426dbd427288891b03'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-air-cargo-daily', 'state': 'ENABLED', 'expression': 'cron(40 10 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-air-cargo', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 0 | {'code_sha256': 'lmpjy45N8YRSVKh78WlU30R+JgWcWIMVYlvB0N/cqhw=', 'source_files_checked': 3, 'handler_bytes': 9335, 'timeout': 180, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '0303dd0ed41e97da7bc735426dbd427288891b03'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-air-cargo-daily', 'state': 'ENABLED', 'expression': 'cron(40 10 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-air-cargo', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} |  | True | 2 | 0 | {'404': 1, '403': 1} | {'lambda_function.py': 'b28539389e91d2550b2ff3800cc896fae1370722db17810127c95c2c625b92bc', 'air_store.py': '9a85a5c347a564fc82f65680543f7807c9e97293dec590f1a51cc97a237ed984', 'air_measurements.py': '83c91476fd0ddaffa445ceddffe0b515721bf25390d2230814c9689adf960ec8'} | 0303dd0ed41e97da7bc735426dbd427288891b03 | [] | 0 | {'status': 'pending_original_1040_publication', 'generated_at': '2026-09-26T10:40:40.832716+00:00', 'version': '2.0.0', 'bytes': 846, 'sha256': '7b033890c5a5a60f55f2e697fea4f50db261f61f19856cf322ff8ee1e216ca19', 'month': '2026-05'} | 1 | 0 | 0 | 0 | Complete workbook/ledger replay and monthly freight tonnage. No cargo-value, commodity-mix, original-vintage, financial forecast or sizing qualification. Publication remains non-atomic; partial-head recovery is separate. |

## Log

