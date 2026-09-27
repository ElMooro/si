
**Status:** success  
**Duration:** 4.4s  
**Finished:** 2026-09-27T13:53:02+00:00  

## Data

| account_reads | actual_runtime | all_denied | anonymous_origins_checked | attempt_outcome_counts | compiler_sha256 | credential_reads | expected_commit | failures | history_writes | native_invocations | native_publication | protected_paths_checked | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'XUw42W5kHHmIPxDUH85YlbQm3AD2vTbY58O2VMWjlbo=', 'source_files_checked': 4, 'handler_bytes': 31746, 'timeout': 120, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': 'cac0fa83b8df15aefac7419d42dbcf1d08b442b2'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'china-liquidity-daily', 'state': 'ENABLED', 'expression': 'cron(30 14 ? * MON-FRI *)', 'native_targets': 1}], 'function_name': 'justhodl-china-liquidity', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | True | 2 | {'404': 1, '403': 1} | {'lambda_function.py': '0c7bf8ec3dddb9feb443d3afaa281a6dfbb4865797e7db1c7ba4165bdc86dc03', 'china_store.py': '5921cab4edc391fb0bf0a5145fd418f8cd5f5a58ae51f68b231a11e4e46c7ef2', 'china_measurements.py': '974bd173fc6d58d14615fcfa23a3d1cdabacd1e655b3256ebe090af50de744d8', '_fred_shim.py': 'd08c506bffca294140df149d42a4f2895f445029e26c2d51ca1d3de6cc9b3428'} | 0 | cac0fa83b8df15aefac7419d42dbcf1d08b442b2 | [] | 0 | 0 | {'status': 'pending_original_daily_1430_publication', 'bytes': 6804, 'sha256': '067b7e4bc64652c2c5a5bbc77d697a50fc208192f3479df7404834b67b6bb690', 'generated_at': '2026-09-25T14:30:32.880735+00:00', 'version': None} | 1 | 0 | 0 | 0 | Whole acquired sources, complete history and exact monetary/rate/price calendars. Legacy NBS/PBoC extraction, historical vintages, economic leads and portfolio consequences remain unqualified. |

## Log

