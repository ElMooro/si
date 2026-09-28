
**Status:** success  
**Duration:** 9.6s  
**Finished:** 2026-09-28T15:18:42+00:00  

## Data

| account_reads | actual_runtime | all_denied | anonymous_origins_checked | attempt_outcome_counts | compiler_sha256 | credential_reads | expected_commit | failures | history_writes | native_invocations | native_publication | protected_paths_checked | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'rTLqyoSzBvyCX0wYFGwwJmpgHWnsFy2TATNlmqZ/mJ8=', 'source_files_checked': 4, 'handler_bytes': 31926, 'timeout': 120, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': 'b84b1513c115572e3cfffb5a5c66389134ea895f'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'china-liquidity-daily', 'state': 'ENABLED', 'expression': 'cron(30 14 ? * MON-FRI *)', 'native_targets': 1}], 'function_name': 'justhodl-china-liquidity', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | True | 2 | {'404': 1, '403': 1} | {'lambda_function.py': '019f21685fbd2af56938b325c58e4f8434743bd5f548bc8e2f5d84d2ef4261e0', 'china_store.py': 'e7ba58170bb1316057486783087d0f6999791fb3f88a43f0a64299af24fde98c', 'china_measurements.py': '974bd173fc6d58d14615fcfa23a3d1cdabacd1e655b3256ebe090af50de744d8', '_fred_shim.py': 'd08c506bffca294140df149d42a4f2895f445029e26c2d51ca1d3de6cc9b3428'} | 0 | b84b1513c115572e3cfffb5a5c66389134ea895f | [] | 0 | 0 | {'status': 'pending_original_daily_1430_publication', 'bytes': 6804, 'sha256': '067b7e4bc64652c2c5a5bbc77d697a50fc208192f3479df7404834b67b6bb690', 'generated_at': '2026-09-25T14:30:32.880735+00:00', 'version': None} | 1 | 0 | 0 | 0 | Whole acquired sources, complete history and exact monetary/rate/price calendars. Legacy NBS/PBoC extraction, historical vintages, economic leads and portfolio consequences remain unqualified. |

## Log

