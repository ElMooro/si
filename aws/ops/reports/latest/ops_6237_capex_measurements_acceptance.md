
**Status:** success  
**Duration:** 1.6s  
**Finished:** 2026-09-28T20:45:02+00:00  

## Data

| account_reads | actual_runtime | consumer_output_reads | credential_reads | expected_commit | history_writes | learning_log_reads | native_invocations | native_publication | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'jLVFTOMz3Ar1aSh0uXckjk5hEpA1t4vVOf/Rr4vf3DM=', 'source_files_checked': 3, 'handler_bytes': 22921, 'timeout': 280, 'memory_mb': 768, 'receipt': {'status': 'matched', 'commit': '43a43846e2839c4050ceeef90042a44478b882a8'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-capex-pulse-daily', 'state': 'ENABLED', 'expression': 'cron(40 20 ? * MON-FRI *)', 'native_targets': 1}], 'function_name': 'justhodl-capex-pulse', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 0 | 43a43846e2839c4050ceeef90042a44478b882a8 | 0 | 0 | 0 | {'status': 'published_accounting_projections_reproduced', 'bytes': 4436673, 'sha256': '3039b8a37b5781e96b181e1f71d9aeb2eb40c7c6105bcf9588183fe51ff08eee', 'generated_at': '2026-09-28T20:41:03+00:00', 'version': '1.2.0', 'issuer_rows': 159, 'observations': 1267, 'provider_originals_replayed': False, 'original_sec_filings_replayed': False, 'point_in_time_availability_verified': False} | 0 | 0 | 0 | Exact complete package, unchanged schedule, whole published rows and calendar-cohort reproduction. No original HTTP/SEC replay, first-release availability or investment authority. |

## Log

