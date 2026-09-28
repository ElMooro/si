
**Status:** success  
**Duration:** 2.3s  
**Finished:** 2026-09-28T14:19:40+00:00  

## Data

| account_reads | actual_runtime | consumer_output_reads | credential_reads | expected_commit | history_writes | learning_log_reads | native_invocations | native_publication | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'h+/Nx+d951P3QM5YsAh3IEScKT/IHJ0E2WqHqycuRoI=', 'source_files_checked': 2, 'handler_bytes': 25460, 'timeout': 720, 'memory_mb': 1536, 'receipt': {'status': 'matched', 'commit': '9b22dbdd6de368e446dc94d0df347e0eddf22e5b'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'jh-earnings-quality-daily', 'state': 'ENABLED', 'expression': 'cron(18 14 * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-earnings-quality-weekly', 'state': 'ENABLED', 'expression': 'cron(30 13 ? * WED *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-earnings-quality', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 0 | 9b22dbdd6de368e446dc94d0df347e0eddf22e5b | 0 | 0 | 0 | {'status': 'published_accounting_projections_reproduced', 'bytes': 9802530, 'sha256': 'c2a27e7d8d2c7b6454f583a6321bfbb573e9c89ad4e197da240ad812bc45e563', 'generated_at': '2026-09-28T14:18:41.606344+00:00', 'version': '1.1.0', 'company_occurrences': 120, 'observations': 2880, 'provider_originals_replayed': False, 'original_sec_filings_replayed': False, 'point_in_time_availability_verified': False} | 0 | 0 | 0 | Exact complete package, unchanged schedule, whole published rows and dated observation reproduction. No original HTTP/SEC replay, first-release availability or investment authority. |

## Log

