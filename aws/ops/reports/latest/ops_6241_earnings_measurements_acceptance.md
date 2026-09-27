
**Status:** success  
**Duration:** 4.0s  
**Finished:** 2026-09-27T19:59:30+00:00  

## Data

| account_reads | actual_runtime | consumer_output_reads | credential_reads | expected_commit | history_writes | learning_log_reads | native_invocations | native_publication | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'h+/Nx+d951P3QM5YsAh3IEScKT/IHJ0E2WqHqycuRoI=', 'source_files_checked': 2, 'handler_bytes': 25460, 'timeout': 720, 'memory_mb': 1536, 'receipt': {'status': 'matched', 'commit': '9b22dbdd6de368e446dc94d0df347e0eddf22e5b'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'jh-earnings-quality-daily', 'state': 'ENABLED', 'expression': 'cron(18 14 * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge Scheduler', 'name': 'justhodl-earnings-quality-weekly', 'state': 'ENABLED', 'expression': 'cron(30 13 ? * WED *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-earnings-quality', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 0 | 9b22dbdd6de368e446dc94d0df347e0eddf22e5b | 0 | 0 | 0 | {'status': 'pending_original_schedule_publication', 'bytes': 70428, 'sha256': 'f3194fd60429b11c9588793cf0cd55c2577369e1b31fec82791e0bd30bcf9633', 'generated_at': '2026-09-27T14:18:40Z', 'version': '1.0.0'} | 0 | 0 | 0 | Exact complete package, unchanged schedule, whole published rows and dated observation reproduction. No original HTTP/SEC replay, first-release availability or investment authority. |

## Log

