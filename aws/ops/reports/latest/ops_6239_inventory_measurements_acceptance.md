
**Status:** success  
**Duration:** 1.3s  
**Finished:** 2026-09-27T19:28:02+00:00  

## Data

| account_reads | actual_runtime | consumer_output_reads | credential_reads | expected_commit | history_writes | learning_log_reads | native_invocations | native_publication | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'pDbMCIw7ba9ajF+4wMFROUqRaWgYD8ha/kOLaXAbdm0=', 'source_files_checked': 3, 'handler_bytes': 17461, 'timeout': 180, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': 'c8288121fc6b7e2ddbb47a93aaa8be3c3725b14d'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-inventory-drawdown-weekly', 'state': 'ENABLED', 'expression': 'cron(30 23 ? * TUE *)', 'native_targets': 1}, {'kind': 'EventBridge Scheduler', 'name': 'inventory-drawdown-sched', 'state': 'ENABLED', 'expression': 'cron(30 23 ? * TUE *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-inventory-drawdown', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 0 | c8288121fc6b7e2ddbb47a93aaa8be3c3725b14d | 0 | 0 | 0 | {'status': 'pending_original_schedule_publication', 'bytes': 15541, 'sha256': 'b4cb5016b18fa0cbaa2431985dca56f7139857e517d0148423ea2d2abb36fd62', 'generated_at': '2026-09-22T23:30:58.599641+00:00', 'version': '1.0.0'} | 0 | 0 | 0 | Exact complete package, unchanged schedule, whole published rows and dated observation reproduction. No original HTTP/SEC replay, first-release availability or investment authority. |

## Log

