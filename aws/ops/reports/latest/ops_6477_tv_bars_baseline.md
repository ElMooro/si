
**Status:** success  
**Duration:** 1.7s  
**Finished:** 2026-10-04T21:40:26+00:00  

## Data

| evidence |
|---|
| {'status': 'native_baseline_captured', 'source_commit': '4caf334e86ad3c249aed1420cb783a816fc2446e', 'reviewed_source_sha256': '99c94f7e386f61e9aafcef48598aaa7534c3b8df0a05cf86ec5e591150be06b9', 'native': {'code_sha256': 'qJF5EsUw9CkNC/xFfj8RqIKk2VBZoSnnM423pYUMWg0=', 'source_files_checked': 1, 'handler_bytes': 28995, 'timeout': 600, 'memory_mb': 1024, 'receipt': {'status': 'missing_predecessor_receipt'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-tv-bars-universe-refresh', 'state': 'ENABLED', 'expression': 'cron(30 2 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge rule', 'name': 'justhodl-tv-bars-hourly', 'state': 'DISABLED', 'expression': 'rate(1 hour)', 'native_targets': 1}], 'function_name': 'justhodl-tv-bars', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'declared_settings_match': True, 'observed_enabled_schedules': 1, 'native_invocations': 0, 'provider_requests': 0, 'engine_packet_reads': 0, 'private_reads': 0, 'account_reads': 0, 'credential_reads': 0, 'native_writes': 0, 'schedule_changes': 0, 'application_log_queries': 0, 'scope': 'Exact named Lambda ZIP sources, receipt and selected controls only. No environment values, signed package URL, SSM parameters, provider session, history bank, index, account or log contents returned.'} |

## Log

