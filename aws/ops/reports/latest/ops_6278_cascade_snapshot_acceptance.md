
**Status:** success  
**Duration:** 4.8s  
**Finished:** 2026-09-28T08:21:20+00:00  

## Data

| acceptance | account_reads | actual_package | consumer_output_reads | expected_commit | learning_log_reads | model_requests | native_invocations | private_context_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'status': 'exact_package_and_original_schedule_verified', 'native_output_verified': False, 'consumer_decisions_qualified': False, 'investment_authority': False} | 0 | {'code_sha256': '52Dtws6TSvWHVoO+sropZUcmsCf6QB4bun192Tn9DYg=', 'source_files_checked': 5, 'handler_bytes': 18284, 'timeout': 30, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': '8d40d9ed76f613d9c78362111d9f736c6cd96ed6'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-theme-cascade-backtest-daily', 'state': 'ENABLED', 'expression': 'cron(45 22 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-theme-cascade-backtest', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 8d40d9ed76f613d9c78362111d9f736c6cd96ed6 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Complete Cascade snapshot package, original runtime/schedules and synthetic replay; no private or downstream consumer-output qualification. |

## Log

