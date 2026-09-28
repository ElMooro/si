
**Status:** success  
**Duration:** 4.5s  
**Finished:** 2026-09-28T06:46:58+00:00  

## Data

| acceptance | account_reads | actual_package | consumer_output_reads | expected_commit | learning_log_reads | model_requests | native_invocations | private_context_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'status': 'exact_package_and_original_schedule_verified', 'native_output_verified': False, 'consumer_decisions_qualified': False, 'investment_authority': False} | 0 | {'code_sha256': 'xDGBRbVMQwTwCcFlxvkvGy9J3cQcBKgu8pwbXyrwC5g=', 'source_files_checked': 3, 'handler_bytes': 14930, 'timeout': 120, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '139460efbe430f9b76ac8dea5da262d28577dca9'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-apex-fusion-3h', 'state': 'ENABLED', 'expression': 'cron(21 12 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-apex-fusion', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 139460efbe430f9b76ac8dea5da262d28577dca9 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Complete Apex package, original runtime/schedules and synthetic replay; no private or downstream consumer-output qualification. |

## Log

