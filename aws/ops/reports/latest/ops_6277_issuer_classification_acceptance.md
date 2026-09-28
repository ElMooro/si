
**Status:** success  
**Duration:** 3.9s  
**Finished:** 2026-09-28T08:05:35+00:00  

## Data

| acceptance | account_reads | actual_package | consumer_output_reads | expected_commit | learning_log_reads | model_requests | native_invocations | private_context_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'status': 'exact_package_and_original_schedule_verified', 'native_output_verified': False, 'consumer_decisions_qualified': False, 'investment_authority': False} | 0 | {'code_sha256': 'S0Rhy0ocnCyVZciXj3jtT1nEicb+A6rR2YKKk7VbrDg=', 'source_files_checked': 4, 'handler_bytes': 24459, 'timeout': 180, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': 'be623de98807c0ea92455b32854d33c3431e7d63'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-theme-classifier-6h', 'state': 'ENABLED', 'expression': 'cron(0 */6 * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-theme-classifier', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | be623de98807c0ea92455b32854d33c3431e7d63 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Complete Issuer classification package, original runtime/schedules and synthetic replay; no private or downstream consumer-output qualification. |

## Log

