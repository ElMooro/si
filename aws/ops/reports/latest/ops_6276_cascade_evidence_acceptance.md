
**Status:** success  
**Duration:** 3.5s  
**Finished:** 2026-09-28T07:41:06+00:00  

## Data

| acceptance | account_reads | actual_package | consumer_output_reads | expected_commit | learning_log_reads | model_requests | native_invocations | private_context_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'status': 'exact_package_and_original_schedule_verified', 'native_output_verified': False, 'consumer_decisions_qualified': False, 'investment_authority': False} | 0 | {'code_sha256': '0BXtqvlu6CkeEDTXx9eYM4tpbA/FBld8Nh2kEoPrfIE=', 'source_files_checked': 5, 'handler_bytes': 36239, 'timeout': 60, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '5f889d42bc9bd0c1e3b9b68f7cb55e20638120cd'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-theme-cascade-twice-per-hour', 'state': 'ENABLED', 'expression': 'cron(20,50 * * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-theme-cascade', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 5f889d42bc9bd0c1e3b9b68f7cb55e20638120cd | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Complete Cascade package, original runtime/schedules and synthetic replay; no private or downstream consumer-output qualification. |

## Log

