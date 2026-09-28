
**Status:** success  
**Duration:** 3.9s  
**Finished:** 2026-09-28T09:44:25+00:00  

## Data

| account_reads | actual_runtime | consumer_output_reads | current_archive_verified | expected_commit | history_writes | learning_log_reads | native_invocations | native_publication | private_state_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'LnV+KCmIfn2soHpNOob+04PQsNrLxHUpTVgrdagPrvc=', 'source_files_checked': 4, 'handler_bytes': 25133, 'timeout': 120, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': 'fcf6aa1a6633abb6faace6f2020b44bd9ddc1905'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'estrev-daily-am', 'state': 'ENABLED', 'expression': 'cron(40 13 ? * MON-FRI *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'estrev-daily-pm', 'state': 'ENABLED', 'expression': 'cron(40 17 ? * MON-FRI *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-estimate-revisions-daily', 'state': 'ENABLED', 'expression': 'cron(0 12 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-estimate-revisions', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | False | fcf6aa1a6633abb6faace6f2020b44bd9ddc1905 | 0 | 0 | 0 | {'status': 'pending_original_schedule_calendar_publication', 'bytes': 231172, 'sha256': '34b9686fc1bee82daf5991502da68c814550763b488804722d989308d693de51', 'generated_at': '2026-09-27T12:00:35.962613+00:00', 'version': '3.1.0', 'current_compiler_publication_verified': False, 'calendar_originals_verified': False, 'investment_authority': False} | 0 | 0 | 0 | 0 | Four complete sources and original runtime/schedules. Declared public estimate source and own history only. Whole single calendar response and filtering replay; no additional pages, market-session or first-release qualification, private state or investment authority. |

## Log

