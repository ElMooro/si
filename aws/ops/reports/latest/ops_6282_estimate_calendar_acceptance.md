
**Status:** success  
**Duration:** 4.3s  
**Finished:** 2026-09-28T12:04:55+00:00  

## Data

| account_reads | actual_runtime | consumer_output_reads | current_archive_verified | expected_commit | history_writes | learning_log_reads | native_invocations | native_publication | private_state_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'LnV+KCmIfn2soHpNOob+04PQsNrLxHUpTVgrdagPrvc=', 'source_files_checked': 4, 'handler_bytes': 25133, 'timeout': 120, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': 'fcf6aa1a6633abb6faace6f2020b44bd9ddc1905'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'estrev-daily-am', 'state': 'ENABLED', 'expression': 'cron(40 13 ? * MON-FRI *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'estrev-daily-pm', 'state': 'ENABLED', 'expression': 'cron(40 17 ? * MON-FRI *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-estimate-revisions-daily', 'state': 'ENABLED', 'expression': 'cron(0 12 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-estimate-revisions', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | True | fcf6aa1a6633abb6faace6f2020b44bd9ddc1905 | 0 | 0 | 0 | {'status': 'published_original_estimate_observations_replayed', 'bytes': 4475044, 'sha256': '9fc51152a1ca1b4ada41615fcebd7518a0f9ef17939f736ea814c4738b50f2a6', 'generated_at': '2026-09-28T12:01:01.521309+00:00', 'version': '3.3.0', 'calendar_occurrences': 675, 'request_occurrences': 280, 'estimate_observations': 1587, 'calendar_originals_verified': True, 'first_release_history_verified': False, 'investment_authority': False, 'current_compiler_publication_verified': True, 'compiler_files': 4, 'calendar_received_occurrences': 1000, 'calendar_excluded_occurrences': 325, 'calendar_pagination': 'next_page_unrequested', 'provider_universe_complete': False, 'market_session_qualified': False} | 0 | 0 | 0 | 0 | Four complete sources and original runtime/schedules. Declared public estimate source and own history only. Whole single calendar response and filtering replay; no additional pages, market-session or first-release qualification, private state or investment authority. |

## Log

