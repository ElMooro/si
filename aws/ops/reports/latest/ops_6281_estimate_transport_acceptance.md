
**Status:** success  
**Duration:** 5.6s  
**Finished:** 2026-09-28T09:13:56+00:00  

## Data

| account_reads | actual_runtime | consumer_output_reads | current_archive_verified | expected_commit | history_writes | learning_log_reads | native_invocations | native_publication | private_state_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'ye0CDock3nRTlnNt2RTqKHVbOilDjt6a/btyg3q/v6I=', 'source_files_checked': 4, 'handler_bytes': 23045, 'timeout': 120, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': 'fe700da7fcf65da577db35246e7114ae8b02aac6'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'estrev-daily-am', 'state': 'ENABLED', 'expression': 'cron(40 13 ? * MON-FRI *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'estrev-daily-pm', 'state': 'ENABLED', 'expression': 'cron(40 17 ? * MON-FRI *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-estimate-revisions-daily', 'state': 'ENABLED', 'expression': 'cron(0 12 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-estimate-revisions', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | False | fe700da7fcf65da577db35246e7114ae8b02aac6 | 0 | 0 | 0 | {'status': 'pending_original_schedule_compiler_bound_publication', 'bytes': 231172, 'sha256': '34b9686fc1bee82daf5991502da68c814550763b488804722d989308d693de51', 'generated_at': '2026-09-27T12:00:35.962613+00:00', 'version': '3.1.0', 'current_compiler_publication_verified': False, 'investment_authority': False} | 0 | 0 | 0 | 0 | Four complete native/compiler sources and original runtime/schedules. Declared public estimate source and own immutable history only; calendar originals, first releases, partial-run history continuity and investment authority remain unqualified. |

## Log

