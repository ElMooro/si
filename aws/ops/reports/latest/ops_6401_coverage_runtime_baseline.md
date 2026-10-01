
**Status:** success  
**Duration:** 2.4s  
**Finished:** 2026-10-01T01:10:05+00:00  

## Data

| evidence |
|---|
| {'status': 'baseline_observed', 'repository_source_commit': '281862152ccba8f7f324580068b349d13a9d55f7', 'commit_bound_release_verified': False, 'reviewed_source_hashes': {'aws/lambdas/justhodl-coverage-gap-report/source/lambda_function.py': 'cfe938f70fc781c211088d0ec1e60bb95043e559759d25955dfdeae7a5912258'}, 'native_before': {'code_sha256': 'MCBwqgoD43tK+OnCP6USHDFy5f/AE6Z4ChIGFMNpNc4=', 'source_files_checked': 1, 'handler_bytes': 3310, 'timeout': 120, 'memory_mb': 1024, 'receipt': {'status': 'missing_predecessor_receipt'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-coverage-gap-daily', 'state': 'ENABLED', 'expression': 'cron(45 6 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-coverage-gap-report', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'native_after': {'code_sha256': 'MCBwqgoD43tK+OnCP6USHDFy5f/AE6Z4ChIGFMNpNc4=', 'source_files_checked': 1, 'handler_bytes': 3310, 'timeout': 120, 'memory_mb': 1024, 'receipt': {'status': 'missing_predecessor_receipt'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-coverage-gap-daily', 'state': 'ENABLED', 'expression': 'cron(45 6 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-coverage-gap-report', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'declared_settings': {'function_name': 'justhodl-coverage-gap-report', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'timeout': 120, 'memory_mb': 1024, 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role'}, 'declared_settings_match': True, 'all_observed_schedules_enabled': True, 'normal_publication_verified': False, 'source_replay_verified': False, 'investment_authority': False, 'native_invocations': 0, 'provider_requests': 0, 'current_packet_reads': 0, 'private_reads': 0, 'account_reads': 0, 'consumer_reads': 0, 'native_writes': 0, 'schedule_changes': 0, 'application_log_queries': 0, 'scope': 'Exact native package, public release receipt and selected resource/schedule controls only. No environment values, signed package URL, source summaries, original archive, current report or logs are returned.'} |

## Log

