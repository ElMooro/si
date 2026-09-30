
**Status:** success  
**Duration:** 1.9s  
**Finished:** 2026-09-30T21:48:43+00:00  

## Data

| evidence |
|---|
| {'source_commit': 'b84b1513c115572e3cfffb5a5c66389134ea895f', 'native_before': {'code_sha256': 'rTLqyoSzBvyCX0wYFGwwJmpgHWnsFy2TATNlmqZ/mJ8=', 'source_files_checked': 4, 'handler_bytes': 31926, 'timeout': 120, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': 'b84b1513c115572e3cfffb5a5c66389134ea895f'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'china-liquidity-daily', 'state': 'ENABLED', 'expression': 'cron(30 14 ? * MON-FRI *)', 'native_targets': 1}], 'function_name': 'justhodl-china-liquidity', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'native_after': {'code_sha256': 'rTLqyoSzBvyCX0wYFGwwJmpgHWnsFy2TATNlmqZ/mJ8=', 'source_files_checked': 4, 'handler_bytes': 31926, 'timeout': 120, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': 'b84b1513c115572e3cfffb5a5c66389134ea895f'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'china-liquidity-daily', 'state': 'ENABLED', 'expression': 'cron(30 14 ? * MON-FRI *)', 'native_targets': 1}], 'function_name': 'justhodl-china-liquidity', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'window_start': '2026-09-30T14:30:00+00:00', 'window_end': '2026-09-30T14:45:00+00:00', 'status': 'no_reviewed_failure_context_found', 'complete_matching_diagnostics': [], 'native_invocations': 0, 'provider_requests': 0, 'current_packet_reads': 0, 'private_reads': 0, 'account_reads': 0, 'consumer_reads': 0, 'native_writes': 0, 'schedule_changes': 0, 'publication_or_recovery_verified': False, 'scope': 'Only exact reviewed safe failure-context events in the original September 30 14:30-14:45 UTC window. No raw exception text, arbitrary logs, current packets or retained private source reads.'} |

## Log

