
**Status:** success  
**Duration:** 27.4s  
**Finished:** 2026-09-28T11:44:32+00:00  

## Data

| account_reads | consumer_output_reads | history_writes | learning_log_reads | native_invocations | private_state_reads | producer | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0 | 0 | 0 | 0 | 0 | {'function': 'justhodl-volatility-squeeze-hunter', 'key': 'data/volatility-squeeze.json', 'expected_commit': 'c665a99758acd208469643d3061ee39b0b6bcaf9', 'actual_runtime': {'code_sha256': '5gLPc/gwtTQBbUFGqgNN1kZ+lHoqb+h/h/jNAerR+9U=', 'source_files_checked': 3, 'handler_bytes': 26739, 'timeout': 600, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': 'c665a99758acd208469643d3061ee39b0b6bcaf9'}, 'schedules': [], 'function_name': 'justhodl-volatility-squeeze-hunter', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'fanout_route': {'manifest_key': 'config/fanout-manifest.json', 'sha256': 'decdf10e3114fef2919769327ad6c19a7041fb6f33f8b8b6a017a7ed03f086dd', 'bytes': 11573, 'etag': '"a8b0d37b4f8e60d26597b7c0fd0f08a5"', 'matching_ticks': ['daily-morn'], 'routes': [{'tick': 'daily-morn', 'kind': 'EventBridge rule', 'name': 'jhk-tick-daily-morn', 'state': 'ENABLED', 'expression': 'cron(0 11 * * ? *)', 'timezone': 'UTC'}], 'router_code_sha256': 'fIYL7j/Di8OpAhWWDKG3dnE9ay6kNjsWbe9tt72okZE=', 'router_sources_checked': 2}, 'producer_settings': {'request_limits': {'N_WORKERS': 12, 'MAX_TICKERS': 1500, 'TIMEOUT_BUDGET_S': 550}, 'public_head': 'data/volatility-squeeze.json', 'bucket': 'justhodl-dashboard-live'}, 'native_publication': {'status': 'pending_original_schedule_resumed_publication', 'version': '2.0.0', 'generated_at': '2026-09-28T11:00:25.922316+00:00', 'bytes': 3184786, 'sha256': 'da9a5634fad9b82e9fd664bc086fa8152157268ab979d0f0a3329fde49de10cb', 'resumption_publication_verified': False, 'investment_authority': False}, 'current_archive_verified': True, 'consumer_qualification': False, 'investment_authority': False} | 0 | 0 | 0 | Exact public Price Compression producer and original route only; complete own originals/history and selected request progress. No invocation or investment qualification. |

## Log

