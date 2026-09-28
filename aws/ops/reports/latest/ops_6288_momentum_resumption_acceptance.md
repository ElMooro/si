
**Status:** success  
**Duration:** 37.5s  
**Finished:** 2026-09-28T12:06:40+00:00  

## Data

| account_reads | consumer_output_reads | history_writes | learning_log_reads | native_invocations | private_state_reads | producer | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|
| 0 | 0 | 0 | 0 | 0 | 0 | {'function': 'justhodl-momentum-breakout', 'key': 'data/momentum-breakout.json', 'expected_commit': '7978892379f5ab33bd65beb8a7640c12a2ab921f', 'actual_runtime': {'code_sha256': '5FYlZ8HrqG+c9yjHGWd7EX+m9YYdD9YTHaSVUvU6ZG0=', 'source_files_checked': 3, 'handler_bytes': 23776, 'timeout': 300, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '7978892379f5ab33bd65beb8a7640c12a2ab921f'}, 'schedules': [], 'function_name': 'justhodl-momentum-breakout', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'fanout_route': {'manifest_key': 'config/fanout-manifest.json', 'sha256': 'decdf10e3114fef2919769327ad6c19a7041fb6f33f8b8b6a017a7ed03f086dd', 'bytes': 11573, 'etag': '"a8b0d37b4f8e60d26597b7c0fd0f08a5"', 'matching_ticks': ['daily-morn'], 'routes': [{'tick': 'daily-morn', 'kind': 'EventBridge rule', 'name': 'jhk-tick-daily-morn', 'state': 'ENABLED', 'expression': 'cron(0 11 * * ? *)', 'timezone': 'UTC'}], 'router_code_sha256': 'fIYL7j/Di8OpAhWWDKG3dnE9ay6kNjsWbe9tt72okZE=', 'router_sources_checked': 2}, 'producer_settings': {'request_limits': {'N_WORKERS': 12, 'MAX_TICKERS': 600, 'TIMEOUT_BUDGET_S': 260, 'MIN_DOLLAR_VOL': '5000000'}, 'public_head': 'data/momentum-breakout.json', 'bucket': 'justhodl-dashboard-live'}, 'native_publication': {'status': 'pending_original_schedule_resumed_publication', 'version': '2.0.0', 'generated_at': '2026-09-28T11:00:25.675909+00:00', 'bytes': 4270783, 'sha256': 'b6f8059968ca64bc14e6b91800a333d08f75d86c8ac2f66ae9d03991e83ee057', 'resumption_publication_verified': False, 'investment_authority': False}, 'current_archive_verified': True, 'consumer_qualification': False, 'investment_authority': False} | 0 | 0 | 0 | Exact public Momentum Price producer and original route only; complete own originals/history and selected request progress. No invocation or investment qualification. |

## Log

