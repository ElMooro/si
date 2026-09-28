
**Status:** success  
**Duration:** 12.3s  
**Finished:** 2026-09-28T11:09:54+00:00  

## Data

| account_reads | actual_runtime | consumer_output_reads | current_archive_verified | expected_commit | fanout_route | history_writes | learning_log_reads | native_invocations | native_publication | private_state_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'c9Zj4oVjad1hZyVKyGYPXiZB3tlkfVJ/PA5JZGBm9+Q=', 'source_files_checked': 3, 'handler_bytes': 28003, 'timeout': 600, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '84b204d6ff7363c79a5cfee845e60e1cc43b54a0'}, 'schedules': [], 'function_name': 'justhodl-revenue-acceleration', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | True | 84b204d6ff7363c79a5cfee845e60e1cc43b54a0 | {'manifest_key': 'config/fanout-manifest.json', 'sha256': 'decdf10e3114fef2919769327ad6c19a7041fb6f33f8b8b6a017a7ed03f086dd', 'bytes': 11573, 'etag': '"a8b0d37b4f8e60d26597b7c0fd0f08a5"', 'matching_ticks': ['daily-morn'], 'routes': [{'tick': 'daily-morn', 'kind': 'EventBridge rule', 'name': 'jhk-tick-daily-morn', 'state': 'ENABLED', 'expression': 'cron(0 11 * * ? *)', 'timezone': 'UTC'}], 'router_code_sha256': 'fIYL7j/Di8OpAhWWDKG3dnE9ay6kNjsWbe9tt72okZE=', 'router_sources_checked': 2} | 0 | 0 | 0 | {'status': 'published_revenue_originals_replayed', 'bytes': 4029846, 'sha256': '0be90e4a291a739650c3005b6d8d38ee2dc7509ebcaba1adde32746987e12783', 'generated_at': '2026-09-28T11:00:15.540645+00:00', 'version': '1.2.0', 'universe_occurrences': 5135, 'request_occurrences': 10, 'statement_observations': 75, 'first_release_history_verified': False, 'independent_evidence': False, 'investment_authority': False, 'new_queue_live_verified': True, 'planned_occurrences': 10, 'visited_occurrences': 10, 'remaining_occurrences': 0, 'received_statements': 10, 'whole_universe_current_coverage_qualified': False, 'stop_reason': 'planned_window_complete', 'retained_provider_bytes': 105315} | 0 | 0 | 0 | 0 | Exact full producer package and original enabled fanout. Declared public producer and its own immutable source/history only. No whole-universe freshness or investment qualification. |

## Log

