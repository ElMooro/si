
**Status:** success  
**Duration:** 26.2s  
**Finished:** 2026-09-27T12:02:39+00:00  

## Data

| account_reads | actual_runtime | baseline_sha256 | expected_commit | fixture_scope | history_writes | native_invocations | native_publication | notifications_sent | provider_requests | public_writes | schedules_changed | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'e9cvyME5mQ/hL3thhfMEKZySjqKKyr+nBVyweqasuY0=', 'source_files_checked': 4, 'handler_bytes': 14532, 'timeout': 600, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '77dd12daf1d0a7d085ce43d77d2d490c4cd51d9e'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-geopolitical-risk-daily', 'state': 'ENABLED', 'expression': 'cron(30 11 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-geopolitical-risk', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 26fdf187025347feabe9af8f2cb0b257488283853952a39a277060dfaad64a6a | 77dd12daf1d0a7d085ce43d77d2d490c4cd51d9e | Every actual configured feed (164) and country (22), all output fields and 120 retained legacy history dates; synthetic bodies do not substitute for native publication proof. | 0 | 0 | {'status': 'complete_native_original_response_and_history_replayed', 'generated_at': '2026-09-27T11:30:30.651494+00:00', 'version': '2.0.0', 'bytes': 9019458, 'sha256': '847569fdce887f91ee813348c65d2d270856cff34ff44b7782a9ce57760908a6', 'replay': {'status': 'complete_original_response_calculation_replayed', 'feed_acquisitions': 164, 'country_rows': 22, 'entries': 8674, 'retained_legacy_history_dates': 68, 'research_history_runs': 1, 'provider_requests': 0, 'public_writes': 0, 'point_in_time_verified': False, 'forecast_qualified': False}, 'configured_feeds': 164, 'attempted_feeds': 164, 'parsed_feeds': 161, 'quality': 'partial'} | 0 | 0 | 0 | 0 | Whole observed RSS response and deterministic measurement/history replay. News truth, independence, historical vintages, predictive skill and portfolio permission are unqualified. |

## Log

