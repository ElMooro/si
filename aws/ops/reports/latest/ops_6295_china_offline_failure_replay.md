
**Status:** success  
**Duration:** 15.4s  
**Finished:** 2026-09-28T15:00:05+00:00  

## Data

| account_reads | actual_runtime | consumer_reads | history_writes | offline_replay | production_invocations | provider_requests | public_writes | retained_attempts | retained_originals | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'XUw42W5kHHmIPxDUH85YlbQm3AD2vTbY58O2VMWjlbo=', 'source_files_checked': 4, 'handler_bytes': 31746, 'timeout': 120, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': 'cac0fa83b8df15aefac7419d42dbcf1d08b442b2'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'china-liquidity-daily', 'state': 'ENABLED', 'expression': 'cron(30 14 ? * MON-FRI *)', 'native_targets': 1}], 'function_name': 'justhodl-china-liquidity', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 0 | {'status': 'offline_calculation_completed', 'production_invocations': 0, 'provider_requests': 0, 'public_writes': 0, 'isolated_calculations': 1, 'replay_clock': '2026-09-28T14:30:04.103130+00:00', 'clock_basis': 'First retained attempt, not a reconstruction of the original handler start', 'projected_keys': ['data/china-liquidity-history.json', 'data/china-liquidity.json', 'pboc/afre-flow-cache.json'], 'native_result_type': 'dict', 'attempts_replayed': 40, 'retained_attempts': 40, 'last_request': {'source_host': 'www.pbc.gov.cn', 'endpoint': 'https://justhodl-data-proxy.raafouis.workers.dev/gov', 'parameter_names': ['u']}, 'staged_keys': ['data/china-liquidity-history.json', 'data/china-liquidity.json', 'pboc/afre-flow-cache.json'], 'session_failure': None, 'notifications_suppressed': 0} | 0 | 0 | 0 | 40 | 31 | 0 | Actual source code executed only in an isolated test-loaded calculator with retained provider responses and in-memory staged outputs. No production invocation, provider transport, notification, secret load or source-body disclosure. |

## Log

