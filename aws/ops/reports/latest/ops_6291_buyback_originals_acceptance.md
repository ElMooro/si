
**Status:** success  
**Duration:** 3.6s  
**Finished:** 2026-09-28T13:34:17+00:00  

## Data

| account_reads | actual_runtime | consumer_output_reads | expected_commit | history_writes | learning_log_reads | native_invocations | native_publication | private_state_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': '5GLKyqIiUyfFCg7IK+wuhu6sT0wSqkNfkGDauyqsr/k=', 'source_files_checked': 6, 'handler_bytes': 24604, 'timeout': 600, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '95fb76319673c491a7863e294b54ca365b591a4b'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-buyback-engine-daily', 'state': 'ENABLED', 'expression': 'cron(30 13 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-buyback-engine', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 95fb76319673c491a7863e294b54ca365b591a4b | 0 | 0 | 0 | {'status': 'published_statement_arithmetic_reproduced', 'bytes': 3830868, 'sha256': 'a3c5820a5e37f9b558185cc7a8baf3d3c453e4441c27f628064bc15ebdd3587c', 'generated_at': '2026-09-28T13:30:42.038407+00:00', 'version': '1.2.0', 'issuer_rows': 124, 'cashflow_observations': 618, 'provider_originals_replayed': False, 'point_in_time_availability_verified': False, 'source_capture_status': 'pending_original_schedule_source_capture', 'publication_history_verified': False, 'investment_authority': False} | 0 | 0 | 0 | 0 | Exact public Buyback producer and original cadence; own whole public history and provider originals. Upstream selection/context, SEC first-release vintages and investment authority remain unqualified. |

## Log

