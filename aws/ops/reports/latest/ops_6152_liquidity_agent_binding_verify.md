
**Status:** success  
**Duration:** 59.1s  
**Finished:** 2026-09-26T13:50:32+00:00  

## Data

| code_and_receipt_verified | consumer_invocations | current_public_output_replayed | expected_commit | normal_publication_verified | notifications_sent | original_provider_replay_performed | packages | private_account_reads | producer_invocations | provider_requests | public_writes | schedules_changed | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| True | 0 | {'calls_eligible': False, 'contract': 'liquidity-agent-research.v1', 'current_series': 61, 'generated_at': '2026-09-26T12:30:26.504394+00:00', 'missing_series': ['EXCSRESNW', 'WSHONBIILB', 'WSHOBL', 'WSHOMBLS', 'H41RESPPALDKNWA', 'WCBSL', 'TRESEGUSM052N', 'BAMLC0A0CMFOAS', 'BAMLC0A0CMIOAS', 'BAMLC0A0CMUOAS', 'DRTSCLM', 'OFRFSI'], 'original_rows': 145590, 'replayed': True, 'requested_series': 73, 'sizing_eligible': False} | 3838b7ef01939ac00e120e2ab34fd81e9a83490b | False | 0 | True | [{'runtime': {'code_sha256': 'TD3FsYGmZLR+TlJ3bWAm3ISHOzbc8CnGed66aq3ZXVU=', 'source_files_checked': 11, 'handler_bytes': 1323, 'timeout': 300, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '3838b7ef01939ac00e120e2ab34fd81e9a83490b'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-liquidity-agent-daily', 'state': 'ENABLED', 'expression': 'cron(30 12 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-liquidity-agent', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['arm64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'trigger_inventory': {'schedule_groups_scanned': 1, 'schedules_scanned': 408, 'matching_schedules': [{'Name': 'justhodl-liquidity-agent-daily', 'GroupName': 'default', 'State': 'ENABLED', 'ScheduleExpression': 'cron(30 12 * * ? *)', 'ScheduleExpressionTimezone': 'UTC', 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-liquidity-agent'}], 'event_source_mappings': [], 'direct_bucket_notifications': [], 'indirect_lambda_stepfunction_and_eventbus_callers_verified': False}, 'reviewed_publication_timing_matches': True}] | 0 | 0 | 0 | 0 | 0 | Actual code, exact receipt, preserved timing and full current original-source replay. The existing output may predate this storage-only fix; a normal publication under new code remains separate. |

## Log

