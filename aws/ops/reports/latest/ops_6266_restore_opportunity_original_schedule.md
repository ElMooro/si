
**Status:** success  
**Duration:** 1.9s  
**Finished:** 2026-09-28T03:18:53+00:00  

## Data

| actual_after | actual_before | consumer_output_reads | expected_commit | extra_rule_disabled | extra_target_detached | learning_log_reads | native_invocations | notifications_sent | original_scheduler_changed | original_schedules | privacy | provider_requests | public_writes | rollback_original | status |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | {'code_sha256': '9NVR/l+/1QHoheuomB9gXCtsTiGldw30lSf48rk/4q8=', 'source_files_checked': 2, 'handler_bytes': 22823, 'timeout': 120, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': 'efeed143c9a0cd01814af0e8d38e31cd6a89074f'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'opportunity-screener-daily', 'state': 'ENABLED', 'expression': 'cron(0 14 * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge Scheduler', 'name': 'opportunity-screener-sched', 'state': 'ENABLED', 'expression': 'cron(0 14 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-opportunity-screener', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} |  |  |  |  |  |  |  |  | [{'kind': 'EventBridge Scheduler', 'name': 'opportunity-screener-sched', 'state': 'ENABLED', 'expression': 'cron(0 14 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}] |  |  |  |  |  |
| {'code_sha256': '9NVR/l+/1QHoheuomB9gXCtsTiGldw30lSf48rk/4q8=', 'source_files_checked': 2, 'handler_bytes': 22823, 'timeout': 120, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': 'efeed143c9a0cd01814af0e8d38e31cd6a89074f'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'opportunity-screener-sched', 'state': 'ENABLED', 'expression': 'cron(0 14 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-opportunity-screener', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} |  | 0 | efeed143c9a0cd01814af0e8d38e31cd6a89074f | True | True | 0 | 0 | 0 | False |  | {'protected_paths_checked': 1, 'anonymous_origins_checked': 2, 'attempt_outcome_counts': {'404': 1, '403': 1}, 'all_denied': True, 'failures': []} | 0 | 0 | {'key': 'audit-private/20260909-originals/momentum-breakout-research/bf4a740f2a26439294184a3429eb5def9884c062d0814130f7c59995a3260750.bin', 'bytes': 2144, 'sha256': 'bf4a740f2a26439294184a3429eb5def9884c062d0814130f7c59995a3260750'} | original_schedule_restored |

## Log

