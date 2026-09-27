
**Status:** success  
**Duration:** 3.9s  
**Finished:** 2026-09-27T21:32:34+00:00  

## Data

| actual_after | actual_before | consumer_output_reads | expected_commit | extra_rule_disabled | extra_target_detached | learning_log_reads | native_invocations | notifications_sent | original_scheduler_changed | original_schedules | privacy | provider_requests | public_writes | rollback_original | status |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | {'code_sha256': 'kPUiS7dZvCO2MdSInCi3pprwiRYjhqSd+ZP77aFBNAY=', 'source_files_checked': 2, 'handler_bytes': 18876, 'timeout': 180, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': 'aefc70173ce90d5da2b44b61f8d6e166c3e15a70'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-scarcity-radar-daily', 'state': 'ENABLED', 'expression': 'cron(45 22 ? * MON-FRI *)', 'native_targets': 1}, {'kind': 'EventBridge Scheduler', 'name': 'scarcity-radar-sched', 'state': 'ENABLED', 'expression': 'cron(45 22 ? * MON-FRI *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-scarcity-radar', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} |  |  |  |  |  |  |  |  | [{'kind': 'EventBridge Scheduler', 'name': 'scarcity-radar-sched', 'state': 'ENABLED', 'expression': 'cron(45 22 ? * MON-FRI *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}] |  |  |  |  |  |
| {'code_sha256': 'kPUiS7dZvCO2MdSInCi3pprwiRYjhqSd+ZP77aFBNAY=', 'source_files_checked': 2, 'handler_bytes': 18876, 'timeout': 180, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': 'aefc70173ce90d5da2b44b61f8d6e166c3e15a70'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'scarcity-radar-sched', 'state': 'ENABLED', 'expression': 'cron(45 22 ? * MON-FRI *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-scarcity-radar', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} |  | 0 | aefc70173ce90d5da2b44b61f8d6e166c3e15a70 | True | True | 0 | 0 | 0 | False |  | {'protected_paths_checked': 1, 'anonymous_origins_checked': 2, 'attempt_outcome_counts': {'404': 1, '403': 1}, 'all_denied': True, 'failures': []} | 0 | 0 | {'key': 'audit-private/20260909-originals/scarcity-radar-research/ddaf267a0bfe941df2e761cfc7918dd0d139364530cef2f417d9066a6812ffea.bin', 'bytes': 2210, 'sha256': 'ddaf267a0bfe941df2e761cfc7918dd0d139364530cef2f417d9066a6812ffea'} | original_schedule_restored |

## Log

