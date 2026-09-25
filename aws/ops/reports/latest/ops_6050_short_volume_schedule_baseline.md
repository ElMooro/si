
**Status:** success  
**Duration:** 1.4s  
**Finished:** 2026-09-25T04:25:57+00:00  

## Data

| engine_invocations | findings | manifest | notifications_sent | private_account_reads | provider_requests | public_head_writes | schedule_mutations |
|---|---|---|---|---|---|---|---|
| 0 | {'contract': 'short-volume-schedule-baseline.v1', 'generated_at': '2026-09-25T04:25:56.977258+00:00', 'request_id': 'chatgpt-short-volume-schedules-6050', 'schedules': {'justhodl-finra-short': [{'name': 'finra-short-sched', 'group': 'default', 'expression': 'cron(0 1 ? * TUE-SAT *)', 'timezone': 'UTC', 'state': 'ENABLED', 'flexible_time_window': {'Mode': 'OFF'}, 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-finra-short', 'target_role_arn': 'arn:aws:iam::857687956942:role/justhodl-scheduler-role', 'input_sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 'retry_policy': {'MaximumEventAgeInSeconds': 86400, 'MaximumRetryAttempts': 185}, 'dead_letter_arn': None, 'start_date': None, 'end_date': None}], 'justhodl-short-pressure': [{'name': 'short-pressure-sched', 'group': 'default', 'expression': 'cron(30 12 ? * MON-FRI *)', 'timezone': 'UTC', 'state': 'ENABLED', 'flexible_time_window': {'Mode': 'OFF'}, 'target_arn': 'arn:aws:lambda:us-east-1:857687956942:function:justhodl-short-pressure', 'target_role_arn': 'arn:aws:iam::857687956942:role/justhodl-scheduler-role', 'input_sha256': 'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855', 'retry_policy': {'MaximumEventAgeInSeconds': 86400, 'MaximumRetryAttempts': 185}, 'dead_letter_arn': None, 'start_date': None, 'end_date': None}]}, 'classic_default_bus_rules': {'justhodl-finra-short': [], 'justhodl-short-pressure': []}, 'scheduler_groups_scanned': ['default'], 'schedulers_scanned': 407, 'other_invocation_paths_excluded_from_claim': True} | {'key': 'audit-private/20260909-originals/short-volume-research/f44d5935f9f40108ad046342cf465c6e9a458b762cff8971954b9f161a1332b5.bin', 'sha256': 'f44d5935f9f40108ad046342cf465c6e9a458b762cff8971954b9f161a1332b5', 'bytes': 1489} | 0 | 0 | 0 | 0 | 0 |

## Log

