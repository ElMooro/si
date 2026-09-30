
**Status:** success  
**Duration:** 5.7s  
**Finished:** 2026-09-30T08:26:55+00:00  

## Data

| actual_runtime | all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_package_verified | expected_commit | native_invocations | normal_private_publication_verified | private_reads | provider_requests | schedule_changes |
|---|---|---|---|---|---|---|---|---|---|---|
| {'code_sha256': 'Zpb6MS83A3og1QFIQayxVuuwnBhpqL4Wrh8zRouvz60=', 'source_files_checked': 3, 'handler_bytes': 52028, 'timeout': 180, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '43bb915e3d64d264e5894782d84b412d745204dd'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-portfolio-snapshot-hourly', 'state': 'ENABLED', 'expression': 'cron(40 * * * ? *)', 'native_targets': 1, 'qualified_targets': ['arn:aws:lambda:us-east-1:857687956942:function:justhodl-portfolio-snapshot:live']}, {'kind': 'EventBridge Scheduler', 'name': 'portfolio-snapshot-sched', 'state': 'ENABLED', 'expression': 'cron(40 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default', 'target_qualifier': 'live'}], 'function_name': 'justhodl-portfolio-snapshot', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'active_alias': {'alias': 'live', 'version': '9', 'code_sha256': 'Zpb6MS83A3og1QFIQayxVuuwnBhpqL4Wrh8zRouvz60='}} |  |  |  |  | 43bb915e3d64d264e5894782d84b412d745204dd |  |  |  |  |  |
|  | True | 0 | 0 | True |  | 0 | False | 0 | 0 | 0 |

## Log

