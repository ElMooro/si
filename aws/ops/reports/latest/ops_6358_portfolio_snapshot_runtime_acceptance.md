
**Status:** success  
**Duration:** 4.2s  
**Finished:** 2026-09-30T05:10:57+00:00  

## Data

| actual_runtime | all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_package_verified | expected_commit | native_invocations | native_publication_verified | original_resources_and_bindings_recorded | phase | private_reads | provider_requests | schedule_changes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'code_sha256': 'bGmMRr5o/EK+8qcVB7ChG6tR44BzUOdVO7EspAFZ+zU=', 'source_files_checked': 3, 'handler_bytes': 23226, 'timeout': 180, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '17a7e8288d62002b1602644bd0d3d8842694fa58'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-portfolio-snapshot-hourly', 'state': 'ENABLED', 'expression': 'cron(40 * * * ? *)', 'native_targets': 1, 'qualified_targets': ['arn:aws:lambda:us-east-1:857687956942:function:justhodl-portfolio-snapshot:live']}, {'kind': 'EventBridge Scheduler', 'name': 'portfolio-snapshot-sched', 'state': 'ENABLED', 'expression': 'cron(40 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default', 'target_qualifier': 'live'}], 'function_name': 'justhodl-portfolio-snapshot', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'active_alias': {'alias': 'live', 'version': '5', 'code_sha256': 'bGmMRr5o/EK+8qcVB7ChG6tR44BzUOdVO7EspAFZ+zU='}} |  |  |  |  | None |  |  |  | predecessor_baseline |  |  |  |
|  | None | 0 | 0 | True |  | 0 | False | True |  | 0 | 0 | 0 |

## Log

