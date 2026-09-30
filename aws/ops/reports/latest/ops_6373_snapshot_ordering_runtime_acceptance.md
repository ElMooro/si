
**Status:** success  
**Duration:** 5.3s  
**Finished:** 2026-09-30T15:31:57+00:00  

## Data

| all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_package_verified | expected_commit | native_invocations | normal_private_publication_verified | private_reads | provider_requests | reviewed_snapshot_origin_verified | runtime | schedule_changes | worker_prerequisite |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| True | 0 | 0 | True | 9a15ce1210ca6d7da0770337e08da5782883ea6f | 0 | False | 0 | 0 | True | {'code_sha256': '50s3yAY3al5CsdsVQpWr8aBGVBbbT0ezJ0KSCazkK+4=', 'source_files_checked': 6, 'handler_bytes': 70558, 'timeout': 180, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '9a15ce1210ca6d7da0770337e08da5782883ea6f'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-portfolio-snapshot-hourly', 'state': 'ENABLED', 'expression': 'cron(40 * * * ? *)', 'native_targets': 1, 'qualified_targets': ['arn:aws:lambda:us-east-1:857687956942:function:justhodl-portfolio-snapshot:live']}, {'kind': 'EventBridge Scheduler', 'name': 'portfolio-snapshot-sched', 'state': 'ENABLED', 'expression': 'cron(40 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default', 'target_qualifier': 'live'}], 'function_name': 'justhodl-portfolio-snapshot', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'active_alias': {'alias': 'live', 'version': '15', 'code_sha256': '50s3yAY3al5CsdsVQpWr8aBGVBbbT0ezJ0KSCazkK+4='}} | 0 | {'function': 'justhodl-portfolio-snapshot', 'worker': 'justhodl-data-proxy', 'commit': 'f3e77dcb361941e7d9bbf298c1f2ed93275bc0c4', 'status': 'exact_prerequisite_matched', 'worker_invocations': 0, 'private_reads': 0} |

## Log

