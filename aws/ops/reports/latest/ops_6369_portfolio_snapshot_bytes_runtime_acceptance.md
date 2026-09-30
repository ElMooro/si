
**Status:** success  
**Duration:** 9.4s  
**Finished:** 2026-09-30T13:06:40+00:00  

## Data

| all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_package_verified | expected_commit | native_invocations | normal_private_publication_verified | private_reads | provider_requests | reviewed_snapshot_origin_verified | runtimes | schedule_changes | worker_prerequisite |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| True | 0 | 0 | True | 90e3a348058dafefa34637fb06ccc2f66345337e | 0 | False | 0 | 0 | True | {'justhodl-portfolio-snapshot': {'code_sha256': '9tDCWara8K3SObouwkjLNkhKz3VGes7YkN9k0LfENL4=', 'source_files_checked': 5, 'handler_bytes': 69511, 'timeout': 180, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '90e3a348058dafefa34637fb06ccc2f66345337e'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-portfolio-snapshot-hourly', 'state': 'ENABLED', 'expression': 'cron(40 * * * ? *)', 'native_targets': 1, 'qualified_targets': ['arn:aws:lambda:us-east-1:857687956942:function:justhodl-portfolio-snapshot:live']}, {'kind': 'EventBridge Scheduler', 'name': 'portfolio-snapshot-sched', 'state': 'ENABLED', 'expression': 'cron(40 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default', 'target_qualifier': 'live'}], 'function_name': 'justhodl-portfolio-snapshot', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'active_alias': {'alias': 'live', 'version': '14', 'code_sha256': '9tDCWara8K3SObouwkjLNkhKz3VGes7YkN9k0LfENL4='}}, 'justhodl-portfolio-admin': {'code_sha256': 'dhWtE6dBRvjE3tX+HzN/lbFBckBXrQ+YGzN1fcu5fF8=', 'source_files_checked': 1, 'handler_bytes': 29794, 'timeout': 30, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': '90e3a348058dafefa34637fb06ccc2f66345337e'}, 'schedules': [], 'function_name': 'justhodl-portfolio-admin', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}} | 0 | {'function': 'justhodl-portfolio-snapshot', 'worker': 'justhodl-data-proxy', 'commit': 'faead4f66168eff2ee4c87b303882c21e9715c44', 'status': 'exact_prerequisite_matched', 'worker_invocations': 0, 'private_reads': 0} |

## Log

