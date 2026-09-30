
**Status:** success  
**Duration:** 7.8s  
**Finished:** 2026-09-30T11:50:30+00:00  

## Data

| all_original_resources_and_schedules_preserved | archive_history_reads | current_packet_reads | exact_native_package_verified | expected_commit | native_invocations | normal_private_publication_verified | private_reads | provider_requests | runtimes | schedule_changes |
|---|---|---|---|---|---|---|---|---|---|---|
| True | 0 | 0 | True | 6b22576d485b7bf788229e3129aee0fba4165d73 | 0 | False | 0 | 0 | {'justhodl-portfolio-snapshot': {'code_sha256': 'mwNms/xd3LhH0oj1SuuRajn8vOVmjTHIGc+oNVy24nQ=', 'source_files_checked': 4, 'handler_bytes': 69700, 'timeout': 180, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '6b22576d485b7bf788229e3129aee0fba4165d73'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-portfolio-snapshot-hourly', 'state': 'ENABLED', 'expression': 'cron(40 * * * ? *)', 'native_targets': 1, 'qualified_targets': ['arn:aws:lambda:us-east-1:857687956942:function:justhodl-portfolio-snapshot:live']}, {'kind': 'EventBridge Scheduler', 'name': 'portfolio-snapshot-sched', 'state': 'ENABLED', 'expression': 'cron(40 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default', 'target_qualifier': 'live'}], 'function_name': 'justhodl-portfolio-snapshot', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'active_alias': {'alias': 'live', 'version': '13', 'code_sha256': 'mwNms/xd3LhH0oj1SuuRajn8vOVmjTHIGc+oNVy24nQ='}}, 'justhodl-portfolio-risk': {'code_sha256': '1rdY8JPyF9DbMs9HdjTbYHEXat1YwUl5kbzaaVu29NI=', 'source_files_checked': 8, 'handler_bytes': 17453, 'timeout': 300, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '6b22576d485b7bf788229e3129aee0fba4165d73'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-portfolio-risk-hourly', 'state': 'ENABLED', 'expression': 'cron(43 * * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge Scheduler', 'name': 'portfolio-risk-sched', 'state': 'ENABLED', 'expression': 'cron(43 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-portfolio-risk', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}} | 0 |

## Log

