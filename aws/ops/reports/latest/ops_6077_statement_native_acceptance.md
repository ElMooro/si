
**Status:** failure  
**Duration:** 38.4s  
**Finished:** 2026-09-25T11:24:28+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6077_statement_native_acceptance.py", line 150, in main
    ready=prepare_ready(s3,read);primary=invoke(lam,s3,commit);r.kv(ready=ready,primary_request=primary)
                                         ^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6077_statement_native_acceptance.py", line 103, in invoke
    assert status and status['status']=='complete','Inspect retained execution; never blindly reinvoke'
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: Inspect retained execution; never blindly reinvoke

```

## Data

| commit | pages_commit | runtime_packages |
|---|---|---|
| 41efe217e24cd3572ea5f58622de6ea7c41d12a4 | 41efe217e24cd3572ea5f58622de6ea7c41d12a4 | {'justhodl-forensic-screen': {'code_sha256': '82VbVijk6ZShvA4tOKNtj4vT8vtmnx8IsAmqCeP1RH4=', 'source_files_checked': 10, 'handler_bytes': 4498, 'timeout': 300, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '41efe217e24cd3572ea5f58622de6ea7c41d12a4'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-forensic-screen-12h', 'state': 'ENABLED', 'expression': 'cron(26 17 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-forensic-screen', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'justhodl-fundamental-census': {'code_sha256': 'MFSSfHrSTQNdGr/NLfBPt99IwZLzjU7APLVC+Xbad6M=', 'source_files_checked': 3, 'handler_bytes': 40909, 'timeout': 900, 'memory_mb': 1536, 'receipt': {'status': 'matched', 'commit': '41efe217e24cd3572ea5f58622de6ea7c41d12a4'}, 'schedules': [], 'function_name': 'justhodl-fundamental-census', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'justhodl-fundamental-graphs': {'code_sha256': 'P5sLuHPLhIXVGlphNUUFgXg9DHhZMVKA7/Lk1mEIfiw=', 'source_files_checked': 2, 'handler_bytes': 112399, 'timeout': 120, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '41efe217e24cd3572ea5f58622de6ea7c41d12a4'}, 'schedules': [], 'function_name': 'justhodl-fundamental-graphs', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'justhodl-opportunity-engine': {'code_sha256': '1e+kASJ2dDL/gdZWR/Ir7nsN1CqZISFaWbaU997X2uY=', 'source_files_checked': 5, 'handler_bytes': 60530, 'timeout': 600, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '41efe217e24cd3572ea5f58622de6ea7c41d12a4'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'opportunity-engine-daily', 'state': 'ENABLED', 'expression': 'cron(0 14 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-opportunity-engine', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'justhodl-short-book': {'code_sha256': 'jVH48bYe6KBBg9qloxdLSPk4lv6fruEhdWsLBBXOGOI=', 'source_files_checked': 5, 'handler_bytes': 10630, 'timeout': 240, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '41efe217e24cd3572ea5f58622de6ea7c41d12a4'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-short-book-daily', 'state': 'ENABLED', 'expression': 'cron(50 21 ? * * *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-short-book', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}} |

## Log

