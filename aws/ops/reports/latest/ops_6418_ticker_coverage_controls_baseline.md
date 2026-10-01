
**Status:** success  
**Duration:** 6.4s  
**Finished:** 2026-10-01T16:10:30+00:00  

## Data

| evidence |
|---|
| {'status': 'stable_native_control_baseline', 'functions': {'justhodl-flow-confluence': {'FunctionName': 'justhodl-flow-confluence', 'CodeSha256': 'RxAeLpUwdRd/uYoRPBTYW5fn9Kmvk12dE2Hy4OThMdw=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 120, 'MemorySize': 512, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-flow-confluence-daily', 'state': 'ENABLED', 'expression': 'cron(25 13 * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-flow-confluence-sched', 'state': 'ENABLED', 'expression': 'cron(45 22 ? * MON-FRI *)', 'native_targets': 1}]}, 'justhodl-best-ideas': {'FunctionName': 'justhodl-best-ideas', 'CodeSha256': '5EpHUeyZGJDChxbH03VINs34EN3xk0wJP4ATkQtmK0U=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 120, 'MemorySize': 320, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'best-ideas-sched', 'state': 'ENABLED', 'expression': 'cron(45 14 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge rule', 'name': 'best-ideas-daily', 'state': 'ENABLED', 'expression': 'cron(45 14 * * ? *)', 'native_targets': 1}]}, 'justhodl-ticker-360': {'FunctionName': 'justhodl-ticker-360', 'CodeSha256': '+vO4MN6hN4NFxaRGfVWTccoK7wbZsFIfp/QPiihv/R8=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 600, 'MemorySize': 1024, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-ticker-360-schedule', 'state': 'ENABLED', 'expression': 'cron(20 6,18 * * ? *)', 'native_targets': 1}]}}, 'native_invocations': 0, 'provider_requests': 0, 'application_packet_reads': 0, 'private_reads': 0, 'account_reads': 0, 'native_writes': 0, 'schedule_changes': 0, 'source_qualified': False, 'normal_publication_verified': False, 'investment_authority': False} |

## Log

