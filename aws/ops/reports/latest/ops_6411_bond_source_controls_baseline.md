
**Status:** success  
**Duration:** 4.3s  
**Finished:** 2026-10-01T13:14:04+00:00  

## Data

| evidence |
|---|
| {'status': 'stable_native_control_baseline', 'functions': {'justhodl-bond-trace': {'FunctionName': 'justhodl-bond-trace', 'CodeSha256': 'HeKE0R/G0Jwdv9R3YRigS9L/5IkS2gLrYNbqrV85oP4=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 120, 'MemorySize': 256, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'bond-trace-daily', 'state': 'ENABLED', 'expression': 'cron(0 21 ? * MON-FRI *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-bond-trace-cadence', 'state': 'ENABLED', 'expression': 'cron(0 21 ? * MON-FRI *)', 'native_targets': 1}]}, 'justhodl-ai-website-synthesis': {'FunctionName': 'justhodl-ai-website-synthesis', 'CodeSha256': '9p3u0r4zkqPWWAxfI7WvQXZ0xkJgcqJhrD196I07G30=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 180, 'MemorySize': 512, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-ai-website-synthesis-hourly', 'state': 'ENABLED', 'expression': 'cron(25 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}]}}, 'native_invocations': 0, 'provider_requests': 0, 'application_packet_reads': 0, 'private_reads': 0, 'account_reads': 0, 'native_writes': 0, 'schedule_changes': 0, 'source_qualified': False, 'normal_publication_verified': False, 'investment_authority': False} |

## Log

