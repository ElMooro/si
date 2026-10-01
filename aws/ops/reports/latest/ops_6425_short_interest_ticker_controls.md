
**Status:** success  
**Duration:** 3.8s  
**Finished:** 2026-10-01T19:15:39+00:00  

## Data

| evidence |
|---|
| {'status': 'stable_native_control_baseline', 'functions': {'justhodl-short-interest': {'FunctionName': 'justhodl-short-interest', 'CodeSha256': 'QLxH3AFBytQzoLonroRuwT3XOVRSPSmttX/ZzpMFXeI=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 360, 'MemorySize': 512, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-short-interest-6h', 'state': 'ENABLED', 'expression': 'cron(20 12 * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-short-interest-sched', 'state': 'ENABLED', 'expression': 'cron(15 21 ? * MON,WED *)', 'native_targets': 1}]}, 'justhodl-microcap-float-squeeze': {'FunctionName': 'justhodl-microcap-float-squeeze', 'CodeSha256': 'NWaltUYgecSIHijRkv3yPLhRIFUX+lbn0WOaZH0CXFs=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 600, 'MemorySize': 2048, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-microcap-float-squeeze-daily', 'state': 'ENABLED', 'expression': 'cron(0 22 ? * MON-FRI *)', 'native_targets': 1}]}}, 'native_invocations': 0, 'provider_requests': 0, 'application_packet_reads': 0, 'private_reads': 0, 'account_reads': 0, 'native_writes': 0, 'schedule_changes': 0, 'source_qualified': False, 'normal_publication_verified': False, 'investment_authority': False} |

## Log

