
**Status:** success  
**Duration:** 1.5s  
**Finished:** 2026-10-02T06:22:29+00:00  

## Data

| evidence |
|---|
| {'status': 'stable_named_ticker_control_baseline', 'functions': {'justhodl-ticker-360': {'FunctionName': 'justhodl-ticker-360', 'CodeSha256': 'Kfmf54bOievTIWRryItxkHZBRxIi3tX0OmDNlJhT8c4=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 600, 'MemorySize': 1024, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-ticker-360-schedule', 'state': 'ENABLED', 'expression': 'cron(20 6,18 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}, {'kind': 'EventBridge rule', 'name': 'justhodl-ticker-360-schedule', 'state': 'ENABLED', 'expression': 'cron(20 6,18 * * ? *)', 'native_targets': 1}], 'schedule_observation': 'bound_schedules_observed', 'other_invocation_routes_verified': False}}, 'native_invocations': 0, 'provider_requests': 0, 'application_packet_reads': 0, 'private_reads': 0, 'account_reads': 0, 'native_writes': 0, 'schedule_changes': 0, 'source_qualified': False, 'normal_publication_verified': False, 'investment_authority': False} |

## Log

