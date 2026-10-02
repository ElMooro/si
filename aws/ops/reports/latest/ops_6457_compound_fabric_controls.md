
**Status:** success  
**Duration:** 1.2s  
**Finished:** 2026-10-02T10:16:37+00:00  

## Data

| evidence |
|---|
| {'status': 'stable_named_compound_fabric_control_baseline', 'functions': {'justhodl-compound-aggregator': {'FunctionName': 'justhodl-compound-aggregator', 'CodeSha256': 'VheOT+MnS9oYam6NGv4kqGZMTAUP2zsk0vHTznDtpCU=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 120, 'MemorySize': 512, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'compound-aggregator-daily', 'state': 'ENABLED', 'expression': 'cron(15 21 ? * MON-FRI *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'schedule_observation': 'bound_schedules_observed', 'other_invocation_routes_verified': False}, 'justhodl-signal-fabric': {'FunctionName': 'justhodl-signal-fabric', 'CodeSha256': '8FzeRJD3R5LTEL8/e+2bKQUusUuUI9LCXL4BSGMo25E=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 300, 'MemorySize': 512, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-signal-fabric-hourly', 'state': 'ENABLED', 'expression': 'cron(5 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-signal-fabric-cadence', 'state': 'ENABLED', 'expression': 'cron(10 0 * * ? *)', 'native_targets': 1}], 'schedule_observation': 'bound_schedules_observed', 'other_invocation_routes_verified': False}}, 'native_invocations': 0, 'provider_requests': 0, 'application_packet_reads': 0, 'private_reads': 0, 'account_reads': 0, 'native_writes': 0, 'schedule_changes': 0, 'source_qualified': False, 'normal_publication_verified': False, 'investment_authority': False} |

## Log

