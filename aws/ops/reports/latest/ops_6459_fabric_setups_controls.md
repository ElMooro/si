
**Status:** success  
**Duration:** 2.4s  
**Finished:** 2026-10-02T11:13:35+00:00  

## Data

| evidence |
|---|
| {'status': 'stable_named_fabric_setups_control_baseline', 'functions': {'justhodl-best-setups': {'FunctionName': 'justhodl-best-setups', 'CodeSha256': 'nUbZWjtmn0gOYHI7UxZrChBfmTsVgZ7pSPWYs8gLFMQ=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 120, 'MemorySize': 512, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-best-setups-hourly', 'state': 'ENABLED', 'expression': 'cron(10 * * * ? *)', 'native_targets': 1}], 'schedule_observation': 'bound_schedules_observed', 'other_invocation_routes_verified': False}, 'justhodl-signal-fabric': {'FunctionName': 'justhodl-signal-fabric', 'CodeSha256': 'PLscOeHbKnNGNKSzC2GVHsdh4BS9ylV7wvGb3TVPFHw=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 300, 'MemorySize': 512, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-signal-fabric-hourly', 'state': 'ENABLED', 'expression': 'cron(5 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1}, {'kind': 'EventBridge rule', 'name': 'justhodl-signal-fabric-cadence', 'state': 'ENABLED', 'expression': 'cron(10 0 * * ? *)', 'native_targets': 1}], 'schedule_observation': 'bound_schedules_observed', 'other_invocation_routes_verified': False}}, 'native_invocations': 0, 'provider_requests': 0, 'application_packet_reads': 0, 'private_reads': 0, 'account_reads': 0, 'native_writes': 0, 'schedule_changes': 0, 'source_qualified': False, 'normal_publication_verified': False, 'investment_authority': False} |

## Log

