
**Status:** success  
**Duration:** 1.2s  
**Finished:** 2026-10-02T02:35:34+00:00  

## Data

| evidence |
|---|
| {'status': 'stable_named_sentiment_consumer_control_baseline', 'functions': {'justhodl-allocator': {'FunctionName': 'justhodl-allocator', 'CodeSha256': 'N9VGmX2r3fXqyBwhAuK/yWoodOusg38cL9DMXFTWiak=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 60, 'MemorySize': 256, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'schedules': [], 'schedule_observation': 'no_binding_observed_in_complete_schedule_inventory', 'other_invocation_routes_verified': False}, 'justhodl-signal-logger': {'FunctionName': 'justhodl-signal-logger', 'CodeSha256': '2tyJ5Ejbgt0iP00+o7UFRja5rjIbaBHWccoKTKZhXX0=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 120, 'MemorySize': 256, 'Architectures': ['arm64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-signal-logger-6h', 'state': 'ENABLED', 'expression': 'cron(0 21 * * ? *)', 'native_targets': 1}], 'schedule_observation': 'bound_schedules_observed', 'other_invocation_routes_verified': False}}, 'native_invocations': 0, 'provider_requests': 0, 'application_packet_reads': 0, 'private_reads': 0, 'account_reads': 0, 'native_writes': 0, 'schedule_changes': 0, 'source_qualified': False, 'normal_publication_verified': False, 'investment_authority': False} |

## Log

