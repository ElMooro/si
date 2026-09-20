
**Status:** success  
**Duration:** 1.6s  
**Finished:** 2026-09-20T04:50:22+00:00  

## Data

| configuration | engine_invocations | eventbridge_rules | notifications_sent | paid_ai_calls | portfolio_writes | private_account_reads | repository_source_matches | runtime_source_bytes | runtime_source_sha256 | scheduler_targets | source_product_changed | whole_preceding_products |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'FunctionName': 'justhodl-domain-barometers', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 600, 'MemorySize': 1536, 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'Architectures': ['x86_64'], 'CodeSha256': 'jU9oJmeUPmnghqLOTbS2HClDgREuXLU54bPRWD6d6bU=', 'State': 'Active', 'LastUpdateStatus': 'Successful'} |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 0 |  |  |  |  |  | True | 41081 | c46d6f740696d722f485891b42f4efe80062d627b8740dd52899b25c80ef19d3 |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | [{'key': 'data/domain-barometers.json', 'sha256': '55312170b68abf30e3844bed57f7c9747550728ee809ac6e6eeec644d579fc95', 'bytes': 5808213, 'anonymous_denied': True, 'generated_at': '2026-09-11T12:20:34.024908+00:00', 'sized_rows': 0, 'version': '1.0', 'contract': None}] |
|  |  | [] |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 0 | 0 | 0 | 0 |  |  |  | [{'Name': 'domain-barometers-daily', 'GroupName': 'default', 'ScheduleExpression': 'cron(20 12 * * ? *)', 'ScheduleExpressionTimezone': 'UTC', 'State': 'ENABLED', 'FlexibleTimeWindow': {'Mode': 'OFF'}}] | False |  |

## Log

