
**Status:** success  
**Duration:** 1.3s  
**Finished:** 2026-09-20T04:20:22+00:00  

## Data

| configuration | engine_invocations | eventbridge_rules | notifications_sent | paid_ai_calls | portfolio_writes | private_account_reads | repository_source_matches | runtime_source_bytes | runtime_source_sha256 | scheduler_targets | source_product_changed | whole_preceding_products |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'FunctionName': 'justhodl-position-sizer', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 60, 'MemorySize': 256, 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'Architectures': ['x86_64'], 'CodeSha256': 'BNpj4DFJt8NjPQ+uSL0MCO5qjGsmomC8wJPJHVaRY7M=', 'State': 'Active', 'LastUpdateStatus': 'Successful'} |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 0 |  |  |  |  |  | True | 5463 | 68e2073693e02671694cf1c4a543893d2d70c109a2f6e6eaf612ccb8e1a61cdf |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | [{'key': 'data/position-sizing.json', 'sha256': 'f3028581ef6b40c9d0817cc35e54d9e81576092969e54c24c9b764930006889d', 'bytes': 2693, 'anonymous_denied': True, 'generated_at': '2026-09-19T23:32:24.858927+00:00', 'sized_rows': 8, 'version': '1.1', 'contract': None}] |
|  |  | [{'Name': 'justhodl-position-sizer-6h', 'ScheduleExpression': 'rate(6 hours)', 'State': 'ENABLED', 'matching_target_ids': ['1']}] |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 0 | 0 | 0 | 0 |  |  |  | [] | False |  |

## Log

