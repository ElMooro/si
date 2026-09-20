
**Status:** success  
**Duration:** 1.7s  
**Finished:** 2026-09-20T02:46:39+00:00  

## Data

| configuration | engine_invocations | eventbridge_rules | notifications_sent | paid_ai_calls | portfolio_writes | private_account_reads | repository_source_matches | runtime_source_bytes | runtime_source_sha256 | scheduler_targets | source_product_changed | whole_preceding_products |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'FunctionName': 'justhodl-capital-flow', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 180, 'MemorySize': 512, 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'Architectures': ['x86_64'], 'CodeSha256': 'WLr7nNS88r2CXyJ3aN9TqVx6XazvNJJ+YqeTZqY3zJg=', 'State': 'Active', 'LastUpdateStatus': 'Successful'} |  |  |  |  |  |  |  |  |  |  |  |  |
|  | 0 |  |  |  |  |  | True | 24155 | e05acc57c0691b907a058b8803ab5394f518d96db915d11ad4ee32fce09d3795 |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | [{'key': 'data/capital-flow.json', 'sha256': '5d70d0c9d1ea1d91bde22b5fe0f2c0b8afaf53a351891a86d384a8678d61e576', 'bytes': 258947, 'anonymous_denied': True, 'generated_at': '2026-09-19T16:30:53.651312+00:00', 'reported_sources': {'13f': True, 'etf_flows': 45, 'inst_change': 500, 'categories': 15, 'inst_deep': 82, 'funds13f_join': 86}, 'ticker_rows': 300, 'accumulating_rows': 40, 'distributing_rows': 25}, {'key': 'data/capital-flow-history.json', 'sha256': 'f654110f2d85bef437608c9c3820d2d43335db46a563b9c4bc6e90af65f860c5', 'bytes': 90581, 'anonymous_denied': True, 'generated_at': '2026-09-19T16:30:53.581003+00:00', 'history_entries': 66}] |
|  |  | [{'Name': 'justhodl-capital-flow-daily', 'ScheduleExpression': 'cron(30 16 * * ? *)', 'State': 'ENABLED', 'matching_target_ids': ['1']}] |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 0 | 0 | 0 | 0 |  |  |  | [] | False |  |

## Log

