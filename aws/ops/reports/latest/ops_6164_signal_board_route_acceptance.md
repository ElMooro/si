
**Status:** success  
**Duration:** 9.8s  
**Finished:** 2026-09-26T18:42:28+00:00  

## Data

| actual_runtime | checked_at | consumer_invocations | expected_commit | ici_metadata | native_invocations | normal_new_code_publication_verified | notifications_sent | private_account_reads | provider_requests | public_derived_read_attempts | public_route_probe | public_writes | schedules_changed | scope | synthetic_regressions |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'code_sha256': 'zraSySWu0Ky24uI8Xyf/InN+F4xhQzXQ8CIsHQgB52Q=', 'source_files_checked': 5, 'handler_bytes': 1179, 'timeout': 600, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '895427505e65d4d8aa13d15e7d11dd92673e08ba'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'signal-board-3h', 'state': 'ENABLED', 'expression': 'cron(15 0/6 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-signal-board', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 2026-09-26T18:42:28.053952+00:00 | 0 | 895427505e65d4d8aa13d15e7d11dd92673e08ba | {'runtime': {'FunctionName': 'justhodl-ici-flows', 'State': 'Active', 'LastUpdateStatus': 'Successful', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 120, 'MemorySize': 256, 'LastModified': '2026-07-02T07:14:35.000+0000', 'CodeSha256': 'aCLhccgfw7t1eC4shHbObCJdvjIjzi24qZ5ebObiPxs='}, 'public_head': {'status': '404'}} | 0 | False | 0 | 0 | 0 | 1 | {'key': 'screener/mean-reversion.json', 'status': 200, 'artifact_identity': 'screener/mean-reversion.json', 'bytes': 70857, 'sha256': '15c652b636d06ea35a272c97a207d899f32739f0ed5e760b9f5491d2d4739b91', 'complete_public_response_read': True} | 0 | 0 | Actual deployed code, preserved runtime and one existing public packet; no native publication is forced. ICI diagnostic is metadata only. | passed |

## Log

