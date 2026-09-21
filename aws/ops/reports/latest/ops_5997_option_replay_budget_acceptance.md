
**Status:** success  
**Duration:** 401.8s  
**Finished:** 2026-09-21T15:05:13+00:00  

## Data

| commit | current_compiler_matches_runtime | current_publication | engine_invocations | original_output_unchanged | post_capture_reserve_seconds | proof_key | provider_requests | runner_replay_seconds | runtime_package | schedules_changed | source_packet_writes |
|---|---|---|---|---|---|---|---|---|---|---|---|
| dee010d93c0e3468ca13c6b222473302e11cba0a | False | {'generated_at': '2026-09-21T14:16:16.465370+00:00', 'sha256': '5a981992db0d91e667dba59d5ef20de0549730b9aa6015e6af132face529a66b', 'replay': {'manifest_key': 'data/option-flow-research/runs/766c98dc95802c0f51596df696b2ebe08605b451125f0d2b08516eed5fe97b73.json', 'output_sha256': '563d4da3260257c37b517424f0ada8083231616286ce87fc881edb8d19ffa5f9'}} | 0 | True | 780 | data/option-flow-budget-verification.json | 0 | 399.644 | {'code_sha256': 'pq6PdNeKy2D9TJCywtpfP+k0Deonmnzo0/CZjJQKM6Q=', 'source_files_checked': 8, 'handler_bytes': 2474, 'timeout': 900, 'memory_mb': 4096, 'receipt': {'status': 'matched', 'commit': 'dee010d93c0e3468ca13c6b222473302e11cba0a'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-polygon-options-flow-hourly', 'state': 'ENABLED', 'expression': 'cron(15 14,15,16,17,18,19 ? * MON-FRI *)', 'native_targets': 1}], 'function_name': 'justhodl-polygon-options-flow', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 0 |

## Log

