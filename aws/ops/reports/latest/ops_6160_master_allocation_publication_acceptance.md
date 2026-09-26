
**Status:** success  
**Duration:** 4.3s  
**Finished:** 2026-09-26T15:29:47+00:00  

## Data

| actual_runtime | consumer_invocations | expected_commit | machine_target_reads | notifications_sent | private_account_reads | producer_invocations | provider_requests | public_and_machine_boundary | public_bytes | public_head_last_modified | public_sha256 | public_writes | schedules_changed | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'code_sha256': 'liQ9sn60zZKthkWXJ+SDf4X/Xl7lWkn1+IXFFh3IqZU=', 'source_files_checked': 13, 'handler_bytes': 36548, 'timeout': 180, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '9ae85b37199793501b0bbc7af9bc8e6b939cacb0'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-master-allocator-3h', 'state': 'ENABLED', 'expression': 'cron(20 0,3,6,9,12,15,18,21 * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'function_name': 'justhodl-master-allocator', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 9ae85b37199793501b0bbc7af9bc8e6b939cacb0 | 1 | 0 | 0 | 0 | 0 | {'contract': 'master-allocation-research.v1', 'generated_at': '2026-09-26T15:20:29.824464+00:00', 'projection_replayed': True, 'machine_target_is_null': True, 'machine_publication_matches': True, 'retained_weight_count': 9, 'retained_momentum_rows': 13, 'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False, 'original_market_sources_replayed': False, 'forecast_qualified': False, 'portfolio_mandate_verified': False} | 9533 | 2026-09-26T15:20:34+00:00 | 96e3c8c52875e9f795633ba5b8a5e2aa571b47e63d1ce2d2ba071ad802da64ba | 0 | 0 | Complete publication and non-actionable machine-target binding; heuristic calculations and market sources remain unqualified. |

## Log

