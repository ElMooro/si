
**Status:** success  
**Duration:** 241.3s  
**Finished:** 2026-09-26T14:36:52+00:00  

## Data

| actual_runtime | consumer_invocations | expected_commit | notifications_sent | private_account_reads | producer_invocations | provider_requests | public_original_replay | public_writes | schedules_changed | scope |
|---|---|---|---|---|---|---|---|---|---|---|
| {'code_sha256': 'y/LDPvazLcj5ALzAqQtEit9rLkMjZl4naupZSqdbS+A=', 'source_files_checked': 18, 'handler_bytes': 59290, 'timeout': 600, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': '16b164d2ab27cdf31dfdbd586e9c79114e3ff456'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-risk-gate-hourly', 'state': 'ENABLED', 'expression': 'rate(1 hour)', 'native_targets': 1, 'qualified_targets': ['arn:aws:lambda:us-east-1:857687956942:function:justhodl-risk-gate:live']}], 'function_name': 'justhodl-risk-gate', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512, 'active_alias': {'alias': 'live', 'version': '10', 'code_sha256': 'y/LDPvazLcj5ALzAqQtEit9rLkMjZl4naupZSqdbS+A='}} | 0 | 16b164d2ab27cdf31dfdbd586e9c79114e3ff456 | 0 | 0 | 0 | 0 | {'calls_eligible': False, 'contract': 'risk-gate-research.v1', 'generated_at': '2026-09-26T13:40:13.817121+00:00', 'output_sha256': '72dde01e96217f658d6970111a8816215cd89d5d3a9c3ce4b50927c5f7eef2c1', 'reconstructed_series': 25, 'replayed': True, 'requested_series': 25, 'scope': 'Retained FRED/ECB original-source reconstruction; fleet remains unqualified context.', 'sizing_eligible': False} | 0 | 0 | Existing native public output and retained original FRED/ECB evidence; no new engine deployment, acquisition, forecast or sizing qualification. |

## Log

