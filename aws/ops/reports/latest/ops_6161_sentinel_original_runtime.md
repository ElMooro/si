
**Status:** success  
**Duration:** 7.4s  
**Finished:** 2026-09-26T15:52:50+00:00  

## Data

| actual_runtime | archive_protection | checked_at | consumer_invocations | expected_commit | normal_publication | notifications_sent | private_account_reads | producer_invocations | provider_requests | public_bytes | public_sha256 | public_writes | schedules_changed | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'code_sha256': 'iCEAGgEYZekywUYjabqMmGLSHjgnRf8LX4sOin2ZyGY=', 'source_files_checked': 5, 'handler_bytes': 702, 'timeout': 300, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': '94b60fe5e7845c4f618dfd1a3d6deb681b7499e7'}, 'schedules': [{'kind': 'EventBridge Scheduler', 'name': 'justhodl-us10y-sentinel-5x', 'state': 'ENABLED', 'expression': 'cron(20 0,6,12,16,20 * * ? *)', 'timezone': 'UTC', 'native_targets': 1}], 'function_name': 'justhodl-us10y-sentinel', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | pending_native_archive | 2026-09-26T15:52:50.124711+00:00 | 0 | 94b60fe5e7845c4f618dfd1a3d6deb681b7499e7 | {'status': 'pending_normal_schedule', 'generated_at': '2026-09-26T12:20:38.515279+00:00'} | 0 | 0 | 0 | 0 | 14528 | 2537866ea2d55ff1b2d19dfd3a14f5511ba2d3d1ad554178e1e29d257c4f7855 | 0 | 0 | Existing source archives are read only if a normal native publication exists. No forced acquisition or publication. |

## Log

