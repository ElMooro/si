
**Status:** success  
**Duration:** 3.7s  
**Finished:** 2026-09-28T15:06:30+00:00  

## Data

| account_reads | actual_runtime | consumer_reads | native_invocations | provider_requests | public_writes | raw_log_bodies_reported | safe_exception_classes | scope |
|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'XUw42W5kHHmIPxDUH85YlbQm3AD2vTbY58O2VMWjlbo=', 'source_files_checked': 4, 'handler_bytes': 31746, 'timeout': 120, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': 'cac0fa83b8df15aefac7419d42dbcf1d08b442b2'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'china-liquidity-daily', 'state': 'ENABLED', 'expression': 'cron(30 14 ? * MON-FRI *)', 'native_targets': 1}], 'function_name': 'justhodl-china-liquidity', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | 0 | 0 | 0 | 0 | [{'timestamp_ms': 1790605881742, 'exception_class': 'EvidenceError'}] | Only the exact reviewed type(exc).__name__ diagnostic from this public producer in September 28 14:30–14:40 UTC. No arbitrary application log body, stack trace or exception text is accepted or reported. |

## Log

