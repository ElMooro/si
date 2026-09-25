
**Status:** success  
**Duration:** 8.4s  
**Finished:** 2026-09-25T15:44:32+00:00  

## Data

| actual_zip_bytes_verified | all_public_guard_outputs_observed | consumer_invocations | forecast_qualified | native_packages | notifications_sent | paid_ai_calls | portfolio_writes | predecessor_zips_reverified | private_account_reads | producer_invocations | provider_requests | public_packets | public_writes | schedules_changed | signal_writes | sizing_qualified | source_commit |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| True | False | 0 | False | {'justhodl-buyback-engine': {'code_sha256': 'LRPsoFRxM9GvBsnkpcin9uDMhsBcX6Wrfmh61K5K4i0=', 'source_files_checked': 3, 'handler_bytes': 21481, 'timeout': 600, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': 'b726f543e69f059f128976b4fba667ecd3061984'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-buyback-engine-daily', 'state': 'ENABLED', 'expression': 'cron(30 13 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-buyback-engine', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}, 'justhodl-equity-research': {'code_sha256': '+kqT+tuZyxnP6YYQ4I7ZbHl9W6WLMRTmU0nGJVhukkQ=', 'source_files_checked': 7, 'handler_bytes': 304553, 'timeout': 300, 'memory_mb': 1024, 'receipt': {'status': 'matched', 'commit': 'b726f543e69f059f128976b4fba667ecd3061984'}, 'schedules': [], 'function_name': 'justhodl-equity-research', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512}} | 0 | 0 | 0 | 2 | 0 | 0 | 0 | {'justhodl-buyback-engine': {'key': 'data/buyback-engine.json', 'http_status': 200, 'bytes': 258320, 'sha256': 'bde3074e04720e2b7b06d6ae7139c14b58806875b89753489dd1bdf4d339b083', 'generated_at': '2026-09-25T13:30:36.836999+00:00', 'guard_output_observed': False, 'packet_refreshed_by_acceptance': False}, 'justhodl-equity-research': {'guard_output_observed': False, 'packet_refreshed_by_acceptance': False, 'read_policy': 'On-demand company research not requested or invoked during acceptance'}} | 0 | 0 | 0 | False | b726f543e69f059f128976b4fba667ecd3061984 |

## Log

