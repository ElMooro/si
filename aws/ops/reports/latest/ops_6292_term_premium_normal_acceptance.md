
**Status:** success  
**Duration:** 3.7s  
**Finished:** 2026-09-28T13:51:49+00:00  

## Data

| account_reads | actual_runtime | consumer_output_reads | expected_commit | history_writes | native_invocations | native_publication | own_acquisition_journal | predecessor_access | private_state_reads | provider_requests | public_writes | schedule_changes | scope |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'WCNv0NgyuziaTIk6TZpe0P4NHubUR3Jl0U2qEe5vNT8=', 'source_files_checked': 23, 'handler_bytes': 1359, 'timeout': 120, 'memory_mb': 512, 'receipt': {'status': 'matched', 'commit': 'bdfd0c10bcafd6d41e47f59512057c7f928d9b45'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-acm-daily', 'state': 'ENABLED', 'expression': 'cron(45 13 ? * MON-FRI *)', 'native_targets': 1}], 'function_name': 'justhodl-term-premium', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | 0 | bdfd0c10bcafd6d41e47f59512057c7f928d9b45 | 0 | 0 | {'bytes': 31416, 'sha256': '7005e5499e13589dd289fd837f333c6802cecdc6db58c1c7a067c5bac83d7a1b', 'generated_at': '2026-09-25T13:45:20+00:00', 'contract': None, 'status': 'pending_original_schedule_publication', 'original_workbook_replayed': False, 'investment_authority': False} | {'status': 'acquisition_attempted', 'started_at': '2026-09-28T13:45:15.369949+00:00', 'provider_request_attempts': 1, 'key': 'audit-private/20260909-originals/term-premium-research/requests/13098c191a84e04aaeb1ab00734aba410c525aa93d31ca5e6cfa9c5bf3a17233.json', 'bytes': 359, 'sha256': '53ca789bc1b3df105c095c8f64c992191b441975f86cf3328440cbf695522a6c', 'last_modified': '2026-09-28T13:45:18+00:00'} | {'protected_paths_checked': 1, 'anonymous_origins_checked': 2, 'attempt_outcome_counts': {'404': 1, '403': 1}, 'all_denied': True, 'failures': []} | 0 | 0 | 0 | 0 | Exact public Term Premium producer, retained original workbook, own predecessor artifacts and latest own acquisition journal only. No downstream consumers, provider acquisition, model refit or investment qualification. |

## Log

