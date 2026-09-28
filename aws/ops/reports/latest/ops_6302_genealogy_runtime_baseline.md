
**Status:** success  
**Duration:** 2.4s  
**Finished:** 2026-09-28T18:25:51+00:00  

## Data

| actual_runtime | calculation_qualified | code_matches_repository | credential_reads | downstream_output_reads | forecast_qualified | history_writes | learning_ledger_reads | native_invocations | private_account_reads | provider_requests | public_writes | schedule_changes | scope | sizing_eligible | source_handler_sha256 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'code_sha256': 'XKubBNxEC3T8xcdTl69Zb07b4+F+FOOT94jgcGTeyRg=', 'source_files_checked': 2, 'handler_bytes': 17498, 'timeout': 180, 'memory_mb': 512, 'receipt': {'status': 'missing_predecessor_receipt'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'signal-genealogy-daily', 'state': 'ENABLED', 'expression': 'cron(40 6 * * ? *)', 'native_targets': 1}], 'function_name': 'justhodl-signal-genealogy', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['x86_64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | False | True | 0 | 0 | False | 0 | 0 | 0 | 0 | 0 | 0 | 0 | Whole packaged-source and receipt baseline only. Synthetic counterexamples do not qualify actual data; no learning ledger, source data or derived report is read. | False | d2140ca6a4647a6944d07b14ce2e63d6f52a1c8c2928d7fddbf74093c19cd660 |

## Log

