
**Status:** success  
**Duration:** 2.5s  
**Finished:** 2026-09-27T03:08:30+00:00  

## Data

| account_reads | actual_runtime | baseline_sha256 | expected_commit | history_writes | learning_log_reads | model_requests | native_invocations | notifications_sent | public_brief | public_writes | recipient_reads | schedules_changed | scope | synthetic_regressions |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'code_sha256': 'Q2idgrHDGBfemI5k59asd9J/4GAtDIVXincPo3jgdUc=', 'source_files_checked': 25, 'handler_bytes': 99109, 'timeout': 120, 'memory_mb': 256, 'receipt': {'status': 'matched', 'commit': '9fcb0e985b87a32e2bc6f972d829bb8890f49877'}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-morning-brief-daily', 'state': 'ENABLED', 'expression': 'rate(1 day)', 'native_targets': 1}], 'function_name': 'justhodl-morning-intelligence', 'runtime': 'python3.12', 'handler': 'lambda_function.lambda_handler', 'architectures': ['arm64'], 'role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'ephemeral_storage_mb': 512} | ccb4e3108429c31638a2af1b20d86fc5623e964e4deea939bb7ac27ab6ef4dbf | 9fcb0e985b87a32e2bc6f972d829bb8890f49877 | 0 | 0 | 0 | 0 | 0 | {'generated_at': '2026-09-27T00:05:38.911378+00:00', 'source_chars': 3117, 'delivery_utf16_units': 3256, 'delivery_limit_utf16_units': 4096, 'status': 'source_backed_public_brief'} | 0 | 0 | 0 | Exact deployed package and isolated complete-message construction. No live notification or recipient delivery is claimed. | Complete final-message limit, astral Unicode, exact 4096 boundary, over-limit link, malformed/oversized publication timestamp, stale/paid/actionable packet rejection. |

## Log

