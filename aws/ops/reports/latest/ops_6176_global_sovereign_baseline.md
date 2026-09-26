
**Status:** success  
**Duration:** 3.4s  
**Finished:** 2026-09-26T21:58:15+00:00  

## Data

| account_reads | actual_runtime | all_denied | anonymous_origins_checked | attempt_outcome_counts | baseline | capture_status | code_matches_repository | failures | history_writes | native_invocations | notifications_sent | protected_paths_checked | provider_requests | public_writes | schedules | schedules_changed | scope | source_difference_names | source_files_checked | source_inventory |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'FunctionName': 'justhodl-global-sovereign', 'CodeSha256': 'fECCywD+Cyskv5WykbCrXn6Sm0ONYgF2WgdvaBPVCvM=', 'LastModified': '2026-07-16T16:32:48.000+0000', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 300, 'MemorySize': 512, 'Architectures': ['x86_64']} | True | 16 | {'404': 8, '403': 8} | {'key': 'audit-private/20260909-originals/global-sovereign-research/4f339f1182e486901716ebed4b223dbba2b7450af470a4f6f782eb89c32ebef0.bin', 'sha256': '4f339f1182e486901716ebed4b223dbba2b7450af470a4f6f782eb89c32ebef0', 'bytes': 7295} | {'data/global-sovereign.json': 'whole_object_retained', 'data/global-sovereign-history.json': 'whole_object_retained', 'data/ops/releases/justhodl-global-sovereign.json': 'missing'} | True | [] | 0 | 0 | 0 | 8 | 0 | 0 | [{'kind': 'EventBridge rule', 'name': 'global-sovereign-12h', 'state': 'ENABLED', 'expression': 'cron(15 6,18 * * ? *)', 'native_targets': 1}] | 0 | Complete predecessor and derived S3 input baseline. Source arithmetic, historical vintages, investment and portfolio qualification remain unverified. | [] | 1 | ['data/global-sovereign-history.json'] |

## Log

