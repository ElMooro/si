
**Status:** success  
**Duration:** 5.3s  
**Finished:** 2026-09-27T00:16:49+00:00  

## Data

| account_reads | actual_runtime | all_denied | anonymous_origins_checked | attempt_outcome_counts | baseline | capture_status | code_matches_repository | failures | history_writes | native_invocations | notifications_sent | protected_paths_checked | provider_requests | public_writes | schedules | schedules_changed | scope | source_difference_names | source_files_checked | source_inventory |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'FunctionName': 'justhodl-global-business-cycle', 'CodeSha256': 'SwUosBZMmcwV6w4IS2bP8C5HUI9BD1nTIM2HbVPssq8=', 'LastModified': '2026-09-09T13:13:06.000+0000', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 900, 'MemorySize': 1536, 'Architectures': ['x86_64']} | True | 30 | {'404': 15, '403': 15} | {'key': 'audit-private/20260909-originals/global-business-cycle-research/5385a8c96a3929aa463af0b3e78cd6acba9dc1b99936d4a4863428f1e762f3d9.bin', 'sha256': '5385a8c96a3929aa463af0b3e78cd6acba9dc1b99936d4a4863428f1e762f3d9', 'bytes': 15423} | {'data/cycle/features.json.gz': 'whole_object_retained', 'data/cycle/features-manifest.json': 'whole_object_retained', 'data/portwatch.json': 'whole_object_retained', 'data/global-business-cycle.json': 'whole_object_retained', 'data/global-business-cycle-history.json': 'whole_object_retained', 'data/global-business-cycle-composite-history.json': 'whole_object_retained', 'data/ops/releases/justhodl-global-business-cycle.json': 'missing'} | True | [] | 0 | 0 | 0 | 15 | 0 | 0 | [{'kind': 'EventBridge rule', 'name': 'justhodl-gbc-daily', 'state': 'ENABLED', 'expression': 'cron(0 12 * * ? *)', 'native_targets': 1}] | 0 | Complete actual predecessor package, published histories and fixed derived inputs only. Earlier provider bodies and dynamic price-warehouse acquisitions are not recreated. Source arithmetic, historical vintages, investment and portfolio qualification remain unverified. | [] | 4 | {'fixed_derived_inputs': ['data/cycle/features.json.gz', 'data/cycle/features-manifest.json', 'data/portwatch.json'], 'prior_head': 'data/global-business-cycle.json', 'dynamic_price_family': 'data/warm/polygon-full/grouped/YYYY/YYYY-MM-DD.json.gz', 'dynamic_price_family_retained': False, 'provider_families': ['Yahoo Finance chart responses', 'FRED observations through managed-secret shim'], 'original_provider_responses_retained': False} |

## Log

