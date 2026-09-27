
**Status:** success  
**Duration:** 8.8s  
**Finished:** 2026-09-27T01:05:16+00:00  

## Data

| account_reads | actual_runtime | all_denied | anonymous_origins_checked | attempt_outcome_counts | baseline | capture_status | code_matches_repository | failures | history_writes | native_invocations | notifications_sent | protected_paths_checked | provider_requests | public_writes | schedules | schedules_changed | scope | source_difference_names | source_files_checked | source_inventory |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | {'FunctionName': 'justhodl-global-recession', 'CodeSha256': 'tObAhs03ZQ0S1CsiqI4vSHKZWIuPjTNuAnkiXiF4Ts8=', 'LastModified': '2026-08-01T02:00:40.000+0000', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 300, 'MemorySize': 512, 'Architectures': ['x86_64']} | True | 24 | {'404': 12, '403': 12} | {'key': 'audit-private/20260909-originals/global-recession-research/863712562cb23f3ac62fb46aaea39d14a98f1ce48afe1ae1f01ab5e37520016a.bin', 'sha256': '863712562cb23f3ac62fb46aaea39d14a98f1ce48afe1ae1f01ab5e37520016a', 'bytes': 12219} | {'data/global-business-cycle.json': 'whole_object_retained', 'data/oecd-cli.json': 'whole_object_retained', 'data/portwatch.json': 'whole_object_retained', 'data/china-liquidity.json': 'whole_object_retained', 'data/indicator-bus.json': 'whole_object_retained', 'data/global-recession.json': 'whole_object_retained', 'data/ops/releases/justhodl-global-recession.json': 'missing'} | True | [] | 0 | 0 | 0 | 12 | 0 | 0 | [] | 0 | Complete actual predecessor package, current output and five fixed derived inputs only. Earlier FRED provider bodies are not recreated. Source arithmetic, historical vintages, investment and portfolio qualification remain unverified. | [] | 1 | {'fixed_derived_inputs': ['data/global-business-cycle.json', 'data/oecd-cli.json', 'data/portwatch.json', 'data/china-liquidity.json', 'data/indicator-bus.json'], 'prior_head': 'data/global-recession.json', 'native_writer_stages': 2, 'provider_families': ['FRED T10Y3M and SAHMCURRENT'], 'original_provider_responses_retained': False, 'historical_provider_acquisition_gap': 'Earlier original FRED responses were not preserved by this producer.'} |

## Log

