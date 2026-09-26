
**Status:** success  
**Duration:** 5.2s  
**Finished:** 2026-09-26T18:51:58+00:00  

## Data

| access_evidence | account_reads | all_denied | anonymous_origins_checked | attempt_outcome_counts | capture_status | code_matches_repository | failures | history_writes | manifest | native_invocations | notifications_sent | predecessor_runtime | protected_paths_checked | provider_attempts | provider_results | public_writes | schedules | schedules_changed | scope | source_qualification |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'key': 'audit-private/20260909-originals/ici-research/bea0a5314c1e2a69d3ca985a55b62798cf984883f147f8b0163d111ba001e1c7.bin', 'sha256': 'bea0a5314c1e2a69d3ca985a55b62798cf984883f147f8b0163d111ba001e1c7', 'bytes': 8220} | 0 | True | 22 | {'404': 11, '403': 11} | {'data/ici-flows.json': 'missing', 'data/history/ici-mmf.json': 'whole_object_retained', 'data/history/ici-flows.json': 'whole_object_retained', 'data/ops/releases/justhodl-ici-flows.json': 'missing'} | True | [] | 0 | {'key': 'audit-private/20260909-originals/ici-research/16f1ae5e41fc156ed4c09c61e2acb5e170c3ebaa2d615103c0bbbe031c0755c2.bin', 'sha256': '16f1ae5e41fc156ed4c09c61e2acb5e170c3ebaa2d615103c0bbbe031c0755c2', 'bytes': 7691} | 0 | 0 | {'FunctionName': 'justhodl-ici-flows', 'CodeSha256': 'aCLhccgfw7t1eC4shHbObCJdvjIjzi24qZ5ebObiPxs=', 'LastModified': '2026-07-02T07:14:35.000+0000', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 120, 'MemorySize': 256, 'Architectures': ['x86_64']} | 11 | 2 | {'mmf': {'status': 'whole_http_response_retained', 'http_status': 200, 'error_type': None, 'original': {'key': 'audit-private/20260909-originals/ici-research/cc4c32f2ae006470cecfe83f2ebfb24a5f3ef57ae3d362a19b4a59e5ebe03af4.bin', 'sha256': 'cc4c32f2ae006470cecfe83f2ebfb24a5f3ef57ae3d362a19b4a59e5ebe03af4', 'bytes': 357873}}, 'combined_flows': {'status': 'whole_http_response_retained', 'http_status': 200, 'error_type': None, 'original': {'key': 'audit-private/20260909-originals/ici-research/4dd9f533bf4a1d85e165f37e9fd9933b51029e44f1af2c250d1adb96462ba5d6.bin', 'sha256': '4dd9f533bf4a1d85e165f37e9fd9933b51029e44f1af2c250d1adb96462ba5d6', 'bytes': 354823}}} | 0 | [{'kind': 'EventBridge rule', 'name': 'justhodl-ici-flows-weekly', 'state': 'DISABLED', 'expression': 'cron(30 16 ? * WED,THU *)', 'native_targets': 1}] | 0 | Whole protected predecessor baseline and two one-attempt official requests; no output is seeded or produced. | False |

## Log

