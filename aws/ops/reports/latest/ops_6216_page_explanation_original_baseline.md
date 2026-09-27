
**Status:** failure  
**Duration:** 6.4s  
**Finished:** 2026-09-27T08:56:13+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6216_page_explanation_original_baseline.py", line 55, in main
    if not all(row['code_matches_repository'] is True for row in summaries.values()):raise ValueError('Actual source differs from repository; reconcile before editing')
                                                                                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ValueError: Actual source differs from repository; reconcile before editing

```

## Data

| account_reads | all_denied | anonymous_origins_checked | attempt_outcome_counts | baseline | failures | history_writes | learning_log_reads | model_requests | native_invocations | page_output_reads | producer_runtimes | protected_paths_checked | provider_requests | public_writes | schedules_changed | scope | whole_source_pins |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 0 | True | 6 | {'404': 3, '403': 3} | {'key': 'audit-private/20260909-originals/shipping-consumer-research/c9808246c108a49ac06757d91352ace7597b5b68d0713d526d5e476db9611b79.bin', 'sha256': 'c9808246c108a49ac06757d91352ace7597b5b68d0713d526d5e476db9611b79', 'bytes': 52386} | [] | 0 | 0 | 0 | 0 | 0 | {'justhodl-page-ai': {'status': 'whole_actual_package_retained', 'runtime': {'FunctionName': 'justhodl-page-ai', 'CodeSha256': 'eAdfcdn62NVU+5AOqaj4sd+WhEfR6DLiSCG0MesRX9U=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 300, 'MemorySize': 512, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'LastModified': '2026-08-06T00:12:09.000+0000'}, 'whole_zip': {'key': 'audit-private/20260909-originals/shipping-consumer-research/78075f71d9fad8d554fb900ea9a8f8b1df968447d1e832e24821b431eb115fd5.bin', 'sha256': '78075f71d9fad8d554fb900ea9a8f8b1df968447d1e832e24821b431eb115fd5', 'bytes': 98139}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-page-ai-wave', 'state': 'ENABLED', 'expression': 'cron(10 5 * * ? *)', 'native_targets': 1}, {'kind': 'EventBridge Scheduler', 'name': 'page-ai-sched', 'state': 'ENABLED', 'expression': 'cron(15 * * * ? *)', 'timezone': 'UTC', 'native_targets': 1, 'group': 'default'}], 'active_alias': None, 'repository_config_present': True, 'code_matches_repository': False, 'source_files_checked': 4, 'source_differences': {'llm_cost.py': {'status': 'different_bytes', 'tracked': {'sha256': 'b929fe17aa3f42f8f4f96584f6dadcb78740ea141ca70ef2b4cddd6ee9635e84', 'bytes': 13265}, 'deployed_members': [{'index': 26, 'name': 'llm_cost.py', 'sha256': 'd16f02d98afe010e24deb9d145a551b3491f276c893fd414a9136c9421d1300d', 'bytes': 12935}]}, 'llm_router.py': {'status': 'different_bytes', 'tracked': {'sha256': '24f31bd545b3ac7add99f1b01629231e7d1aac15b208c2f42413fa4e3d45c047', 'bytes': 16483}, 'deployed_members': [{'index': 27, 'name': 'llm_router.py', 'sha256': 'd443ad467b10983edb5665e6ff60bd6ecdfb3ed01aa484399233e4938a53f8df', 'bytes': 15331}]}, 'xai_voice.py': {'status': 'missing_from_package', 'tracked': {'sha256': 'bdbb4a3e47514627195f6cce8ad50ef348b6b6580eb93c4395327f386943a2c8', 'bytes': 2049}, 'deployed_members': []}}}, 'justhodl-page-ai-commentary': {'status': 'whole_actual_package_retained', 'runtime': {'FunctionName': 'justhodl-page-ai-commentary', 'CodeSha256': '1wjwwGo3Oy7ZM/MkSVwRmdzvgieGrv87/YQdSwFNMCQ=', 'Runtime': 'python3.12', 'Handler': 'lambda_function.lambda_handler', 'Timeout': 300, 'MemorySize': 512, 'Architectures': ['x86_64'], 'Role': 'arn:aws:iam::857687956942:role/lambda-execution-role', 'EphemeralStorage': {'Size': 512}, 'LastModified': '2026-09-26T12:37:43.000+0000'}, 'whole_zip': {'key': 'audit-private/20260909-originals/shipping-consumer-research/d708f0c06a373b2ed933f324495c1199dcef822786aeff3bfd841d4b014d3024.bin', 'sha256': 'd708f0c06a373b2ed933f324495c1199dcef822786aeff3bfd841d4b014d3024', 'bytes': 789205}, 'schedules': [{'kind': 'EventBridge rule', 'name': 'justhodl-page-ai-commentary-daily', 'state': 'ENABLED', 'expression': 'cron(0 14 ? * MON-FRI *)', 'native_targets': 1}], 'active_alias': None, 'repository_config_present': True, 'code_matches_repository': True, 'source_files_checked': 13, 'source_differences': {}}} | 3 | 0 | 0 | 0 | Both entire original packages and runtime/cadence metadata only. Source-context completeness, scorecard matching, research permissions and model provenance remain unqualified. No cached page output or customer context was read. | {'justhodl-page-ai': {'bytes': 10195, 'sha256': 'c01267c87d5607f57001676f9ddb6eb74f691805bc5b40426223a1c5b7cd3da8', 'imported_or_executed': False}, 'justhodl-page-ai-commentary': {'bytes': 19882, 'sha256': '40d7312e88234218a76b475d3ca5bcdbcf29f6998562ca358d6ed6681df24e72', 'imported_or_executed': False}} |

## Log

