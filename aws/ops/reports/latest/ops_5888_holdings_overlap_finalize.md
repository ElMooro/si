
**Status:** failure  
**Duration:** 70.2s  
**Finished:** 2026-09-20T02:27:16+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5887_holdings_overlap_acceptance.py", line 156, in main
    build = model.decode(public('build-manifest.json')[0]); assert build['commit_sha'] == commit
                                                                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError

```

## Data

| actual_packaged_sources | anonymous_denied | code_sha256 | commit | counts | finalization_engine_invocations | manager_disclosures | original_sec_to_overlap_reproduced | prior_invocation | prior_runtime | public_artifacts_replayed | schedule | scopes | whole_legacy_product_preserved |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {'holdings_overlap.py': {'sha256': 'a8c0b5c49f2945e871b658b636914c0eac27930e494e075407d586821a2bc105', 'bytes': 13498}, 'lambda_function.py': {'sha256': 'ffeaf20e85e5c1b3acf87907c1584e60bda000d56e0143bd9c1829e945bfaa6f', 'bytes': 14778}, 'overlap_store.py': {'sha256': '5292778762d8f808854bba1dd5370931751a6fd66854da531a9858fcde149b41', 'bytes': 6631}} |  | ggJ494bDqNp8SxwDuPZ0UAXg9lP04SEGvoF2jNnymkI= | 2ab77452739ea9ba9f4e91409f6edf2d6a5dc579 |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | {'name': 'justhodl-smart-money-cluster-hourly', 'expression': 'cron(55 * * * ? *)', 'state': 'ENABLED', 'configured_target_verified': True, 'recurring_execution_observed_by_this_op': False} |  |  |
|  | True |  |  |  |  |  |  |  |  |  |  |  | {'sha256': 'a6f60a7b662c4d4f502b4b04d7e2a905cc63824c3bcbb05ceededd59f8d56218', 'bytes': 5417715} |
|  |  |  |  |  | 0 |  |  | {'run': 35483697031, 'report': 'aws/ops/reports/latest/ops_5887_holdings_overlap_acceptance.md', 'started_at': '2026-09-20T02:19:57.215265+00:00'} | {'request_id': '65e187ae-2222-462d-ab8b-7defca86d7ce', 'duration_ms': 16844.29, 'memory_size_mb': 1024, 'max_memory_used_mb': 320} |  |  |  |  |
|  |  |  |  | {'funds_in_roster': 18, 'funds_with_complete_current_disclosures': 18, 'current_cohort_managers': 15, 'period_cohorts': 4} |  | 39198 | True |  |  | 217 |  | [{'period': '2023-12-31', 'scope': 'SH|NONE', 'managers': 1, 'identities': 40, 'pairs': 0, 'sha256': '9a626dc81602e13afbfb0f43fa18db95d773b1b337cc300fb69997ed4b04263e'}, {'period': '2025-09-30', 'scope': 'SH|CALL', 'managers': 1, 'identities': 2, 'pairs': 0, 'sha256': 'e258725e2be8c5817256c6aeb98b6bcbf89304a2fbf2148904b44bc33bc3fffe'}, {'period': '2025-09-30', 'scope': 'SH|NONE', 'managers': 1, 'identities': 4, 'pairs': 0, 'sha256': '9127c11414b6d34371b850a6e3046736a2515d204988a17d5402258b5c41d648'}, {'period': '2025-09-30', 'scope': 'SH|PUT', 'managers': 1, 'identities': 2, 'pairs': 0, 'sha256': '9884d11d79215fb2451dc9ae8c2a30ba02872ccdd6a614dcc28352cdf79abf33'}, {'period': '2026-03-31', 'scope': 'SH|NONE', 'managers': 1, 'identities': 11, 'pairs': 0, 'sha256': 'd89c6c7046f747e1201ad794618d1e263ba90c63e00e23d5cb29d534e694bb1c'}, {'period': '2026-06-30', 'scope': 'PRN|NONE', 'managers': 15, 'identities': 216, 'pairs': 105, 'sha256': '6d9d5db3f24a39a14facc7316c7d173a2ab678d2962a5068cad68daac9c4f32a'}, {'period': '2026-06-30', 'scope': 'SH|CALL', 'managers': 15, 'identities': 4115, 'pairs': 105, 'sha256': 'dbbcb13de566a0148c80e18bd5926b9cc497a8a88e866453d08527964b35a2e7'}, {'period': '2026-06-30', 'scope': 'SH|NONE', 'managers': 15, 'identities': 7820, 'pairs': 105, 'sha256': '3cce47a8e7ba3a2ad36a286b3f231c8bf253c372b4961124dbf556972889a60f'}, {'period': '2026-06-30', 'scope': 'SH|PUT', 'managers': 15, 'identities': 3409, 'pairs': 105, 'sha256': 'e42ddcfd37eb2a99fe0abb1b23e4913e71bc0100f87143548e3cc4a01cf26cde'}] |  |

## Log

