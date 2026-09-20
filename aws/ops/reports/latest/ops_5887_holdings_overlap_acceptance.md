
**Status:** failure  
**Duration:** 20.3s  
**Finished:** 2026-09-20T02:20:16+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/STAGED/ops_5887_holdings_overlap_acceptance.py", line 129, in main
    manifest, output = verify_current(read, run=packet['replay']['sha256'])
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/scripts/replay_holdings_overlap.py", line 25, in verify_current
    _, products = replay_canonical(read, run=upstream)
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/scripts/replay_holdings_canonical.py", line 25, in verify_current
    products = canonical.replay(manifest, read)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/lambdas/justhodl-13f-positions/source/holdings_canonical.py", line 163, in replay
    research, binding = source(manifest['research']['manifest_key'], read)
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/lambdas/justhodl-13f-positions/source/holdings_canonical.py", line 59, in source
    output = store.replay(manifest, read)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/lambdas/justhodl-13f-positions/source/holdings_store.py", line 153, in replay
    output, artifacts = model.build(probe, read, manifest['generated_at'])
                        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/lambdas/justhodl-13f-positions/source/holdings_native.py", line 481, in build
    index = decode(original(index_ref, read, index_ref['url'], generated_at))
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/lambdas/justhodl-13f-positions/source/holdings_native.py", line 164, in original
    raise ValueError('Original SEC evidence bytes differ')
ValueError: Original SEC evidence bytes differ

```

## Data

| actual_packaged_sources | anonymous_denied | code_sha256 | commit | invocation_started | published | rejected_before_acceptance | runtime | schedule | whole_legacy_product_preserved |
|---|---|---|---|---|---|---|---|---|---|
| {'holdings_overlap.py': {'sha256': 'a8c0b5c49f2945e871b658b636914c0eac27930e494e075407d586821a2bc105', 'bytes': 13498}, 'lambda_function.py': {'sha256': 'ffeaf20e85e5c1b3acf87907c1584e60bda000d56e0143bd9c1829e945bfaa6f', 'bytes': 14778}, 'overlap_store.py': {'sha256': '5292778762d8f808854bba1dd5370931751a6fd66854da531a9858fcde149b41', 'bytes': 6631}} |  | ggJ494bDqNp8SxwDuPZ0UAXg9lP04SEGvoF2jNnymkI= | 2ab77452739ea9ba9f4e91409f6edf2d6a5dc579 |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | {'name': 'justhodl-smart-money-cluster-hourly', 'expression': 'cron(55 * * * ? *)', 'state': 'ENABLED', 'configured_target_verified': True, 'recurring_execution_observed_by_this_op': False} |  |
|  | True |  |  |  |  |  |  |  | {'sha256': 'a6f60a7b662c4d4f502b4b04d7e2a905cc63824c3bcbb05ceededd59f8d56218', 'bytes': 5417715} |
|  |  |  |  | 2026-09-20T02:19:57.215265+00:00 | True | 0 | {'request_id': '65e187ae-2222-462d-ab8b-7defca86d7ce', 'duration_ms': 16844.29, 'memory_size_mb': 1024, 'max_memory_used_mb': 320} |  |  |

## Log

