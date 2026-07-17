## G1 — FMP default (no year/quarter) behavior

**Status:** failure  
**Duration:** 0.5s  
**Finished:** 2026-07-17T00:44:21+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_3364_ignition_filed_quarter.py", line 66, in <module>
    deploy_lambda(report=r, function_name=FN,
  File "/home/runner/work/si/si/aws/ops/_lambda_deploy_helpers.py", line 257, in deploy_lambda
    zip_bytes = build_zip(source_dir)
                ^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/_lambda_deploy_helpers.py", line 59, in build_zip
    local_names = {f.name for f in source_dir.rglob("*.py") if f.is_file()}
                                   ^^^^^^^^^^^^^^^^
AttributeError: 'str' object has no attribute 'rglob'

```

## Log
- `00:44:21` [G1] no-param probe → HTTP 400
- `00:44:21` ✅ G1 ✓ endpoint REQUIRES year+quarter (400 without) → ignition's unpinned call was silently 400ing into the institutional-holders fallback = inst lens DEAD (no change-fields). v1.1.0 revives it with clean pinned data.
## G2 — deploy ignition v1.1.0

