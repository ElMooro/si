
**Status:** failure  
**Duration:** 0.6s  
**Finished:** 2026-07-07T00:31:22+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_2947_verify_engine_wiring.py", line 42, in main
    rep.update(manifest_live_http=c)
    ^^^^^^^^^^
AttributeError: 'Report' object has no attribute 'update'

```

## Log

