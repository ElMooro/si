## A. Orphan roster

**Status:** failure  
**Duration:** 0.3s  
**Finished:** 2026-07-07T23:34:29+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_2984_u9_recon.py", line 59, in main
    n = r.get("name") or r.get("engine")
        ^^^^^
AttributeError: 'str' object has no attribute 'get'

```

## Log

