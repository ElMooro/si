# ops 4893 — ECB unblock (ciss-stress access pattern)

**Status:** failure  
**Duration:** 5.7s  
**Finished:** 2026-08-18T17:12:13+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/pending/ops_4893_ecb_unblock_ciss_pattern.py", line 106, in main
    rep.row(fn=fn, deploy="ok")
    ^^^^^^^
AttributeError: 'Report' object has no attribute 'row'. Did you mean: 'rows'?

During handling of the above exception, another exception occurred:

Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4893_ecb_unblock_ciss_pattern.py", line 108, in main
    rep.row(fn=fn, deploy="FAIL",
    ^^^^^^^
AttributeError: 'Report' object has no attribute 'row'. Did you mean: 'rows'?

```

## Log
- `17:12:07`   zip: 100323 bytes
## 1. Lambda

- `17:12:07`   Lambda exists — updating
- `17:12:13` ✅   ✓ updated justhodl-ecb-full-catalog
