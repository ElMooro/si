# Final system audit — post all fixes

**Status:** failure  
**Duration:** 1.2s  
**Finished:** 2026-04-27T22:41:59+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/_final_system_audit.py", line 48, in main
    fresh_max_h = fresh_max / 3600
                  ~~~~~~~~~~^~~~~~
TypeError: unsupported operand type(s) for /: 'NoneType' and 'int'

```

## Log
## S3 freshness

