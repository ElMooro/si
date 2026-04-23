# EventBridge rule cleanup — LIVE

**Status:** failure  
**Duration:** 0.2s  
**Finished:** 2026-04-23T22:00:40+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/46_eb_cleanup.py", line 137, in <module>
    target_descr = ", ".join(t["Arn"].split(":")[-1] for t in targets)
                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/pending/46_eb_cleanup.py", line 137, in <genexpr>
    target_descr = ", ".join(t["Arn"].split(":")[-1] for t in targets)
                             ~^^^^^^^
TypeError: string indices must be integers, not 'str'

```

## Log
## Plan

- `22:00:39`   10 rules queued for deletion
- `22:00:39` 
- `22:00:39` 
  → justhodl-daily-8am  (expect → justhodl-daily-report-v3)  [duplicate of v9-morning]
