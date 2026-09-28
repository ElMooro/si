
**Status:** failure  
**Duration:** 2.7s  
**Finished:** 2026-09-28T16:57:00+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6297_credit_cadence_baseline.py", line 58, in main
    before=runtime(lam,s3,events,scheduler,FN);validate_runtime(before)
                                               ^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6297_credit_cadence_baseline.py", line 45, in validate_runtime
    raise ValueError('One original weekday 22:10 UTC schedule required')
ValueError: One original weekday 22:10 UTC schedule required

```

## Log

