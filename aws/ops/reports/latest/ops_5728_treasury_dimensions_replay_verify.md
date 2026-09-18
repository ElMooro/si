
**Status:** failure  
**Duration:** 1812.7s  
**Finished:** 2026-09-18T22:02:24+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5728_treasury_dimensions_replay_verify.py", line 46, in main
    assert time.monotonic() < deadline, 'missing exact release '+fn
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: missing exact release justhodl-treasury-fiscal-full

```

## Log

