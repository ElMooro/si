
**Status:** failure  
**Duration:** 97.1s  
**Finished:** 2026-09-21T19:52:59+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6011_futures_calendar_candidate.py", line 222, in main
    counts=independent(output,source['sources'],read);elapsed=round(time.monotonic()-start,3)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6011_futures_calendar_candidate.py", line 98, in independent
    assert cal['returned_rows']==len(event_rows) and cal['invalid_source_row_ordinals']==[]
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError

```

## Log

