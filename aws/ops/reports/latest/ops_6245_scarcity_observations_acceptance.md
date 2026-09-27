
**Status:** failure  
**Duration:** 2.7s  
**Finished:** 2026-09-27T21:17:01+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6245_scarcity_observations_acceptance.py", line 72, in main
    before[fn]=runtime(*args,fn);check_runtime(before[fn],original,expected[fn],count)
                                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6232_sec_search_research_acceptance.py", line 35, in check_runtime
    raise ValueError('Original runtime or cadence differs')
ValueError: Original runtime or cadence differs

```

## Log

