
**Status:** failure  
**Duration:** 8.8s  
**Finished:** 2026-09-26T04:01:14+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6129_term_premium_original_baseline.py", line 118, in main
    for path in sorted(paths): progress['repo_predecessors'][path] = retain(s3, (ROOT / path).read_bytes())
                                                                     ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6129_term_premium_original_baseline.py", line 31, in retain
    raise ValueError('Complete bounded predecessor required')
ValueError: Complete bounded predecessor required

```

## Log

