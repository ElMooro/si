
**Status:** failure  
**Duration:** 2.9s  
**Finished:** 2026-09-26T13:52:06+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6151_share_flows_normal_publication_replay.py", line 57, in main
    execution = native_execution(s3, ref)
                ^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6151_share_flows_normal_publication_replay.py", line 29, in native_execution
    if scanned > 1000: raise ValueError('Research request inventory bound exceeded')
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ValueError: Research request inventory bound exceeded

```

## Log

