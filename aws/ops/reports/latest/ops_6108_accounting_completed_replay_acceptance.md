
**Status:** failure  
**Duration:** 0.6s  
**Finished:** 2026-09-25T17:16:22+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6108_accounting_completed_replay_acceptance.py", line 48, in main
    assert previous['qualification']==ready['qualification'] and original['status']=='complete'
           ~~~~~~~~^^^^^^^^^^^^^^^^^
KeyError: 'qualification'

```

## Log

