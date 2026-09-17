
**Status:** failure  
**Duration:** 210.3s  
**Finished:** 2026-09-17T23:27:12+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5701_reversal_pd_verify.py", line 42, in main
    assert datetime.fromisoformat(doc['generated_at'].replace('Z','+00:00'))>=started
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError

```

## Log

