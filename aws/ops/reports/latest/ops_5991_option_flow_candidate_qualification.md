
**Status:** failure  
**Duration:** 205.0s  
**Finished:** 2026-09-21T13:15:48+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_5991_option_flow_candidate_qualification.py", line 125, in main
    try:result=store.run(s3,BUCKET,REQUEST,'runner-ops-5991',credential=secret,remaining_seconds=900,publish_current=False)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/shared/option_flow_store.py", line 301, in run
    raise RuntimeError('Native option research failed; inspect retained request evidence') from None
RuntimeError: Native option research failed; inspect retained request evidence

```

## Log

