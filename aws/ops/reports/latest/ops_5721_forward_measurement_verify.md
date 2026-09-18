- `19:23:09` justhodl-signal-harvester exact commit and runtime hash f310f09346bfd587b45865d7d8c3809efbdb6524
- `19:23:10` justhodl-prospective-evaluator exact commit and runtime hash f310f09346bfd587b45865d7d8c3809efbdb6524
**Status:** failure  
**Duration:** 32.7s  
**Finished:** 2026-09-18T19:23:41+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5721_forward_measurement_verify.py", line 47, in main
    invoke('justhodl-signal-harvester',{'capture_only':True,'suppress_alerts':True})
  File "/home/runner/work/si/si/aws/ops/pending/ops_5721_forward_measurement_verify.py", line 28, in invoke
    assert not result.get('FunctionError') and payload.get('statusCode',200)<400,fn+' invoke failed'
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
AssertionError: justhodl-signal-harvester invoke failed

```

## Log

