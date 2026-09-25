
**Status:** failure  
**Duration:** 0.9s  
**Finished:** 2026-09-25T17:20:49+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6110_accounting_completed_stage_acceptance.py", line 73, in main
    proof=completed_proof(previous,ready,Path(prior.__file__).read_bytes())
          ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6110_accounting_completed_stage_acceptance.py", line 49, in completed_proof
    if type(value) not in (int,float) or not math.isfinite(value) or value<=0:raise ValueError('Completed verified stage required')
                                                                              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ValueError: Completed verified stage required

```

## Log

