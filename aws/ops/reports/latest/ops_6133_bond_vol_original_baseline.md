
**Status:** failure  
**Duration:** 0.6s  
**Finished:** 2026-09-26T05:11:45+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6133_bond_vol_original_baseline.py", line 124, in main
    before = runtime(lam, s3, events, scheduler, FUNCTION)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6133_bond_vol_original_baseline.py", line 95, in runtime
    inventory=package_inventory(raw,expected)
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6133_bond_vol_original_baseline.py", line 72, in package_inventory
    if len(set(names))!=len(names):raise ValueError('Ambiguous duplicate ZIP member')
                                   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ValueError: Ambiguous duplicate ZIP member

```

## Log

