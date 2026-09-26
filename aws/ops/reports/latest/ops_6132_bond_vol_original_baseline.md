
**Status:** failure  
**Duration:** 1.3s  
**Finished:** 2026-09-26T05:06:35+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6132_bond_vol_original_baseline.py", line 75, in main
    before = runtime(lam, s3, events, scheduler, FUNCTION)
             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/checks/market_runtime_evidence.py", line 75, in runtime
    if archive.read(name)!=path.read_bytes():raise ValueError('Actual packaged source differs: '+name)
                                             ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
ValueError: Actual packaged source differs: _fred_shim.py

```

## Log

