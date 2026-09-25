
**Status:** failure  
**Duration:** 0.3s  
**Finished:** 2026-09-25T07:28:38+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6061_sec_settlement_complete_archives.py", line 97, in main
    selected, retained, index, cutoff = plan(s3, baseline)
                                        ^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6061_sec_settlement_complete_archives.py", line 47, in plan
    selected = sec.advertised_archives(index, cutoff, 12)
               ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/checks/sec_ftd_inventory.py", line 51, in advertised_archives
    raise ValueError('Advertised half-month archive window has gaps')
ValueError: Advertised half-month archive window has gaps

```

## Log

