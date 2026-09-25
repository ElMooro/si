
**Status:** failure  
**Duration:** 42.3s  
**Finished:** 2026-09-25T02:06:09+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6040_offexchange_complete_partitions.py", line 118, in main
    assert not errors,errors
           ^^^^^^^^^^
AssertionError: {'monthlySummary/OTC_M_SMBL_FIRM/2026-06-01/NMS': {'type': 'ValueError', 'reason': 'Duplicate grain across source pages'}}

```

## Data

| capture_failures |
|---|
| {'monthlySummary/OTC_M_SMBL_FIRM/2026-06-01/NMS': {'type': 'ValueError', 'reason': 'Duplicate grain across source pages'}} |

## Log

