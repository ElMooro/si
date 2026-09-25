
**Status:** failure  
**Duration:** 3.4s  
**Finished:** 2026-09-25T07:49:02+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/staged/ops_6064_sec_reported_settlement_history.py", line 160, in main
    adopted_journals = adopt_july(s3, baseline_ref, selected, retained)
                       ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/staged/ops_6064_sec_reported_settlement_history.py", line 81, in adopt_july
    captured['inventory'] = sec.inventory(successful(s3, captured), JULY, captured['received_at'][:10])
                            ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/checks/sec_ftd_inventory.py", line 136, in inventory
    records = rows(body, url, cutoff)
              ^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/checks/sec_ftd_inventory.py", line 112, in rows
    raise ValueError(f'Invalid exact reported identity at line {line_number}')
ValueError: Invalid exact reported identity at line 27154

```

## Log

