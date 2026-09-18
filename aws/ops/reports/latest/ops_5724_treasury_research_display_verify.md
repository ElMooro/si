
**Status:** failure  
**Duration:** 7.1s  
**Finished:** 2026-09-18T19:52:09+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5724_treasury_research_display_verify.py", line 44, in main
    if row['instrument_kind'] in ('TIPS','FRN','UNKNOWN'):assert row['tail_bp'] is None
       ~~~^^^^^^^^^^^^^^^^^^^
KeyError: 'instrument_kind'

```

## Log

