# ops 4192 — MPRYY retry + bare census + wave-2 sizing

**Status:** failure  
**Duration:** 0.7s  
**Finished:** 2026-07-31T20:54:46+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4192_mpryy_census.py", line 54, in main
    attrs = list(dict(re.findall(
                 ^^^^^^^^^^^^^^^^
ValueError: dictionary update sequence element #0 has length 7; 2 is required

```

## Log
## A. MPRYY: PPI flow, freq variants, vintages

