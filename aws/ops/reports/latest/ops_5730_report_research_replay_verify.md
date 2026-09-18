- `22:03:50` Exact commit/runtime verified: 6eddd83d5daf6f29463fae72822fb651a8c7f282
**Status:** failure  
**Duration:** 460.5s  
**Finished:** 2026-09-18T22:11:30+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 98, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_5730_report_research_replay_verify.py", line 50, in main
    row=packet['measurements'][sid]
        ~~~~~~~~~~~~~~~~~~~~~~^^^^^
KeyError: 'CPIAUCSL'

```

## Log

