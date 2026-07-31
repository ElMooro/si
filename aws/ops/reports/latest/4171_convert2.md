# ops 4165 — convert the chewed queue

**Status:** failure  
**Duration:** 0.0s  
**Finished:** 2026-07-31T18:07:41+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4171_convert2.py", line 68, in main
    checksA = settle(rep, "justhodl-tradingview",
              ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/pending/ops_4171_convert2.py", line 28, in settle
    assert mark in src
           ^^^^^^^^^^^
AssertionError

```

## Log

