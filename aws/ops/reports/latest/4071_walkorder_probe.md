# ops 4071 — PROBE: walk order + source-map producer

**Status:** failure  
**Duration:** 0.2s  
**Finished:** 2026-07-29T02:28:38+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4071_walkorder_probe.py", line 88, in main
    r.row(unique_symbols=len(order), already_sourced=len(have),
    ^^^^^
AttributeError: 'Report' object has no attribute 'row'. Did you mean: 'rows'?

```

## Log
## H1 — where does the payoff sit in the queue?

