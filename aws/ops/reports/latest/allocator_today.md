# Allocator headline

**Status:** failure  
**Duration:** 0.2s  
**Finished:** 2026-05-04T19:04:39+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/_allocator_today.py", line 45, in main
    for ru in rules[:15]:
              ~~~~~^^^^^
KeyError: slice(None, 15, None)

```

## Log
- `19:04:39`   generated_at: 2026-05-04T19:02:30.378531+00:00
- `19:04:39`   regime_headline: BALANCED_NEUTRAL
- `19:04:39`   rules: 10 of 10
- `19:04:39`   cash_buffer_pct: 20.0
# Recommended weights (sorted)

- `19:04:39`   QQQ              32.90%
- `19:04:39`   SPY              20.40%
- `19:04:39`   CASH             20.00%
- `19:04:39`   DBC              15.70%
- `19:04:39`   EEM               7.80%
- `19:04:39`   GLD               3.10%
# Overweights

- `19:04:39`   QQQ
# Underweights

- `19:04:39`   TLT
# Asset scores (composite)

# Rule results

- `19:04:39`   total rules: 10
