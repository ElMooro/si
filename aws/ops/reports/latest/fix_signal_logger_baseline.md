# Fix signal-logger to capture baseline_price for every signal

**Status:** success  
**Duration:** 3.7s  
**Finished:** 2026-04-24T23:25:34+00:00  

## Data

| baseline_price_capture | new_signals | old_signals |
|---|---|---|
| now automatic for all signals | will all have baseline_price + baseline_benchmark_price | legacy 4,400 records can't be retroactively scored |

## Log
- `23:25:30` ✅   Inserted price-fetcher block at top of logger
- `23:25:30` ✅   Updated log_sig() to auto-fetch baseline_price + baseline_benchmark_price
- `23:25:30` ✅   Source valid (12777 bytes), saved
- `23:25:34` ✅   Deployed signal-logger (4,738 bytes)
## Trigger fresh signal-logger run with baseline_price capture

- `23:25:34` ✅   Async-triggered signal-logger (status 202)
- `23:25:34`   Next run will create signals WITH baseline_price for all signal types.
- `23:25:34`   When daily outcome-checker fires (cron(30 22 ? * MON-FRI *)),
- `23:25:34`   it will be able to score them properly.
- `23:25:34` Done
