# Create/update justhodl-backtest-harness Lambda + EB rule

**Status:** success  
**Duration:** 31.8s  
**Finished:** 2026-04-27T22:02:06+00:00  

## Log
- `22:01:34`   zip: 5407 bytes
## 1. Lambda

- `22:01:34`   Lambda missing — creating
- `22:01:39` ✅   ✓ created justhodl-backtest-harness
- `22:01:39` ✅   ✓ reserved concurrency = 1
## 2. EB rule + permissions

- `22:01:39` ✅   ✓ created rule justhodl-backtest-harness-daily
- `22:01:39` ✅   ✓ target → justhodl-backtest-harness
- `22:01:40` ✅   ✓ added invoke permission
## 3. Smoke test

- `22:01:40`   invoking justhodl-backtest-harness…
- `22:02:06` ✅   ✓ smoke test passed
- `22:02:06`     ok                       True
- `22:02:06`     n_signal_types_in_summary 4
