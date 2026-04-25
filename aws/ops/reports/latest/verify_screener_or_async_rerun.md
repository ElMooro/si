# Verify screener cache or async-rerun if stale

**Status:** success  
**Duration:** 0.7s  
**Finished:** 2026-04-25T23:19:44+00:00  

## Error

```
SystemExit: 0
```

## Data

| n_death | n_golden | ran_async | sma200_pct | sma50_pct |
|---|---|---|---|---|
| 98 | 66 | False | 98.6 | 98.8 |

## Log
## A. Current screener/data.json mtime

- `23:19:43`   LastModified: 2026-04-25 23:17:04 UTC
- `23:19:43`   Age: 2.7 min
- `23:19:43` ✅   ✅ Cache updated since rerun started (Lambda completed despite client timeout)
## B. Coverage check (current cache)

- `23:19:44`   Total stocks: 503
- `23:19:44`   name:    494/503 (98.2%)
- `23:19:44`   peRatio: 463/503 (92.0%)
- `23:19:44`   sma50:   497/503 (98.8%)
- `23:19:44`   sma200:  496/503 (98.6%)
- `23:19:44`   GOLDEN:  66
- `23:19:44`   DEATH:   98
- `23:19:44` ✅ 
  ✅ SMA coverage at 98.8% — fix worked, no rerun needed
- `23:19:44` Done
