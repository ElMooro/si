# Trigger screener async + verify Altman Z

**Status:** success  
**Duration:** 541.5s  
**Finished:** 2026-04-25T23:50:28+00:00  

## Data

| altman_n | altman_pct | cache_updated | n_stocks | sma50_pct |
|---|---|---|---|---|
| 0 | 0.0 | True | 503 | 99.2 |

## Log
## A. Current cache state

- `23:41:28`   Pre-mtime: 2026-04-25 23:27:12+00:00
- `23:41:28`   Pre-altmanZ populated: 0/503
## B. Async-invoke screener with force=true

- `23:41:28` ✅   Queued (StatusCode=202)
## C. Wait 9 minutes (single block, no polling)

- `23:50:28`   Slept 540s
## D. Post state

- `23:50:28`   Post-mtime: 2026-04-25 23:45:10+00:00
- `23:50:28`   Cache updated: True
- `23:50:28` 
- `23:50:28`   Total stocks: 503
- `23:50:28`   name:    499/503 (99.2%)
- `23:50:28`   peRatio: 468/503 (93.0%)
- `23:50:28`   sma50:   499/503 (99.2%)
- `23:50:28`   altmanZ: 0/503 (0.0%)  ← was 0 before fix
- `23:50:28` ✗ 
  ❌ Altman Z still null — fix didn't take
- `23:50:28` Done
