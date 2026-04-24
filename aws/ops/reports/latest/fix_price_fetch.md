# THE FIX — Replace broken price fetchers in outcome-checker

**Status:** success  
**Duration:** 4.0s  
**Finished:** 2026-04-24T23:17:41+00:00  

## Data

| backfill_triggered | deployed | fix |
|---|---|---|
| True | True | replaced /v2/last/trade and /v3/quote-short with /prev, /stable/quote, CoinGecko |

## Log
- `23:17:37` ✅   Source valid (13718 bytes), saved
- `23:17:41` ✅   Deployed justhodl-outcome-checker (4,187 bytes)
## Trigger fresh backfill with working price fetchers

- `23:17:41` ✅   Async-triggered outcome-checker (status 202)
- `23:17:41`   This run should actually score outcomes correctly now.
- `23:17:41` Done
