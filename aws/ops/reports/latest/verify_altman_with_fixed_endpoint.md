# Verify Altman Z populated after endpoint fix

**Status:** success  
**Duration:** 540.5s  
**Finished:** 2026-04-26T00:14:04+00:00  

## Data

| altman_n | altman_pct | n_stocks |
|---|---|---|
| 494 | 98.2 | 503 |

## Log
## A. Confirm Lambda deployed

- `00:05:04` ✅   Deployed: 2026-04-26T00:05:00
## B. Async-invoke screener

- `00:05:04`   Pre-mtime: 2026-04-25 23:45:10+00:00
- `00:05:04` ✅   Queued (StatusCode=202)
## C. Wait 9 min for screener to complete

## D. Coverage check

- `00:14:04`   Post-mtime: 2026-04-26 00:09:02+00:00
- `00:14:04`   Cache updated: True
- `00:14:04` 
- `00:14:04`   Total stocks: 503
- `00:14:04`   peRatio: 469/503 (93.2%)
- `00:14:04`   sma50:   499/503 (99.2%)
- `00:14:04`   altmanZ: 494/503 (98.2%)  ← was 0
- `00:14:04` ✅ 
  ✅ Altman Z fix worked — 98.2% coverage
- `00:14:04` 
  Top 5 safest:
- `00:14:04`     UNP    Industrials          Z=2188.95
- `00:14:04`     PLTR   Technology           Z=140.73
- `00:14:04`     TPL    Energy               Z=112.04
- `00:14:04`     MPWR   Technology           Z= 75.22
- `00:14:04`     NVDA   Technology           Z= 66.19
- `00:14:04` 
  Bottom 5 (potential distress):
- `00:14:04`     KEY    Financial Services   Z= -0.27  Distress
- `00:14:04`     WFC    Financial Services   Z= -0.32  Distress
- `00:14:04`     PNC    Financial Services   Z= -0.32  Distress
- `00:14:04`     SATS   Technology           Z= -0.73  Distress
- `00:14:04`     VRSN   Technology           Z=-13.01  Distress
- `00:14:04` Done
