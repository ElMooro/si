# ops 3165 — Thesis Engine

**Status:** success  
**Duration:** 50.4s  
**Finished:** 2026-07-12T21:27:19+00:00  

## Error

```
SystemExit: 0
```

## Data

| elapsed_s | n_fails | n_theses | n_warns | signals_logged | spy_base_21d | status | theses_firing_now | theses_with_significant_edge | verdict |
|---|---|---|---|---|---|---|---|---|---|
| 36.5 |  | 56 |  | 0 | 1.36 | LIVE |  |  |  |
|  |  |  |  |  |  |  | 24 | 34 |  |
|  | 0 |  | 1 |  |  |  |  |  | PASS |

## Log
## 1. Deploy

- `21:26:30` env keys: ['FRED_KEY', 'POLYGON_KEY', 'S3_BUCKET']
- `21:26:30`   zip: 58564 bytes
## 1. Lambda

- `21:26:30`   Lambda exists — updating
- `21:26:37` ✅   ✓ updated justhodl-thesis-engine
- `21:26:37` ✅   ✓ Function URL: https://e2kamt5p6takze7mxepzkp7bbi0lsqwq.lambda-url.us-east-1.on.aws/
## 2. EB rule + permissions

- `21:26:38` ✅   ✓ created rule thesis-engine-daily
- `21:26:38` ✅   ✓ target → justhodl-thesis-engine
- `21:26:38` ✅   ✓ added invoke permission
## 2. First run (cold: full history backfill)

- `21:26:38` async invoke fired — backfilling ~550 FRED + ~1,700 Polygon series, then event-studying every thesis
## 3. Results

- `21:27:19` ── STRONGEST LEADS (|t| on 21d forward SPY):
- `21:27:19`   Business Cycle                             act  25.0% ( 52.2p) | SPY21 excess   2.36% hit  92.2% t=  8.59 n=141
- `21:27:19`   Economy                                    act  44.8% ( 67.0p) | SPY21 excess   2.62% hit  96.9% t=  7.56 n=97
- `21:27:19`   Country ETFs                               act  45.5% ( 67.4p) | SPY21 excess   0.33% hit  81.8% t=  0.74 n=99
- `21:27:19`   Employment                                 act  66.7% ( 78.3p) | SPY21 excess   2.58% hit  82.5% t=  6.07 n=97
- `21:27:19`   EuroDollar banks                           act  77.8% ( 86.1p) | SPY21 excess  -0.51% hit  77.5% t= -2.23 n=120  ★FIRING
- `21:27:19`   Credit Spreads                             act  30.0% ( 80.7p) | SPY21 excess    1.9% hit  88.7% t=  6.41 n=141  ★FIRING
- `21:27:19`   Europe Stocks Indices                      act  25.0% ( 42.2p) | SPY21 excess  -0.53% hit  69.1% t= -1.59 n=149
- `21:27:19`   Fed Interest Rates                         act  37.5% ( 59.9p) | SPY21 excess   0.06% hit  78.7% t=  0.36 n=127
- `21:27:19`   Consumers                                  act  18.2% ( 53.5p) | SPY21 excess   1.48% hit  91.3% t=   6.2 n=127
- `21:27:19`   Financial Conditions                       act  36.4% ( 83.5p) | SPY21 excess   2.04% hit  90.3% t=  6.13 n=113  ★FIRING
- `21:27:19`   fed powell holding                         act  40.0% ( 55.3p) | SPY21 excess   0.54% hit  75.5% t=  1.87 n=143
- `21:27:19`   Credit Risk                                act  20.0% ( 76.7p) | SPY21 excess   1.91% hit  87.3% t=  5.51 n=110
- `21:27:19` ✅ 56 theses scored · 34 carry a statistically significant 21d lead (|t|>=2, n>=20) · 24 firing today
- `21:27:19` ── SIGNIFICANT (these are the ones that actually lead):
- `21:27:19`   · Business Cycle: risk-ON tell — SPY 21d +2.36% vs base, hit 92.2%, t=8.59, n=141
- `21:27:19`   · Economy: risk-ON tell — SPY 21d +2.62% vs base, hit 96.9%, t=7.56, n=97
- `21:27:19`   · Employment: risk-ON tell — SPY 21d +2.58% vs base, hit 82.5%, t=6.07, n=97
- `21:27:19`   · EuroDollar banks: risk-OFF tell — SPY 21d -0.51% vs base, hit 77.5%, t=-2.23, n=120
- `21:27:19`   · Credit Spreads: risk-ON tell — SPY 21d +1.90% vs base, hit 88.7%, t=6.41, n=141
- `21:27:19`   · Consumers: risk-ON tell — SPY 21d +1.48% vs base, hit 91.3%, t=6.2, n=127
- `21:27:19`   · Financial Conditions: risk-ON tell — SPY 21d +2.04% vs base, hit 90.3%, t=6.13, n=113
- `21:27:19`   · Credit Risk: risk-ON tell — SPY 21d +1.91% vs base, hit 87.3%, t=5.51, n=110
## 4. Page

- `21:27:19` ⚠ page: HTTP Error 404: Not Found
