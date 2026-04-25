# Phase 2B — Asymmetric Reward/Risk Equity Scorer

**Status:** success  
**Duration:** 8.6s  
**Finished:** 2026-04-25T16:16:37+00:00  

## Data

| invoke_s | n_setups | n_value_traps | top_5 | zip_size |
|---|---|---|---|---|
| 1.1 | 17 | 8 | ['INCY', 'FSLR', 'RMD', 'DECK', 'PTC'] | 15692 |

## Log
## 1. Verify screener data dependency

- `16:16:28`   screener/data.json: 503 stocks under key 'stocks'
- `16:16:28`   Sample fields: ['altmanZ', 'beta', 'chg1d', 'chg1m', 'chg1w', 'chg1y', 'chg3m', 'chg6m', 'currentRatio', 'debtToEquity', 'dividendYield', 'epsGrowth', 'evEbitda', 'fcfGrowth', 'grossMargin']...
## 2. Set up justhodl-asymmetric-scorer Lambda

- `16:16:28` ✅   Wrote source: 15,523B, 417 LOC
- `16:16:32` ✅   Created justhodl-asymmetric-scorer
## 3. Test invoke

- `16:16:36` ✅   Invoked in 1.1s
- `16:16:36` 
  Response body:
- `16:16:36`     n_setups                  17
- `16:16:36`     n_value_traps             8
- `16:16:36`     n_new_this_week           17
- `16:16:36`     n_dropped_this_week       0
- `16:16:36`     top_5_symbols             ['INCY', 'FSLR', 'RMD', 'DECK', 'PTC']
- `16:16:36`     sector_breakdown          {'Healthcare': 3, 'Energy': 1, 'Consumer Cyclical': 1, 'Technology': 5, 'Communication Services': 2, 'Real Estate': 1, 'Industrials': 2, 'Financial Services': 2}
## 4. Read opportunities/asymmetric-equity.json

- `16:16:36`   Screener total: 503
- `16:16:36`   Passed quality gate: 77
- `16:16:36`   High-conviction setups: 17
- `16:16:36`   Value traps tracked: 8
- `16:16:36`   Failures by reason: {'debt_high': 27, 'fcf_negative': 37, 'piotroski_low': 20, 'liquidity_weak': 26, 'no_earnings': 1, 'price_too_low': 315}
- `16:16:36` 
  Sector breakdown of setups:
- `16:16:36`     Technology                     5
- `16:16:36`     Healthcare                     3
- `16:16:36`     Communication Services         2
- `16:16:36`     Industrials                    2
- `16:16:36`     Financial Services             2
- `16:16:36`     Energy                         1
- `16:16:36`     Consumer Cyclical              1
- `16:16:36`     Real Estate                    1
- `16:16:36` 
  Top 10 setups:
- `16:16:36`     INCY   Incyte Corporation             sector=Healthcare      q= 86.2 sf= 99.7 v= 98.6 m= 88.9 composite= 93.3 (4/4)
- `16:16:36`     FSLR   First Solar, Inc.              sector=Energy          q= 82.7 sf= 98.8 v= 97.0 m= 77.8 composite= 89.1 (4/4)
- `16:16:36`     RMD    ResMed Inc.                    sector=Healthcare      q= 90.4 sf= 96.5 v= 77.1 m= 74.9 composite= 84.7 (3/4)
- `16:16:36`     DECK   Deckers Outdoor Corporation    sector=Consumer Cyclic q= 80.8 sf= 97.1 v= 95.2 m= 58.0 composite= 82.8 (3/4)
- `16:16:36`     PTC    PTC Inc.                       sector=Technology      q= 91.2 sf= 65.0 v= 97.9 m= 74.3 composite= 82.1 (3/4)
- `16:16:36`     DXCM   DexCom, Inc.                   sector=Healthcare      q= 77.5 sf= 84.8 v= 79.1 m= 86.0 composite= 81.8 (3/4)
- `16:16:36`     FOXA   Fox Corporation                sector=Communication S q= 59.4 sf= 81.4 v=100.0 m= 86.6 composite= 81.8 (3/4)
- `16:16:36`     FOX    Fox Corporation                sector=Communication S q= 59.4 sf= 81.4 v=100.0 m= 86.6 composite= 81.8 (3/4)
- `16:16:36`     TTD    The Trade Desk, Inc.           sector=Technology      q= 64.1 sf= 83.1 v=100.0 m= 64.4 composite= 77.9 (3/4)
- `16:16:36`     JKHY   Jack Henry & Associates, Inc.  sector=Technology      q= 78.7 sf= 78.2 v=100.0 m= 53.9 composite= 77.7 (3/4)
## 5. Schedule cron(30 13 ? * MON-FRI *)

- `16:16:37` ✅   Created rule cron(30 13 ? * MON-FRI *)
- `16:16:37` ✅   Added invoke permission
- `16:16:37` Done
