# Force-invoke screener to populate new SMA+cross fields

**Status:** success  
**Duration:** 18.4s  
**Finished:** 2026-04-25T23:05:25+00:00  

## Data

| elapsed_min | n_death | n_golden | n_no_cross | n_stocks | sma200_coverage | sma50_coverage |
|---|---|---|---|---|---|---|
| 0.3 | 39 | 14 | 450 | 503 | 146 | 147 |

## Log
## A. Verify Lambda deployment

- `23:05:07`   CodeSha256: 1sejBZVi1bHMM7ih...
- `23:05:07`   LastModified: 2026-04-25T23:04:24
- `23:05:07` ✅   ✅ Lambda recently deployed
## B. Force-invoke screener (5-7 min expected)

- `23:05:25` ✅   Invoked in 18.1s (0.3 min)
- `23:05:25`   Response: count=503 elapsed=17.3s
## C. Verify new fields present in screener/data.json

- `23:05:25`   Total stocks in cache: 503
- `23:05:25`   Fields on first stock: ['altmanZ', 'beta', 'chg1d', 'chg1m', 'chg1w', 'chg1y', 'chg3m', 'chg6m', 'crossDaysAgo', 'crossSignal', 'currentRatio', 'debtToEquity', 'dividendYield', 'epsGrowth', 'evEbitda', 'fcfGrowth', 'grossMargin', 'industry', 'instChgPct', 'instHolders', 'instSignal', 'interestCoverage', 'marketCap', 'name', 'netMargin', 'operatingMargin', 'pbRatio', 'peRatio', 'piotroski', 'price']
- `23:05:25` ✅   ✅ All 4 new fields present on every stock
## D. Cross-signal distribution across S&P 500

- `23:05:25`   Stocks with sma50:  147/503
- `23:05:25`   Stocks with sma200: 146/503
- `23:05:25` 
- `23:05:25`   🟢 GOLDEN crosses (last 60d): 14
- `23:05:25`   🔴 DEATH crosses  (last 60d): 39
- `23:05:25`   ⚪ No cross signal:           450
## E. Top examples by signal

- `23:05:25` 
  Most recent GOLDEN crosses:
- `23:05:25`     EG     Financial Services     2d ago  $sma50=333.07  $sma200=332.77  $price=343.38
- `23:05:25`     ABNB   Consumer Cyclical      9d ago  $sma50=131.06  $sma200=128.26  $price=142.82
- `23:05:25`     DELL   Technology            21d ago  $sma50=160.59  $sma200=139.7  $price=216.04
- `23:05:25`     LYV    Communication Serv    33d ago  $sma50=157.41  $sma200=151.8  $price=156.64
- `23:05:25`     EQT    Energy                41d ago  $sma50=61.14  $sma200=56.09  $price=58.91
- `23:05:25` 
  Most recent DEATH crosses:
- `23:05:25`     AMCR   Consumer Cyclical      1d ago  $sma50=42.92  $sma200=43.04  $price=38.95
- `23:05:25`     WSM    Consumer Cyclical      2d ago  $sma50=192.53  $sma200=193.11  $price=190.54
- `23:05:25`     AOS    Industrials            3d ago  $sma50=69.23  $sma200=69.94  $price=64.38
- `23:05:25`     CRH    Basic Materials        9d ago  $sma50=112.18  $sma200=114.59  $price=118.0
- `23:05:25`     EXE    Energy                 9d ago  $sma50=104.15  $sma200=105.29  $price=96.44
- `23:05:25` Done — refresh justhodl.ai/screener/ to see new column + tabs
