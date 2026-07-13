# ops 3193 — DERIVED on-chain composites (computed from free primaries)

**Status:** success  
**Duration:** 18.5s  
**Finished:** 2026-07-13T02:55:56+00:00  

## Data

| coverage_before | coverage_now | curated_total | n_fails | n_warns | new_derived | onchain_unmapped | probed | pruned | survivors | verdict |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  | 188 |  |  |  |  |
|  |  |  |  |  | 1 |  |  |  |  |  |
|  |  |  |  |  |  |  | 1 | 0 | 1 |  |
| 75.9 | 76.1 | 22 |  |  |  |  |  |  |  |  |
|  |  |  | 0 | 0 |  |  |  |  |  | PASS |

## Log
## 1. Token census of the unmapped on-chain tiles

- `02:55:38`   RETAILPERCENTAGE               8
- `02:55:38`   RETAIL                         7
- `02:55:38`   WHALES                         7
- `02:55:38`   WHALESPERCENTAGE               7
- `02:55:38`   UNISWAPLIQUIDITY               6
- `02:55:38`   INVESTORS                      6
- `02:55:38`   WHALESASSETS                   6
- `02:55:38`   INVESTORSASSETS                5
- `02:55:38`   INVESTORSPERCENTAGE            5
- `02:55:38`   RETAILASSETS                   5
- `02:55:38`   BEARSVOLUME                    5
- `02:55:38`   NEWADDRESSES                   4
- `02:55:38`   RECEIVINGADDRESSES             4
- `02:55:38`   UNISWAPLIQUIDITYUSD            4
- `02:55:38`   VOLATILITY60                   4
- `02:55:38`   AVGBALANCE                     4
- `02:55:38`   BULLSVOLUME                    4
- `02:55:38`   TRADERS                        4
- `02:55:38`   TXVOLUMEUSD                    4
- `02:55:38`   ATHDRAWDOWN                    3
## 2. Re-map + probe the DERIVED entries

- `02:55:39`     ✓ GLASSNODE:BTC_ATHDRAWDOWN → COINMETRICS~btc|PriceUSD~drawdown_ath  (5839 pts)
## 3. Write + redeploy + kick

- `02:55:40`   zip: 75486 bytes
## 1. Lambda

- `02:55:40`   Lambda exists — updating
- `02:55:46` ✅   ✓ updated justhodl-wl-engines
## 2. EB rule + permissions

- `02:55:47`   rule already correct: wl-engines-daily (cron(30 22 ? * TUE-SAT *))
- `02:55:47` ✅   ✓ target → justhodl-wl-engines
- `02:55:47` ✅   ✓ added invoke permission
- `02:55:47`   zip: 77105 bytes
## 1. Lambda

- `02:55:47`   Lambda exists — updating
- `02:55:51` ✅   ✓ updated justhodl-thesis-engine
## 2. EB rule + permissions

- `02:55:51`   rule already correct: thesis-engine-daily (cron(45 22 ? * TUE-SAT *))
- `02:55:51` ✅   ✓ target → justhodl-thesis-engine
- `02:55:51` ✅   ✓ added invoke permission
- `02:55:52`   zip: 73146 bytes
## 1. Lambda

- `02:55:52`   Lambda exists — updating
- `02:55:55` ✅   ✓ updated justhodl-symbol-dictionary
## 2. EB rule + permissions

- `02:55:56`   rule already correct: symbol-dictionary-weekly (cron(0 5 ? * SUN *))
- `02:55:56` ✅   ✓ target → justhodl-symbol-dictionary
- `02:55:56` ✅   ✓ added invoke permission
