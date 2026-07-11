## 1. Race-guarded deploys (both functions)

**Status:** success  
**Duration:** 67.1s  
**Finished:** 2026-07-11T03:34:38+00:00  

## Data

| aapl | boards | fresh_fetched | insider_trades_sell_rows | justhodl-insider-trades | justhodl-share-flows | n_fails | n_flagged | n_insider_buy_joined | n_insider_sell_joined | n_sbc | n_tickers | n_v12_rows | n_warns | verdict | version | warns_engine |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  | code=repo (race-guarded) |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | code=repo (race-guarded) |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | 4 |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  | 395 |  |  |  |  |  |  |  |  | 619 |  |  |  | 1.2.1 | ["insider join: 2 buy names / 3 sell names (insider-trades tape)", "universe: 1113 names (opp+ranker+soldiers+insider+phase-ring+valuations)", "fresh 395 / needed 1113 (budget 420)"] |
|  |  |  |  |  |  |  | 17 | 0 | 1 | 336 |  | 395 |  |  |  |  |
| {"buyback_ttm_usd": 78196000000, "issuance_ttm_usd": 0, "buyback_net_ttm_usd": 78196000000, "sbc_ttm_usd": 13473000000, "div_ttm_usd": 15550000000, "sh_qoq_pct": -0.29, "sh_yoy_pct": -1.91, "sh_3y_cagr_pct": -2.32, "buyback_yield_pct": 1.69, "buyback_net_yield_pct": 1.69, "sbc_pct_mcap": 0.29, "total_shareholder_yield_pct": 2.02, "as_of": "2026-07-11", "read": "SHRINKING"} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | {'top_buybacks': 20, 'sbc_washers': 0, 'mgmt_selling_into_buyback': 0, 'top_diluters': 20, 'extreme_diluters': 0, 'insider_conviction': 0} |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  | 0 |  |  |  |  |  |  | 1 | PASS |  |  |

## Log
## 2. Insider tape: sells at source

## 3. share-flows v1.2.1 run + gates

## 4. Pages live

## verdict

- `03:34:38` PASS -- share-flows v1.2.1 live end to end
