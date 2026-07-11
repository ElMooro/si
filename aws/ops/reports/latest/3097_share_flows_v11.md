## 1. Function + schedule

**Status:** failure  
**Duration:** 34.9s  
**Finished:** 2026-07-11T03:27:41+00:00  

## Error

```
SystemExit: 1
```

## Data

| aapl | fn | fresh_fetched | n_data_suspect | n_fails | n_insider_buy | n_insider_sell | n_tickers | n_warns | nvda | schedule | top_bb | top_dil | verdict | warns_engine |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | updated |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | exists |  |  |  |  |
|  |  | 82 |  |  |  |  | 366 |  |  |  |  |  |  | ["insider join: 1 buy names / 0 sell names (insider-trades tape)", "no sell rows in tape -- sells omitted honestly", "universe: 448 names (opp+ranker+soldiers+insider+phase-ring+valuations)"] |
| {"buyback_ttm_usd": 78196000000, "issuance_ttm_usd": 0, "sh_qoq_pct": -0.29, "sh_yoy_pct": -1.91, "buyback_yield_pct": 1.69, "as_of": "2026-07-11", "read": "SHRINKING"} |  |  |  |  |  |  |  |  | {"buyback_ttm_usd": 47432000000, "issuance_ttm_usd": 0, "sh_qoq_pct": -0.17, "sh_yoy_pct": -0.89, "buyback_yield_pct": 0.93, "as_of": "2026-07-11", "read": "NEUTRAL"} |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | [{"ticker": "CRM", "buyback_ttm_usd": 37211000000, "issuance_ttm_usd": 0, "sh_qoq_pct": -7.34, "sh_yoy_pct": -10.21, "buyback_yield_pct": 27.82, "as_of": "2026-07-11", "read": "BUYBACK_HEAVY"}, {"ticker": "IT", "buyback_ | [{"ticker": "AVAV", "buyback_ttm_usd": 0, "issuance_ttm_usd": 0, "sh_qoq_pct": 0.67, "sh_yoy_pct": 73.67, "as_of": "2026-07-11", "read": "HEAVY_DILUTION"}, {"ticker": "QXO", "buyback_ttm_usd": 28100000, "issuance_ttm_usd |  |  |
|  |  |  | 8 |  | 0 | 0 |  |  |  |  |  |  |  |  |
|  |  |  |  | 1 |  |  |  | 0 |  |  |  |  | FAIL |  |

## Log
## 2. Invoke + map truth

## 3. Four pages live (this-push markers)

## verdict

- `03:27:41` FAIL: sell side still empty -- management selling not tracked
