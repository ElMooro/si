## 1. Function + schedule

**Status:** failure  
**Duration:** 58.1s  
**Finished:** 2026-07-11T03:25:30+00:00  

## Error

```
SystemExit: 1
```

## Data

| aapl | fn | fresh_fetched | n_data_suspect | n_fails | n_insider_buy | n_insider_sell | n_tickers | n_warns | nvda | schedule | top_bb | top_dil | verdict | warns_engine |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | updated |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  | exists |  |  |  |  |
|  |  | 68 |  |  |  |  | 65 |  |  |  |  |  |  | ["sell feed joined: data/insider-sell-cluster.json (0 names)", "no insider-sell doc found -- sells omitted honestly", "universe: 68 names (opp+ranker+soldiers+insider)"] |
| {"buyback_ttm_usd": 78196000000, "issuance_ttm_usd": 0, "sh_qoq_pct": -0.29, "sh_yoy_pct": -1.91, "buyback_yield_pct": 1.69, "as_of": "2026-07-11", "read": "SHRINKING"} |  |  |  |  |  |  |  |  | {"buyback_ttm_usd": 47432000000, "issuance_ttm_usd": 0, "sh_qoq_pct": -0.17, "sh_yoy_pct": -0.89, "buyback_yield_pct": 0.93, "as_of": "2026-07-11", "read": "NEUTRAL"} |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | [{"ticker": "LUV", "buyback_ttm_usd": 3050000000, "issuance_ttm_usd": 0, "sh_qoq_pct": -3.45, "sh_yoy_pct": -13.87, "buyback_yield_pct": 12.88, "as_of": "2026-07-11", "read": "BUYBACK_HEAVY"}, {"ticker": "CNXC", "buyback | [{"ticker": "WHLR", "buyback_ttm_usd": 0, "issuance_ttm_usd": 0, "sh_qoq_pct": 234.92, "sh_yoy_pct": 74502.03, "as_of": "2026-07-11", "insider_buy_usd_90d": 24558, "insider_n_buyers": 1, "read": "HEAVY_DILUTION"}, {"tick |  |  |
|  |  |  | 0 |  | 16 | 0 |  |  |  |  |  |  |  |  |
|  |  |  |  | 3 |  |  |  | 0 |  |  |  |  | FAIL |  |

## Log
## 2. Invoke + map truth

## 3. Four pages live (this-push markers)

## verdict

- `03:25:30` FAIL: map thin: 65 names (<250)
- `03:25:30` FAIL: insane values: ['VMAR', 'WHLR']
- `03:25:30` FAIL: sell side still empty -- management selling not tracked
