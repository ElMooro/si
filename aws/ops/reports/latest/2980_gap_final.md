## 1. Race-safe deploy wait

**Status:** failure  
**Duration:** 90.9s  
**Finished:** 2026-07-07T21:59:05+00:00  

## Error

```
SystemExit: 1
```

## Data

| body | degraded | deploy_age_s | env_vars | fn_error | implied_corr | invoke_seconds | modules_ok | ok | pctile | source |
|---|---|---|---|---|---|---|---|---|---|---|
|  |  | 61 | 4 |  |  |  |  |  |  |  |
| {"statusCode": 200, "body": "{\"ok\": 10, \"total\": 11, \"elapsed_s\": 14.4}"} |  |  |  | None |  | 15.2 |  |  |  |  |
|  | {"revision_breadth": "strategist: strategist revisions thin (0 signed of 0) | stocks: WARMING_UP: snapshot history 0d of 21d needed (60 names captured today)"} |  |  |  |  |  | 10 | ['baltic_dry', 'bill_share', 'cor3m', 'em_carry', 'global_m2', 'miner_margin', 'muni_ratio', 'ofr_fsi', 'sloos', 'stock_bond_corr'] |  |  |
|  |  |  |  |  | 7.63 |  |  |  | 0.0 | CBOE _COR3M |

## Log
## 2. Invoke

## 3. Verify 10+/11 with the two fixes

- `21:59:05` FAILS=1 WARNS=0
