## 1. Wait for gap-metrics code deploy

**Status:** failure  
**Duration:** 11.3s  
**Finished:** 2026-07-07T21:32:23+00:00  

## Error

```
SystemExit: 1
```

## Data

| body | credit_desk_live | degraded | deploy_age_s | env_vars | fn_error | invoke_seconds | missing | modules_ok | ok | pages_wired_live | public_stock_bond |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | 2 | 4 |  |  |  |  |  |  |  |
| {"statusCode": 200, "body": "{\"ok\": 8, \"total\": 11, \"elapsed_s\": 7.7}"} |  |  |  |  | None | 8.6 |  |  |  |  |  |
|  |  | {"revision_breadth": "only 2 signed rows of 40 in feed (universe too thin for market-level breadth)", "miner_margin": "only 0 miners parsed; errors: none", "cor3m": "no implied-corr symbol returned >=60 closes (tried COR3M/COR90D/COR1M/COR30D)"} |  |  |  |  |  | 8 | ['baltic_dry', 'bill_share', 'em_carry', 'global_m2', 'muni_ratio', 'ofr_fsi', 'sloos', 'stock_bond_corr'] |  |  |
|  |  |  |  |  |  |  | [] |  |  | 10 |  |
|  | True |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | POSITIVE |

## Log
## 2. Re-invoke

## 3. Module verify (post-fix)

## 4. Live pages (runner-side)

- `21:32:23` FAILS=1 WARNS=3
