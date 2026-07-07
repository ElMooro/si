## 1. Wait for gap-metrics code deploy

**Status:** failure  
**Duration:** 175.4s  
**Finished:** 2026-07-07T21:26:12+00:00  

## Error

```
SystemExit: 1
```

## Data

| body | credit_desk_live | degraded | deploy_age_s | env_vars | fn_error | invoke_seconds | missing | modules_ok | ok | pages_wired_live | public_stock_bond |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | 2 | 4 |  |  |  |  |  |  |  |
| {"statusCode": 200, "body": "{\"ok\": 8, \"total\": 11, \"elapsed_s\": 5.4}"} |  |  |  |  | None | 6.3 |  |  |  |  |  |
|  |  | {"revision_breadth": "only 2 signed rows", "miner_margin": "only 0 miners parsed; errors: NEM: HTTP Error 403: Forbidden; GOLD: HTTP Error 403: Forbidden; AEM: HTTP Error 403: Forbidden", "cor3m": "no implied-corr symbol returned >=60 closes (tried COR3M/COR90D/COR1M/COR30D)"} |  |  |  |  |  | 8 | ['baltic_dry', 'bill_share', 'em_carry', 'global_m2', 'muni_ratio', 'ofr_fsi', 'sloos', 'stock_bond_corr'] |  |  |
|  |  |  |  |  |  |  | ['lce.html', 'correlation.html', 'global-macro.html', 'ofr.html'] |  |  | 6 |  |
|  | True |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | POSITIVE |

## Log
## 2. Re-invoke

## 3. Module verify (post-fix)

## 4. Live pages (runner-side)

- `21:26:12` FAILS=1 WARNS=3
