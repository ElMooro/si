## 1. Race-safe deploy wait

**Status:** success  
**Duration:** 91.7s  
**Finished:** 2026-07-07T22:04:40+00:00  

## Data

| basis | body | breadth | breadth_mode | degraded | deploy_age_s | env_vars | fn_error | implied_corr | invoke_seconds | modules_ok | notes | ok | pctile | snapshot_day | snapshot_names | source | strategist | warmup |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  | 64 | 4 |  |  |  |  |  |  |  |  |  |  |  |  |
|  | {"statusCode": 200, "body": "{\"ok\": 11, \"total\": 11, \"elapsed_s\": 15.0}"} |  |  |  |  |  | None |  | 16.3 |  |  |  |  |  |  |  |  |  |
|  |  |  |  | {} |  |  |  |  |  | 11 |  | ['baltic_dry', 'bill_share', 'cor3m', 'em_carry', 'global_m2', 'miner_margin', 'muni_ratio', 'ofr_fsi', 'revision_breadth', 'sloos', 'stock_bond_corr'] |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 7.94 |  |  |  |  | 0.2 |  |  | CBOE _COR3M |  |  |
| None |  | None |  |  |  |  |  |  |  |  | ["strategist: strategist revisions thin (0 signed of 0)", "stocks: WARMING_UP: snapshot history 0d of 21d needed (60 names captured today)"] |  |  |  |  |  | null |  |
|  |  |  | WARMING_UP |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"captured_names": 60, "days_done": 0, "days_needed": 21, "eta": "2026-07-28"} |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-07-07 | 60 |  |  |  |

## Log
## 2. Invoke

## 3. Verify 10+/11 with the two fixes

- `22:04:40` ✅ gap-metrics COMPLETE: 11/11 OK; implied-corr 7.94 (0.2th pctile) via CBOE; breadth None% (WARMING_UP eta 2026-07-28 (60 names)); stock-level warmup running with 60 names
- `22:04:40` FAILS=0 WARNS=0
