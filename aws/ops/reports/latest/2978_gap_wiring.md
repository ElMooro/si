## 1. Wait for gap-metrics code deploy

**Status:** success  
**Duration:** 85.5s  
**Finished:** 2026-07-07T21:45:43+00:00  

## Data

| body | credit_desk_live | degraded | deploy_age_s | env_vars | fn_error | invoke_seconds | missing | modules_ok | ok | pages_wired_live | public_stock_bond |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  | 387 | 4 |  |  |  |  |  |  |  |
| {"statusCode": 200, "body": "{\"ok\": 9, \"total\": 11, \"elapsed_s\": 6.7}"} |  |  |  |  | None | 7.5 |  |  |  |  |  |
|  |  | {"revision_breadth": "data/estimate-revisions.json: only 2 signed of 40 rows; keys: ['baseline_date', 'baseline_eps_est', 'company', 'current_eps_est', 'days_to_earnings', 'direction', 'dispersion_pct', 'earnings_date', 'eps_rev_pct', 'eps_rev_recent_pct', 'estimate_strength', 'fiscal_period'] | data/sellside-views.json:", "cor3m": "no implied-corr symbol returned >=60 closes (tried COR3M/COR90D/C |  |  |  |  |  | 9 | ['baltic_dry', 'bill_share', 'em_carry', 'global_m2', 'miner_margin', 'muni_ratio', 'ofr_fsi', 'sloos', 'stock_bond_corr'] |  |  |
|  |  |  |  |  |  |  | [] |  |  | 10 |  |
|  | True |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | POSITIVE |

## Log
## 2. Re-invoke

## 3. Module verify (post-fix)

## 4. Live pages (runner-side)

- `21:45:43` ✅ gap matrix WIRED: 9/11 modules OK; 10 pages + credit-desk live; feedless pages ofr/metals-miners/activity-nowcast now have real feeds
- `21:45:43` FAILS=0 WARNS=2
