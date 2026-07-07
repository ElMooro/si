## 0. Source probes from the runner

**Status:** success  
**Duration:** 13.7s  
**Finished:** 2026-07-07T21:16:24+00:00  

## Data

| age_min | baltic_dry | bill_share | body | degraded | em_carry | env_keys | fn_error | fred_sloos_latest | global_m2 | invoke_seconds | modules_ok | muni_ratio | ofr_csv_bytes | ofr_fsi | ok | polygon_spy_prev | schedule | sloos | stock_bond_corr | treasurydirect_bytes |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  | ['FMP_KEY', 'FRED_KEY', 'POLYGON_KEY', 'S3_BUCKET'] |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  | 8.1 |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 38729 |
|  |  |  |  |  |  |  |  |  |  |  |  |  | 511098 |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | created |  |  |  |
|  |  |  | {"statusCode": 200, "body": "{\"ok\": 8, \"total\": 11, \"elapsed_s\": 5.4}"} |  |  |  | None |  |  | 6.2 |  |  |  |  |  |  |  |  |  |  |
| 0.1 |  |  |  | {"revision_breadth": "no numeric revision field; row keys: ['baseline_date', 'baseline_eps_est', 'company', 'current_eps_est', 'days_to_earnings', 'direction', 'dispersion_pct', 'earnings_date', 'eps_rev_pct', 'eps_rev_recent_pct', 'estimate_strength', 'fiscal_period', 'fiscal_year', 'fwd_eps_growth |  |  |  |  |  |  | 8 |  |  |  | ['baltic_dry', 'bill_share', 'em_carry', 'global_m2', 'muni_ratio', 'ofr_fsi', 'sloos', 'stock_bond_corr'] |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | verified |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | verified |  |
|  |  |  |  |  |  |  |  |  | verified |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | verified |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | verified |  |  |  |  |  |  |  |  |
|  |  |  |  |  | verified |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | verified |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  | verified |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |

## Log
## 1. Deploy justhodl-gap-metrics

- `21:16:12`   zip: 8308 bytes
## 1. Lambda

- `21:16:12`   Lambda missing — creating
- `21:16:17` ✅   ✓ created justhodl-gap-metrics
## 2. Scheduler daily 21:45 UTC

## 3. Synchronous first run

## 4. Hard verify

- `21:16:24` ✅ gap-metrics LIVE: 8/11 OK; SLOOS 8.1% | stock-bond {"current_63d_corr": 0.415, "regime": "POSITIVE"} | bills 80.8% | M2 yoy -0.65% | OFR -2.682 | muni 0.708
- `21:16:24` FAILS=0 WARNS=0
