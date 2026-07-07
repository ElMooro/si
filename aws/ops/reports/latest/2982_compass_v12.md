## 1. Race-safe deploy wait

**Status:** success  
**Duration:** 126.5s  
**Finished:** 2026-07-07T22:46:26+00:00  

## Data

| assets | body | clusters | corr_tickers | deploy_age_s | div_pairs | doc_warns | env_vars | fn_error | invoke_seconds | ledger | page_v12_live | scenario_assets | schema | spy_beta | spy_scen | tlt_beta | tlt_scen | vintage1_date | vintage1_n |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  | 72 |  |  | 4 |  |  |  |  |  |  |  |  |  |  |  |  |
|  | {"statusCode": 200, "body": "{\"ok\": true, \"assets\": 31, \"er_modeled\": 20, \"warns\": 0}"} |  |  |  |  |  |  | None | 50.4 |  |  |  |  |  |  |  |  |  |  |
| 31 |  |  |  |  |  | [] |  |  |  |  |  |  | 1.2 |  |  |  |  |  |  |
|  |  | 12 | 30 |  | [{"a": "INDA", "b": "USO", "corr": -0.67}, {"a": "DBC", "b": "INDA", "corr": -0.64}, {"a": "EMB", "b": "USO", "corr": -0.63}, {"a": "EFA", "b": "USO", "corr": -0.58}, {"a": "IWM", "b": "USO", "corr":  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"rate_beta_pct_per_100bp": -1.39, "bei_beta_pct_per_100bp": 6.29, "spy_beta": 1.0, "obs": 744} |  | {"rate_beta_pct_per_100bp": -14.48, "bei_beta_pct_per_100bp": -18.58, "spy_beta": 0.14, "obs": 744} |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 30 |  |  | {"plus_100bp_pct": -1.4, "minus_100bp_pct": 1.4, "recession_pct": -22.9, "inflation_shock_pct": 5.2} |  | {"plus_100bp_pct": -14.5, "minus_100bp_pct": 14.5, "recession_pct": 18.2, "inflation_shock_pct": -29.4} |  |  |
|  |  |  |  |  |  |  |  |  |  | {"entries_n": 1, "since": "2026-07-07", "graded_n": 0, "grading": "WARMING_UP", "first_grade_eta": "2027-07-02"} |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 2026-07-07 | 20 |
|  |  |  |  |  |  |  |  |  |  |  | True |  |  |  |  |  |  |  |  |

## Log
## 2. Invoke (full compass run)

## 3. Doc verify

## 4. Page live (runner-side)

- `22:46:26` ✅ COMPASS v1.2 LIVE: ledger vintage #1 (20 assets, first grade 2027-07-02) | matrix 30x30, 12 clusters, top diversifier {"a": "INDA", "b": "USO", "corr": -0.67} | TLT +100bp -14.5%, SPY recession -22.9% | page live
- `22:46:26` FAILS=0 WARNS=0
