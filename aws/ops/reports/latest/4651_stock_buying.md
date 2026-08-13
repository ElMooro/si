# ops 4651 — stock-buying flagship

**Status:** failure  
**Duration:** 213.6s  
**Finished:** 2026-08-13T16:54:05+00:00  

## Error

```
SystemExit: 1
```

## Data

| census | fmp | fn_error | gates | mode | scored | tiers | universe |
|---|---|---|---|---|---|---|---|
|  |  | Unhandled |  |  |  |  |  |
| None | True |  | {"below_sma": 0, "eps_seq": 0, "dilution": 0, "margin_floor": 0} | None | 0 | {"EXPLOSIVE-SETUP": 0, "SETUP": 0, "WATCH": 0, "SCREENED": 0} | 0 |

## Log
## FMP key donor -> engine env

- `16:50:31` key from fmp-fundamentals-agent.FMP_API_KEY (len=32)
## deploy (create-capable) + schedule

- `16:50:45` ✅   [deploy] v1.0.2 live (created=False)
## matrix probe (runner-side)

- `16:50:45` top_keys=['generated_at', 'n_tickers', 'n_metrics', 'tickers', 'sectors', 'industries', 'quality', 'turn', 'flagged', 'metrics'] n_tickers=498 n_cols=293
- `16:50:45` first cols: ['above_ma40w', 'accountsPayable', 'acquisitions', 'altman_z', 'altman_z_prime', 'aoci', 'asset_turnover', 'asset_turnover_ttm', 'assets_per_employee', 'beneish_m', 'beta_2y', 'book_value_per_share', 'book_value_ps', 'breakout_20w', 'buyback_yield_gross_pct', 'buyback_yield_pct', 'bvps_yoy_pct', 'capLeaseObligations', 'capex', 'capex_to_da']
## run + institutional truth

- `16:50:58` CW| [ERROR] TypeError: unsupported operand type(s) for +: 'int' and 'NoneType'
- `16:50:58` CW| File "/var/task/lambda_function.py", line 241, in lambda_handler
- `16:50:58` CW| File "/var/task/lambda_function.py", line 161, in sma_state
- `16:50:58` CW| [ERROR] TypeError: unsupported operand type(s) for +: 'int' and 'NoneType'
- `16:50:58` CW| File "/var/task/lambda_function.py", line 249, in lambda_handler
- `16:50:58` CW| File "/var/task/lambda_function.py", line 169, in sma_state
- `16:50:58` engine matrix_probe: null
- `16:50:58` census fields: []
- `16:50:58` ✗   [universe] CONTRACT MISS — 0 companies in census universe
- `16:50:58` ✗   [scored] CONTRACT MISS — 0 scored rows
- `16:50:58` ✗   [row-integrity] CONTRACT MISS — top row carries pillars+gates+why link (None)
- `16:50:58` ✅   [tiers] tier partition sums: {'EXPLOSIVE-SETUP': 0, 'SETUP': 0, 'WATCH': 0, 'SCREENED': 0}
## edge (CF purge + structural)

- `16:50:58` CF purge issued
- `16:54:05` ✗   [edge] CONTRACT MISS — page structural + dblclick->why + payload at edge
## verdict

- `16:54:05` ✗ stock-buying: 4 red
