# ops 4651 — stock-buying flagship

**Status:** failure  
**Duration:** 212.5s  
**Finished:** 2026-08-13T16:59:22+00:00  

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

- `16:55:49` key from fmp-fundamentals-agent.FMP_API_KEY (len=32)
## deploy (create-capable) + schedule

- `16:56:02` ✅   [deploy] v1.0.3 live (created=False)
## matrix probe (runner-side)

- `16:56:03` top_keys=['generated_at', 'n_tickers', 'n_metrics', 'tickers', 'sectors', 'industries', 'quality', 'turn', 'flagged', 'metrics'] n_tickers=498 n_cols=293
- `16:56:03` first cols: ['above_ma40w', 'accountsPayable', 'acquisitions', 'altman_z', 'altman_z_prime', 'aoci', 'asset_turnover', 'asset_turnover_ttm', 'assets_per_employee', 'beneish_m', 'beta_2y', 'book_value_per_share', 'book_value_ps', 'breakout_20w', 'buyback_yield_gross_pct', 'buyback_yield_pct', 'bvps_yoy_pct', 'capLeaseObligations', 'capex', 'capex_to_da']
## run + institutional truth

- `16:56:15` CW| [ERROR] TypeError: unsupported operand type(s) for +: 'int' and 'NoneType'
- `16:56:15` CW| File "/var/task/lambda_function.py", line 249, in lambda_handler
- `16:56:15` CW| File "/var/task/lambda_function.py", line 169, in sma_state
- `16:56:15` CW| [ERROR] TypeError: unsupported operand type(s) for +: 'int' and 'NoneType'
- `16:56:15` CW| File "/var/task/lambda_function.py", line 249, in lambda_handler
- `16:56:15` CW| File "/var/task/lambda_function.py", line 169, in sma_state
- `16:56:15` engine matrix_probe: null
- `16:56:15` census fields: []
- `16:56:15` ✗   [universe] CONTRACT MISS — 0 companies in census universe
- `16:56:15` ✗   [scored] CONTRACT MISS — 0 scored rows
- `16:56:15` ✗   [row-integrity] CONTRACT MISS — top row carries pillars+gates+why link (None)
- `16:56:15` ✅   [tiers] tier partition sums: {'EXPLOSIVE-SETUP': 0, 'SETUP': 0, 'WATCH': 0, 'SCREENED': 0}
## edge (CF purge + structural)

- `16:56:16` CF purge issued
- `16:59:22` ✗   [edge] CONTRACT MISS — page structural + dblclick->why + payload at edge
## verdict

- `16:59:22` ✗ stock-buying: 4 red
