# ops 4651 — stock-buying flagship

**Status:** failure  
**Duration:** 214.7s  
**Finished:** 2026-08-13T17:29:50+00:00  

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

- `17:26:16` key from fmp-fundamentals-agent.FMP_API_KEY (len=32)
## authority probes

- `17:26:16` S3: data/fundamental-census-history.json (226 B, 2026-08-01 06:48)
- `17:26:16` S3: data/fundamental-census-matrix.json (1180010 B, 2026-08-01 06:48)
- `17:26:16` S3: data/fundamental-census.json (78138 B, 2026-08-01 06:48)
- `17:26:16` S3: data/fundamentals-decisive-call.json (657 B, 2026-08-13 13:20)
- `17:26:16` S3: data/fundamentals.json (13646 B, 2026-08-13 13:00)
- `17:26:16` ⚠ census_idx replica: '{' was never closed (cidx, line 56)
## deploy (create-capable) + schedule

- `17:26:30` ✅   [deploy] v1.0.3 live (created=False)
## matrix probe (runner-side)

- `17:26:31` top_keys=['generated_at', 'n_tickers', 'n_metrics', 'tickers', 'sectors', 'industries', 'quality', 'turn', 'flagged', 'metrics'] n_tickers=498 n_cols=293
- `17:26:31` first cols: ['above_ma40w', 'accountsPayable', 'acquisitions', 'altman_z', 'altman_z_prime', 'aoci', 'asset_turnover', 'asset_turnover_ttm', 'assets_per_employee', 'beneish_m', 'beta_2y', 'book_value_per_share', 'book_value_ps', 'breakout_20w', 'buyback_yield_gross_pct', 'buyback_yield_pct', 'bvps_yoy_pct', 'capLeaseObligations', 'capex', 'capex_to_da']
## run + institutional truth

- `17:26:43` CW| [ERROR] TypeError: unsupported operand type(s) for +: 'int' and 'NoneType'
- `17:26:43` CW| File "/var/task/lambda_function.py", line 249, in lambda_handler
- `17:26:43` CW| File "/var/task/lambda_function.py", line 169, in sma_state
- `17:26:43` CW| [ERROR] TypeError: unsupported operand type(s) for +: 'int' and 'NoneType'
- `17:26:43` CW| File "/var/task/lambda_function.py", line 249, in lambda_handler
- `17:26:43` CW| File "/var/task/lambda_function.py", line 169, in sma_state
- `17:26:44` engine matrix_probe: null
- `17:26:44` census fields: []
- `17:26:44` ✗   [universe] CONTRACT MISS — 0 companies in census universe
- `17:26:44` ✗   [scored] CONTRACT MISS — 0 scored rows
- `17:26:44` ✗   [row-integrity] CONTRACT MISS — top row carries pillars+gates+why link (None)
- `17:26:44` ✅   [tiers] tier partition sums: {'EXPLOSIVE-SETUP': 0, 'SETUP': 0, 'WATCH': 0, 'SCREENED': 0}
## edge (CF purge + structural)

- `17:26:44` CF purge issued
- `17:29:50` ✗   [edge] CONTRACT MISS — page structural + dblclick->why + payload at edge
## verdict

- `17:29:50` ✗ stock-buying: 4 red
