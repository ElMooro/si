# ops 4651 — stock-buying flagship

**Status:** failure  
**Duration:** 213.6s  
**Finished:** 2026-08-13T17:41:27+00:00  

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

- `17:37:53` key from fmp-fundamentals-agent.FMP_API_KEY (len=32)
## authority probes

- `17:37:54` S3: data/fundamental-census-history.json (226 B, 2026-08-01 06:48)
- `17:37:54` S3: data/fundamental-census-matrix.json (1180010 B, 2026-08-01 06:48)
- `17:37:54` S3: data/fundamental-census.json (78138 B, 2026-08-01 06:48)
- `17:37:54` S3: data/fundamentals-decisive-call.json (657 B, 2026-08-13 13:20)
- `17:37:54` S3: data/fundamentals.json (13646 B, 2026-08-13 13:00)
- `17:37:54` ⚠ census_idx replica: '{' was never closed (cidx, line 56)
## verbatim matrix truth

- `17:37:54` top keys: ['generated_at', 'n_tickers', 'n_metrics', 'tickers', 'sectors', 'industries', 'quality', 'turn', 'flagged', 'metrics', 'cols']
- `17:37:54`   generated_at -> str len=32
- `17:37:54`   n_tickers -> int len=-
- `17:37:54`   n_metrics -> int len=-
- `17:37:54`   tickers -> list len=498
- `17:37:54`   sectors -> list len=498
- `17:37:54`   industries -> list len=498
- `17:37:54`   quality -> list len=498
- `17:37:54`   turn -> list len=498
## deploy (create-capable) + schedule

- `17:38:07` ✅   [deploy] v1.0.4 live (created=False)
## matrix probe (runner-side)

- `17:38:07` top_keys=['generated_at', 'n_tickers', 'n_metrics', 'tickers', 'sectors', 'industries', 'quality', 'turn', 'flagged', 'metrics'] n_tickers=498 n_cols=293
- `17:38:07` first cols: ['above_ma40w', 'accountsPayable', 'acquisitions', 'altman_z', 'altman_z_prime', 'aoci', 'asset_turnover', 'asset_turnover_ttm', 'assets_per_employee', 'beneish_m', 'beta_2y', 'book_value_per_share', 'book_value_ps', 'breakout_20w', 'buyback_yield_gross_pct', 'buyback_yield_pct', 'bvps_yoy_pct', 'capLeaseObligations', 'capex', 'capex_to_da']
## engine matrix_probe (last payload)

- `17:38:07` matrix_probe: null
## run + institutional truth

- `17:38:20` CW| [ERROR] TypeError: unsupported operand type(s) for +: 'int' and 'NoneType'
- `17:38:20` CW| File "/var/task/lambda_function.py", line 249, in lambda_handler
- `17:38:20` CW| File "/var/task/lambda_function.py", line 169, in sma_state
- `17:38:20` CW| [ERROR] TypeError: unsupported operand type(s) for +: 'int' and 'NoneType'
- `17:38:20` CW| File "/var/task/lambda_function.py", line 257, in lambda_handler
- `17:38:20` CW| File "/var/task/lambda_function.py", line 169, in sma_state
- `17:38:20` engine matrix_probe: null
- `17:38:20` census fields: []
- `17:38:20` ✗   [universe] CONTRACT MISS — 0 companies in census universe
- `17:38:20` ✗   [scored] CONTRACT MISS — 0 scored rows
- `17:38:20` ✗   [row-integrity] CONTRACT MISS — top row carries pillars+gates+why link (None)
- `17:38:20` ✅   [tiers] tier partition sums: {'EXPLOSIVE-SETUP': 0, 'SETUP': 0, 'WATCH': 0, 'SCREENED': 0}
## edge (CF purge + structural)

- `17:38:20` CF purge issued
- `17:41:27` ✗   [edge] CONTRACT MISS — page structural + dblclick->why + payload at edge
## verdict

- `17:41:27` ✗ stock-buying: 4 red
