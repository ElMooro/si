# ops 4651 — stock-buying flagship

**Status:** failure  
**Duration:** 224.3s  
**Finished:** 2026-08-13T17:55:18+00:00  

## Error

```
SystemExit: 1
```

## Data

| census | fmp | fn_error | gates | mode | scored | tiers | universe |
|---|---|---|---|---|---|---|---|
|  |  | None |  |  |  |  |  |
| data/fundamental-census-matrix.json | True |  | {"below_sma": 127, "eps_seq": 0, "dilution": 498, "margin_floor": 498} | columnar(293 cols) | 498 | {"EXPLOSIVE-SETUP": 0, "SETUP": 0, "WATCH": 17, "SCREENED": 481} | 498 |

## Log
## FMP key donor -> engine env

- `17:51:34` key from fmp-fundamentals-agent.FMP_API_KEY (len=32)
## authority probes

- `17:51:34` S3: data/fundamental-census-history.json (226 B, 2026-08-01 06:48)
- `17:51:34` S3: data/fundamental-census-matrix.json (1180010 B, 2026-08-01 06:48)
- `17:51:34` S3: data/fundamental-census.json (78138 B, 2026-08-01 06:48)
- `17:51:34` S3: data/fundamentals-decisive-call.json (657 B, 2026-08-13 13:20)
- `17:51:34` S3: data/fundamentals.json (13646 B, 2026-08-13 13:00)
- `17:51:34` ⚠ census_idx replica: '{' was never closed (cidx, line 56)
## verbatim matrix truth

- `17:51:35` top keys: ['generated_at', 'n_tickers', 'n_metrics', 'tickers', 'sectors', 'industries', 'quality', 'turn', 'flagged', 'metrics', 'cols']
- `17:51:35`   generated_at -> str len=32
- `17:51:35`   n_tickers -> int len=-
- `17:51:35`   n_metrics -> int len=-
- `17:51:35`   tickers -> list len=498
- `17:51:35`   sectors -> list len=498
- `17:51:35`   industries -> list len=498
- `17:51:35`   quality -> list len=498
- `17:51:35`   turn -> list len=498
## deploy (create-capable) + schedule

- `17:51:48` ✅   [deploy] v1.0.5 live (created=False)
## matrix probe (runner-side)

- `17:51:48` top_keys=['generated_at', 'n_tickers', 'n_metrics', 'tickers', 'sectors', 'industries', 'quality', 'turn', 'flagged', 'metrics'] n_tickers=498 n_cols=293
- `17:51:48` first cols: ['above_ma40w', 'accountsPayable', 'acquisitions', 'altman_z', 'altman_z_prime', 'aoci', 'asset_turnover', 'asset_turnover_ttm', 'assets_per_employee', 'beneish_m', 'beta_2y', 'book_value_per_share', 'book_value_ps', 'breakout_20w', 'buyback_yield_gross_pct', 'buyback_yield_pct', 'bvps_yoy_pct', 'capLeaseObligations', 'capex', 'capex_to_da']
## engine matrix_probe (last payload)

- `17:51:48` matrix_probe: null
## run + institutional truth

- `17:52:12` engine matrix_probe: {"loaded": true, "top_keys": ["generated_at", "n_tickers", "n_metrics", "tickers", "sectors", "industries", "quality", "turn", "flagged", "metrics"], "n_tickers": 498, "n_cols": 293}
- `17:52:12` census fields: ['above_ma40w', 'accountsPayable', 'acquisitions', 'altman_z', 'altman_z_prime', 'aoci', 'asset_turnover', 'asset_turnover_ttm', 'assets_per_employee', 'beneish_m', 'beta_2y', 'book_value_per_share', 'book_value_ps', 'breakout_20w', 'buyback_yield_gross_pct', 'buyback_yield_pct', 'bvps_yoy_pct', 'capLeaseObligations', 'capex', 'capex_to_da', 'capex_to_revenue_pct', 'capex_yoy_pct', 'cash', 'cashSTI', 'cash_conversion_pct', 'cash_ps', 'cash_ratio', 'cash_to_debt', 'ccc_days', 'cff', 'cfi', 'cfo', 'cfo_ps_ttm', 'cfo_to_debt_pct', 'cfo_ttm', 'cfo_yoy_pct']
- `17:52:12` DELL   SCREENED         --        sc=63.6  bt=None  ac=None  fcf=None  val=15.0  cat=100.0 peg=None  
- `17:52:12` ADP    SCREENED         --        sc=63.6  bt=None  ac=None  fcf=None  val=15.0  cat=100.0 peg=None  
- `17:52:12` MPC    SCREENED         --        sc=63.6  bt=None  ac=None  fcf=None  val=15.0  cat=100.0 peg=None  
- `17:52:12` HPQ    SCREENED         --        sc=63.6  bt=None  ac=None  fcf=None  val=15.0  cat=100.0 peg=None  
- `17:52:12` BBY    SCREENED         --        sc=63.6  bt=None  ac=None  fcf=None  val=15.0  cat=100.0 peg=None  
- `17:52:12` TMO    SCREENED         --        sc=63.6  bt=None  ac=None  fcf=None  val=15.0  cat=100.0 peg=None  
- `17:52:12` DHR    SCREENED         --        sc=63.6  bt=None  ac=None  fcf=None  val=15.0  cat=100.0 peg=None  
- `17:52:12` BDX    SCREENED         --        sc=63.6  bt=None  ac=None  fcf=None  val=15.0  cat=100.0 peg=None  
- `17:52:12` ROP    SCREENED         DB        sc=63.6  bt=None  ac=None  fcf=None  val=15.0  cat=100.0 peg=None  
- `17:52:12` IQV    SCREENED         --        sc=63.6  bt=None  ac=None  fcf=None  val=15.0  cat=100.0 peg=None  
- `17:52:12` DGX    SCREENED         --        sc=63.6  bt=None  ac=None  fcf=None  val=15.0  cat=100.0 peg=None  
- `17:52:12` GPN    SCREENED         DB        sc=63.6  bt=None  ac=None  fcf=None  val=15.0  cat=100.0 peg=None  
- `17:52:12` ✅   [universe] 498 companies in census universe
- `17:52:12` ✅   [scored] 498 scored rows
- `17:52:12` ✅   [row-integrity] top row carries pillars+gates+why link (DELL)
- `17:52:12` ✅   [tiers] tier partition sums: {'EXPLOSIVE-SETUP': 0, 'SETUP': 0, 'WATCH': 17, 'SCREENED': 481}
## edge (CF purge + structural)

- `17:52:12` CF purge issued
- `17:55:18` ✗   [edge] CONTRACT MISS — page structural + dblclick->why + payload at edge
## verdict

- `17:55:18` ✗ stock-buying: 1 red
