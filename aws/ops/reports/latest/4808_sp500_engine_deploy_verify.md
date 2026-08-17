# ops 4808 -- justhodl-sp500 birth verify

**Status:** failure  
**Duration:** 18.7s  
**Finished:** 2026-08-17T02:42:32+00:00  

## Error

```
SystemExit: 1
```

## Data

| buyback_yield | cols_missing | cpi | deployed_marker | div_yield | earnings_yield | env_FRED_API_KEY | erp | ev_ebitda | ev_ebitda_ttm | fcf_yield | hist_days | mem | ntm_growth | pb | pe_fwd | pe_ttm | ps | ps_ttm | reprice | rule20 | runtime | spx_level | state | total_mcap | us10y | zip_kb |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  | 1024 |  |  |  |  |  |  |  |  | python3.12 |  | Active |  |  |  |
|  |  |  |  |  |  | present |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 105 |
|  | ['est_net_income_avg', 'est_revenue_avg', 'est_ebitda_avg'] |  |  |  |  |  |  |  |  |  | 1 |  |  |  |  |  |  |  | {"census_px_date": "2026-08-13", "now_px_date": "2026-08-13", "members_repriced": 493, "members_flat": 3} |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 32.467532467532465 vs agg 24.73 med 23.45 pct 71.1 RICH vs index |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | None vs agg None med None pct None None |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 8.967 vs agg 3.55 med 3.12 pct 88.1 RICH vs index |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 38.931 vs agg 5.28 med 3.81 pct 92.5 RICH vs index |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 24.974 vs agg 16.28 med 14.73 pct 82.4 RICH vs index |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 1.23 |  | 3.54 |  | 1.07 | 4.04 |  | -0.59 | 16.28 |  | 3.31 |  |  | None | 5.28 | None | 24.73 | 3.55 |  |  | 28.3 |  | 7786.01 |  | 70865717819487.0 | 4.63 |  |

## Log
## 1. function Active + env heal

## 2. deploy settle (zip marker)

## 3. EventBridge Scheduler ensure

- `02:42:14` ✅ schedule justhodl-sp500-daily already correct
## 4. Event-invoke + poll as_of

- `02:42:30` ✅   fresh doc after ~15s  as_of=2026-08-17T02:42:17.609815+00:00
## 5. truth bands (all real)

- `02:42:30` ✅   members = 495
- `02:42:30` ✅   pe_ttm.agg = 24.73
- `02:42:30` ✗   pe_fwd.agg = None  [band 10..30.9125]
- `02:42:30` ✅   earnings_yield.agg = 4.04
- `02:42:30` ✅   div_yield.agg = 1.07
- `02:42:30` ✅   pe_ttm.median (dist populated) = 23.45
- `02:42:30` ✅   ps_ttm.agg = 3.55
- `02:42:30` ✗   roe.agg = 65.2  [band 5..60]
- `02:42:30` ✅   erp_ttm = -0.59
- `02:42:30` ✅   rule_of_20 = 28.3
## 6. compare-mode smoke (AAPL)

- `02:42:32` ✅   compare rows = 12
- `02:42:32` ✅   compare rows w/ percentile = 11
## 7. headline readout

- `02:42:32` ✗ HARD FAILS: ['pe_fwd.agg', 'roe.agg']
