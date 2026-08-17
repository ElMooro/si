# ops 4808 -- justhodl-sp500 birth verify

**Status:** success  
**Duration:** 24.3s  
**Finished:** 2026-08-17T02:57:48+00:00  

## Data

| buyback_yield | cols_missing | cpi | deployed_marker | div_yield | earnings_yield | env_FRED_API_KEY | erp | ev_ebitda | ev_ebitda_ttm | fcf_yield | hist_days | mem | ntm_growth | pb | pe_fwd | pe_ttm | ps | ps_ttm | reprice | rule20 | runtime | spx_level | state | total_mcap | us10y | zip_kb |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  | 1024 |  |  |  |  |  |  |  |  | python3.12 |  | Active |  |  |  |
|  |  |  |  |  |  | present |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  | True |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 105 |
|  | [] |  |  |  |  |  |  |  |  |  | 1 |  |  |  |  |  |  |  | {"census_px_date": "2026-08-13", "now_px_date": "2026-08-13", "members_repriced": 493, "members_flat": 3} |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 32.467532467532465 vs agg 24.73 med 23.45 pct 71.1 RICH vs index |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 30.735 vs agg 20.37 med 18.34 pct 84.4 RICH vs index |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 8.967 vs agg 3.55 med 3.12 pct 88.1 RICH vs index |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | 38.931 vs agg 5.28 med 3.81 pct 92.5 RICH vs index |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  | 24.974 vs agg 16.28 med 14.73 pct 82.4 RICH vs index |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 1.23 |  | 3.54 |  | 1.07 | 4.04 |  | -0.59 | 16.28 |  | 3.31 |  |  | 21.4 | 5.28 | 20.37 | 24.73 | 3.55 |  |  | 28.3 |  | 7786.01 |  | 70865717819487.0 | 4.63 |  |

## Log
## 1. function Active + env heal

## 2. deploy settle (zip marker)

## 3. EventBridge Scheduler ensure

- `02:57:31` ✅ schedule justhodl-sp500-daily already correct
## 4. Event-invoke + poll as_of

- `02:57:47` ✅   fresh doc after ~15s  as_of=2026-08-17T02:57:34.078881+00:00
## 5. truth bands (all real)

- `02:57:47` ✅   members = 495
- `02:57:47` ✅   pe_ttm.agg = 24.73
- `02:57:47` ✅   pe_fwd.agg = 20.37
- `02:57:47` ✅   earnings_yield.agg = 4.04
- `02:57:47` ✅   div_yield.agg = 1.07
- `02:57:47` ✅   pe_ttm.median (dist populated) = 23.45
- `02:57:47` ✅   ps_ttm.agg = 3.55
- `02:57:47` ✅   roe.agg = 21.3
- `02:57:47` ✅   erp_ttm = -0.59
- `02:57:47` ✅   rule_of_20 = 28.3
## 6. compare-mode smoke (AAPL)

- `02:57:48` ✅   compare rows = 12
- `02:57:48` ✅   compare rows w/ percentile = 12
## 7. headline readout

- `02:57:48` ✅ justhodl-sp500 LIVE -- the index now reads like a single stock
