# ops 4568 — data page truth: verify catalog fixes

**Status:** success  
**Duration:** 107.1s  
**Finished:** 2026-08-09T19:26:40+00:00  

## Data

| fred_catalog_note | fred_coverage_pct | fred_freshest_h | fred_n_keys | fred_series_count | fred_total_mb | prior_as_of | prior_keys | reconcile_ok | served_fred_keys | served_fred_series | served_note_ok | statcan_coverage_pct | statcan_denied | totals | x_equity_research_tickers | x_indicator_bus | x_tradingview_vault_live |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  | 2026-08-09T18:48:53+00:00 | 20178 |  |  |  |  |  |  |  |  |  |  |
| scoped import: 6,680 series · 68/179 categories · walking | None | 0.0 | 3743 | 6680 | 683.46 |  |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  | 96.8 | 293 |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"n": 17791, "keys": 1, "fr": 7.2} |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"n": 81, "keys": 81, "fr": 11.4} |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | {"n": 5598, "keys": 1, "fr": 7.8} |
|  |  |  |  |  |  |  |  | True |  |  |  |  |  | {"providers": 42, "datasets": 47093, "keys": 23706, "gb": 42.29} |  |  |  |
|  |  |  |  |  |  |  |  |  | 3743 | 6680 | True |  |  |  |  |  |  |

## Log
## 1. zip-settle the redeployed engine

- `19:24:53` ✅ marker present after 0s (zip 100KB)
## 2. refresh the hub (Event + as_of poll)

- `19:25:11` ✅ hub refreshed after 18s -> as_of 2026-08-09T19:24:54+00:00
## 3. gates on the corrected numbers

## 4. served-page proof (edge)

- `19:25:12` attempt 1: stale (cf=None)
- `19:25:34` attempt 2: stale (cf=None)
- `19:25:56` attempt 3: stale (cf=None)
- `19:26:18` attempt 4: stale (cf=None)
- `19:26:40` ✅ edge serves new page (attempt 5, cf=None)
- `19:26:40` ✗ GATES FAILED: totals.keys 23706 < 25000 — fred keys did not land in totals
