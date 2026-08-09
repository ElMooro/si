# ops 4567 — FRED scoped-import liveness (read-only)

**Status:** success  
**Duration:** 1.2s  
**Finished:** 2026-08-09T19:09:35+00:00  

## Data

| accounting | blocked_at | categories_done | categories_total | cats | cats_done | errors_3h | excluded_stale | fred | hours | imported | imported_per_hr | imported_total | invocations_3h | of | series_imported | series_queued | series_seen | since | statcan | state_age_min | status | totals | updated_at |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| {"seen": 11083, "accounted": 11083, "reconciles": true} | None |  |  |  | 68 |  | 92 |  |  |  |  | 6269 |  | 179 |  | 5407 | 11083 |  |  | 5.8 | walking |  | 2026-08-09T19:03:47+00:00 |
|  |  |  |  | 0 |  |  |  |  | 2.58 | 4212 | 1634.9 |  |  |  |  |  |  | 2026-08-09T16:35:00+00:00 |  |  |  |  |  |
|  |  |  |  |  |  | 8 |  |  |  |  |  |  | 114 |  |  |  |  |  |  |  |  |  |  |
|  |  | 68 | 179 |  |  |  |  |  |  |  |  |  |  |  | 6269 |  |  |  |  |  | walking |  | 2026-08-09T19:03:47+00:00 |
|  |  |  |  |  |  |  |  | {"n_keys": 298, "total_mb": 31.05, "freshest_h": 0.1} |  |  |  |  |  |  |  |  |  |  | {"n_keys": 7962, "datasets_target": 8221, "coverage_pct": 96.8} |  |  | {"providers": 42, "datasets": 43648, "keys": 20178, "gb": 41.63} |  |

## Log
## state

## delta vs ops-4566 baseline (2057 imported, 68/179 cats)

- `19:09:35` ✅ rule justhodl-fred-catalog-5min: ENABLED rate(5 minutes)
## manifest

## hub

- `19:09:35` ✅ IMPORTING imported=6269 (+4212 in 2.6h) cats=68/179 rule=ENABLED inv3h=114 state_age_min=5.8
