# ops 4569 — disk-true FRED accounting verify

**Status:** success  
**Duration:** 200.5s  
**Finished:** 2026-08-09T19:36:38+00:00  

## Data

| disk_scoped_objects | fred_catalog_note | fred_coverage_pct | fred_freshest_h | fred_n_keys | fred_series_count | fred_total_mb | post_blocked_at | post_imported | post_skipped | post_updated_at | pre_imported | pre_skipped | pre_updated_at | reconcile_ok | totals |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  | 6944 | 916 | 2026-08-09T19:28:47+00:00 |  |  |
|  |  |  |  |  |  |  | None | 7082 | 1058 | 2026-08-09T19:33:19+00:00 |  |  |  |  |  |
| 1341 |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |
|  | scoped import: 1,341 series banked · 68/179 categories · walking | None | 0.0 | 3882 | 1341 | 701.57 |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  |  | True | {"providers": 42, "datasets": 47232, "keys": 23845, "gb": 42.31} |

## Log
## 1. settle both engines by marker

- `19:33:18` ✅ both settled after 0s
## 2. importer dedup proof (state before/after fire)

## 3. hub regen + gates (independent disk recount)

- `19:36:38` ✅ hub refreshed after 18s
## 4. served hub proof (edge)

- `19:36:38` ✅ edge hub carries disk-true note (attempt 1): 1341 series
- `19:36:38` ✅ PASS_ALL — disk is the source of truth end to end
