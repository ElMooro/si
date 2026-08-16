# ops 4743 -- segment-depth grid for ambs/tsy/seclending + soma special-case

**Status:** success  
**Duration:** 3.2s  
**Finished:** 2026-08-16T15:47:47+00:00  

## Data

| banked | check | earliest | family | latest | n_rows | reason | value |
|---|---|---|---|---|---|---|---|
| False |  |  | ambs |  |  | grid_exhausted |  |
| False |  |  | tsy |  |  | grid_exhausted |  |
| False |  |  | seclending |  |  | grid_exhausted |  |
| True |  | 2003-07-09 | soma-summary | 2026-08-12 | 1206 |  |  |
|  | soma_asof_dates_available |  |  |  |  |  | 1206 |

## Log
## A. Grid search: ambs / tsy / seclending

- `15:47:44` ambs: ambs/all/all/results/search.json -> status=400 rows=0
- `15:47:45` ambs: ambs/all/all/search.json -> status=400 rows=0
- `15:47:45` ambs: ambs/all/all/all/results/search.json -> status=400 rows=0
- `15:47:45` ambs: ambs/all/all/all/search.json -> status=400 rows=0
- `15:47:45` ambs: ambs/all/all/all/all/results/search.json -> status=400 rows=0
- `15:47:45` ambs: ambs/all/all/all/all/search.json -> status=400 rows=0
- `15:47:45` ⚠ ambs: grid exhausted 2-4 segments -- next escalation is the site's JS bundle route strings, not more guessing
- `15:47:45` tsy: tsy/all/all/results/search.json -> status=400 rows=0
- `15:47:46` tsy: tsy/all/all/search.json -> status=400 rows=0
- `15:47:46` tsy: tsy/all/all/all/results/search.json -> status=400 rows=0
- `15:47:46` tsy: tsy/all/all/all/search.json -> status=400 rows=0
- `15:47:46` tsy: tsy/all/all/all/all/results/search.json -> status=400 rows=0
- `15:47:46` tsy: tsy/all/all/all/all/search.json -> status=400 rows=0
- `15:47:46` ⚠ tsy: grid exhausted 2-4 segments -- next escalation is the site's JS bundle route strings, not more guessing
- `15:47:46` seclending: seclending/all/all/results/search.json -> status=400 rows=0
- `15:47:46` seclending: seclending/all/all/search.json -> status=400 rows=0
- `15:47:47` seclending: seclending/all/all/all/results/search.json -> status=400 rows=0
- `15:47:47` seclending: seclending/all/all/all/search.json -> status=400 rows=0
- `15:47:47` seclending: seclending/all/all/all/all/results/search.json -> status=400 rows=0
- `15:47:47` seclending: seclending/all/all/all/all/search.json -> status=400 rows=0
- `15:47:47` ⚠ seclending: grid exhausted 2-4 segments -- next escalation is the site's JS bundle route strings, not more guessing
## B. soma special-case

- `15:47:47` soma/summary.json -> status=200 rows=1206
- `15:47:47` ✅ soma summary: banked 1206 rows, 2003-07-09 -> 2026-08-12
- `15:47:47` per-CUSIP holdings exist for 1206 as-of dates -- that is a dedicated-engine build (row volume), flagged, never half-banked here
