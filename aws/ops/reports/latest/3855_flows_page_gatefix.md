# ops 3855 — flows.html render fix proven on the edge, live feed

**Status:** success  
**Duration:** 1.1s  
**Finished:** 2026-07-25T15:40:46+00:00  

## Data

| fails | full_rows | heatmap_bytes | inflow_rows | live_rows | marker | outflow_rows | z_scored |
|---|---|---|---|---|---|---|---|
| [] | 297 | 4251 | 11 | 300 | JH_FLOWS_DIVFIX_3853 | 11 | 296 |

## Log
## 1. EDGE — poll until the new page is actually served

- `15:40:46` ✅   served on attempt 1 (42,811 bytes)
## 2. structural invariant — the bug class itself

- `15:40:46` ✅   renderDivergence defined exactly once
- `15:40:46` ✅   defined BEFORE load() in the same executing block
- `15:40:46` ✅   nav-drawer script tag is self-closed (no trapped body)
- `15:40:46` ✅   no function body left inside any src'd script tag
## 3. execute the page's own script against the LIVE feed

- `15:40:46`   live feed: 300 rows, 296 z-scored
- `15:40:46` ✅   render completed with no exception
- `15:40:46` ✅   meta-bar              74 bytes  rows=0
- `15:40:46` ✅   composite-grid     2,252 bytes  rows=0
- `15:40:46` ✅   sector-heatmap     4,251 bytes  rows=0
- `15:40:46` ✅   top-inflows        3,348 bytes  rows=11
- `15:40:46` ✅   top-outflows       3,355 bytes  rows=11
- `15:40:46` ✅   full-table       108,059 bytes  rows=297
## 4. the four reported sections, specifically

- `15:40:46` ✅   sector heatmap has all 11 SPDR cells
- `15:40:46` ✅   top-inflows renders 10 data rows
- `15:40:46` ✅   top-outflows renders 10 data rows
- `15:40:46` ✅   full universe renders the whole z-scored set
- `15:40:46` ✅ PASS_ALL — all four sections render from live data on the served page
