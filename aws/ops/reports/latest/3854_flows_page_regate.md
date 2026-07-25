# ops 3854 — flows.html render fix proven on the edge, live feed

**Status:** failure  
**Duration:** 2.4s  
**Finished:** 2026-07-25T15:36:47+00:00  

## Error

```
SystemExit: 1
```

## Data

| fails | full_rows | heatmap_bytes | inflow_rows | live_rows | marker | outflow_rows | z_scored |
|---|---|---|---|---|---|---|---|
| ['nav-drawer script tag is self-closed (no trapped body)', 'meta-bar'] | 297 | 4251 | 11 | 300 | JH_FLOWS_DIVFIX_3853 | 11 | 296 |

## Log
## 1. EDGE — poll until the new page is actually served

- `15:36:45` ✅   served on attempt 1 (42,811 bytes)
## 2. structural invariant — the bug class itself

- `15:36:45` ✅   renderDivergence defined exactly once
- `15:36:45` ✅   defined BEFORE load() in the same executing block
- `15:36:45` ✗   nav-drawer script tag is self-closed (no trapped body)
- `15:36:45` ✅   no function body left inside any src'd script tag
## 3. execute the page's own script against the LIVE feed

- `15:36:46`   live feed: 300 rows, 296 z-scored
- `15:36:47` ✅   render completed with no exception
- `15:36:47` ✗   meta-bar              74 bytes  rows=0
- `15:36:47` ✅   composite-grid     2,252 bytes  rows=0
- `15:36:47` ✅   sector-heatmap     4,251 bytes  rows=0
- `15:36:47` ✅   top-inflows        3,348 bytes  rows=11
- `15:36:47` ✅   top-outflows       3,355 bytes  rows=11
- `15:36:47` ✅   full-table       108,059 bytes  rows=297
## 4. the four reported sections, specifically

- `15:36:47` ✅   sector heatmap has all 11 SPDR cells
- `15:36:47` ✅   top-inflows renders 10 data rows
- `15:36:47` ✅   top-outflows renders 10 data rows
- `15:36:47` ✅   full universe renders the whole z-scored set
- `15:36:47` ✗ FAILED 2: ['nav-drawer script tag is self-closed (no trapped body)', 'meta-bar']
