# ops 4106 — working + rendering

**Status:** success  
**Duration:** 2.1s  
**Finished:** 2026-07-29T22:57:10+00:00  

## Data

| agency_slugs | delay_ms | done | economics_rows | lists_n | map_marker | n_new | page_lists | page_sourced | rate | selftest | store | total | union | unjoined |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 700 | 93 |  |  |  |  |  |  | 79.7 | {"n": 3, "matched": 2} | 236 | 10116 |  |  |
| 0 |  |  | 0 |  |  |  |  |  |  |  |  |  |  |  |
|  |  |  |  |  | source-map engine v2.2 ops4100 | 1 |  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  | 491 | 197 |  |  |  |  |  |  |
|  |  |  |  | 491 |  |  |  |  |  |  |  |  | 10319 | 0 |

## Log
## A. harvest vitals (v1.8.3 features live?)

- `22:57:09`   DIAG: {"started": 1785365650561, "done": 93, "total": 10116, "sc_ok": 93, "sc_err": 0, "sc2_ok": 93, "sc2_err": 0, "ss_ok": 0, "ss_err": 0, "matched": 5, "first_err": "", "tier1_done": 93, "rate_per_min": 79.7, "elapsed_s": 70, "delay_ms": 700, "wall_events": 0, "recoveries": 1, "max_delay": 0, "streak_err": 0, "paused_s": 0, "econ_probe": [{"s
## B. source-map purity

- `22:57:09`   NEW    2  US
## C. pages — workbench + BY WATCHLIST render join

- `22:57:10`   STATUS DISTRIBUTION: {"PENDING_RESOLUTION": 8513, "LIVE": 1544, "NO_FREE_SOURCE": 260, "DISCONTINUED": 2}
- `22:57:10` ✅   v1.8.3 self-test visible in diag
- `22:57:10` ✅   BY WATCHLIST join complete (0 unjoined)
- `22:57:10` ✅   491 lists
- `22:57:10` ✅   edge page markers 2/2
- `22:57:10` ✅ PASS_ALL — store 236, 0 agency slugs, union 10319 fully joined, statuses honest
