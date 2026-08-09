# ops 4570 — import checkpoint vs 22:48 snapshot

**Status:** success  
**Duration:** 1.6s  
**Finished:** 2026-08-09T23:52:15+00:00  

## Data

| as_of | banked_now | blocked_at | cats_done | delta | fred_keys | fred_series | hours | hub_age_min | hub_as_of | inv_2h | of | page_snapshot | per_hr | queued | rule | skipped_already | state_age_min | status | totals |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  | 5967 |  |  | 938 |  |  | 1.07 |  |  |  |  | 5029 | 876.1 |  |  |  |  |  |  |
|  |  | None | 81 |  |  |  |  |  |  |  | 179 |  |  | 375782 |  | 125814 | 3.5 | walking |  |
|  |  |  |  |  |  |  |  |  |  | 71 |  |  |  |  | ENABLED |  |  |  |  |
|  |  |  |  |  | 8442 | 5892 |  | 3.4 | 2026-08-09T23:48:53+00:00 |  |  |  |  |  |  |  |  |  | {"providers": 42, "datasets": 51792, "keys": 28405, "gb": 44.13} |
| 2026-08-09T23:48:53+00:00 |  |  |  |  |  | 5892 |  |  |  |  |  |  |  |  |  |  |  |  |  |

## Log
## 1. disk truth

## 2. importer state + schedule + fires

## 3. hub freshness + page parity + regressions

## 4. served hub (what the page fetches)

- `23:52:15` ✅ IMPORTING_AND_PAGE_TRUE banked=5967 (+938 in 1.1h, 876.1/hr) cats=81/179 rule=ENABLED inv2h=71
