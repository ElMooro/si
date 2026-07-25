# ops 3894 — did any signal genuinely LEAD today's known price action

**Status:** success  
**Duration:** 3.5s  
**Finished:** 2026-07-25T23:40:18+00:00  

## Data

| dates | n_convergence_radar_archive_days | n_flagged_tickers_cross_checked_against_live_price | n_history_snapshots_found |
|---|---|---|---|
| ['2026-07-24', '2026-07-23', '2026-07-22', '2026-07-21', '2026-07-20', '2026-07-19', '2026-07-18', '2026-07-17', '2026-07-16', '2026-07-15', '2026-07-14', '2026-07-13', '2026-07-12', '2026-07-11', '2026-07-10', '2026-07-09', '2026-07-08', '2026-07-07', '2026-07-06', '2026-07-05', '2026-07-04'] |  |  | 21 |
|  | 0 |  |  |
|  |  | 0 |  |

## Log
## 1. locate real dated history snapshots for etf-flows, last ~21 calendar days

## 2. SMH/SOXX/XLK — flow/z/quadrant on the OLDEST available date vs TODAY

- `23:40:17`   SMH    [2026-07-04] z=1.28 quadrant=None persistence=1 ret21d_then=None%  ->  [2026-07-24] z=-0.46 quadrant=NEUTRAL ret21d_now=-6.61%
- `23:40:17`   SOXX   [2026-07-04] z=2.21 quadrant=None persistence=2 ret21d_then=None%  ->  [2026-07-24] z=-0.81 quadrant=NEUTRAL ret21d_now=-8.67%
- `23:40:17`   XLK    [2026-07-04] z=0.03 quadrant=None persistence=2 ret21d_then=None%  ->  [2026-07-24] z=0.06 quadrant=NEUTRAL ret21d_now=-3.12%
- `23:40:17`   XLE    [2026-07-04] z=-0.11 quadrant=None persistence=5 ret21d_then=None%  ->  [2026-07-24] z=-0.26 quadrant=NEUTRAL ret21d_now=9.12%
- `23:40:17`   XLV    [2026-07-04] z=1.26 quadrant=None persistence=4 ret21d_then=None%  ->  [2026-07-24] z=2.25 quadrant=TREND_CONFIRMED ret21d_now=6.13%
- `23:40:17`   XLF    [2026-07-04] z=0.19 quadrant=None persistence=1 ret21d_then=None%  ->  [2026-07-24] z=-0.44 quadrant=NEUTRAL ret21d_now=3.68%
## 3. convergence-radar — real dated archive, last 14 days, what got flagged

- `23:40:17` ✗   no convergence-radar archive days found in the last 14 days — either the engine isn't running, or the archive key pattern differs from what its own source declares
## 4. cross-check: are any convergence-radar-flagged tickers ALSO showing notable price moves in the LIVE constituent-pressure/daily data right now

- `23:40:18` ✅ PROBE COMPLETE — see logs above for concrete lead/lag evidence
