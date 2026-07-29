# ops 4072 — v1.7.8 PRIORITY WALK (forecast-gated)

**Status:** failure  
**Duration:** 1.6s  
**Finished:** 2026-07-29T02:38:50+00:00  

## Error

```
SystemExit: 1
```

## Data

| agency_first2000_new | agency_first2000_old | agency_first500_new | agency_first500_old | edge_bytes | files_at_root | new_bytes | old_bytes | queue | tier1_total | version |
|---|---|---|---|---|---|---|---|---|---|---|
| 2000 | 659 | 500 | 136 |  |  |  |  | 10131 | 5081 |  |
|  |  |  |  |  | True |  | 18317 |  |  |  |
|  |  |  |  |  |  | 19286 |  |  |  | 1.7.8 |
|  |  |  |  | 13212 |  |  |  |  |  |  |

## Log
## A. FORECAST the new order on live data (pre-ship gate)

- `02:38:49`   first 500  : 136 agency  →  500 agency
- `02:38:49`   first 2000 : 659 agency  →  2000 agency
- `02:38:49`   tier1 total: 5081   tier2: 811   venue/other: 4239
- `02:38:49`   ✓ forecast passes — proceeding to ship
## What the first 300 walked will now be (top prefixes)

- `02:38:49`     122  FRED
- `02:38:49`      93  ECONOMICS
- `02:38:49`      24  TVC
- `02:38:49`      23  CBOE
- `02:38:49`      21  USI
- `02:38:49`      15  COT3
- `02:38:49`       2  COT
## B. rebuild + upload the extension zip

## C. edge download serves the new bytes

- `02:38:50`   edge zip version: 1.4.0
## VERDICT

- `02:38:50`   ✓ reorder is a pure permutation — nothing dropped
- `02:38:50`   ✓ first-500 agency yield strictly improves
- `02:38:50`   ✓ first 500 is now ~pure agency (>=450/500)
- `02:38:50`   ✓ payoff no longer buried past index 5000
- `02:38:50`   ✓ zip carries v1.7.8
- `02:38:50`   ✓ zip content.js carries the PRIORITY WALK
- `02:38:50`   ✓ zip carries the tier lists
- `02:38:50`   ✓ symsearch demoted to 1-in-200 canary
- `02:38:50`   ✓ step tightened to 240ms
- `02:38:50`   ✓ rate telemetry present
- `02:38:50`   ✓ scanner route still intact
- `02:38:50`   ✓ autonomy preserved (auto-start + auto-sync)
- `02:38:50`   ✗ edge serves v1.7.8
- `02:38:50` ✗ FAILED: ['edge serves v1.7.8']
