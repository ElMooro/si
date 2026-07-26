# ops 3925 — v2.2 conversion pass + BDI/EUGDPYY path probe

**Status:** failure  
**Duration:** 126.2s  
**Finished:** 2026-07-26T21:34:21+00:00  

## Error

```
SystemExit: 1
```

## Data

| coverage_pct | n_live | statuses |
|---|---|---|
| 70.9 | 402 | {'META': 1, 'LIVE': 402, 'NO_FREE_SOURCE': 162, 'DISCONTINUED': 2} |

## Log
## probe: BDI + EUGDPYY paths for the next wire

- `21:32:15`   data/eurodollar-plumbing.json :: bdi: none
- `21:32:15`   data/eurodollar-plumbing.json :: baltic: none
- `21:32:15`   data/macro-nowcast.json :: eu: none
- `21:32:15`   data/macro-nowcast.json :: gdp: none
- `21:32:15` ✅   settled attempt 1
- `21:34:21`   DCPN3M: LIVE value=3.68 src=fred_2nd_chance
- `21:34:21`   BAMLC4A0C710YEY: LIVE value=5.55 src=fred_2nd_chance
- `21:34:21`   RIFSPPNA2P2D90NB: LIVE value=4.01 src=fred_2nd_chance
- `21:34:21`   UVXY: LIVE value=25.09 src=yahoo:UVXY
- `21:34:21`   ES10Y: None value=None src=None
- `21:34:21`   FR10Y: None value=None src=None
- `21:34:21` ✅   v2.2 settled
- `21:34:21` ✅   DCPN3M LIVE
- `21:34:21` ✅   BAMLC4A0C710YEY LIVE
- `21:34:21` ✅   RIFSPPNA2P2D90NB LIVE
- `21:34:21` ✅   UVXY LIVE
- `21:34:21` ✗   ES10Y LIVE
- `21:34:21` ✗   FR10Y LIVE
- `21:34:21` ✅   UNTAGGED = META
- `21:34:21` ✗   LIVE > 405
- `21:34:21` ✅   zero bare UNRESOLVED
- `21:34:21` ✗ FAILED: ['ES10Y LIVE', 'FR10Y LIVE', 'LIVE > 405']
