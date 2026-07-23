# ops 3760 — canary #13: narrow-line PPI acceleration

**Status:** failure  
**Duration:** 151.3s  
**Finished:** 2026-07-23T01:57:53+00:00  

## Error

```
SystemExit: 1
```

## Data

| accelerating | decelerating | failed | lines | verdict |
|---|---|---|---|---|
| 41 | 3 | G4_breadth | 117 | FAIL |

## Log
## G0 — key contract (producer + page)

- `01:55:22` PASS G0_key_contract — producer_missing=[] page_missing=[]
## G1 — settle v1.0.0

- `01:55:38` PASS G1_settle — deployed
## G2 — async invoke + freshness

- `01:55:53` PASS G2_artifact — lines=117 accel=41 decel=3
## G3 — data truth (accel IS the 2nd derivative)

- `01:55:53`   PCU3313143313142       accel=+34.70pp yoy= +58.69% prior= +23.99% m3ann=103.72 z=1.5 ACCELERATING_CONFIRMED
- `01:55:53`   PCU3251993251991       accel=+32.79pp yoy= +59.53% prior= +26.74% m3ann=118.64 z=1.06 ACCELERATING_CONFIRMED
- `01:55:53`   PCU3251803251806       accel=+25.35pp yoy= +17.53% prior=  -7.82% m3ann=96.28 z=0.3 ACCELERATING_CONFIRMED
- `01:55:53`   PCU3252113252111       accel=+22.34pp yoy= +18.35% prior=  -3.99% m3ann=75.76 z=0.58 ACCELERATING_CONFIRMED
- `01:55:53`   PCU325211325211P       accel=+20.62pp yoy= +16.57% prior=  -4.05% m3ann=72.6 z=0.57 ACCELERATING_CONFIRMED
- `01:55:53`   PCU32551032551071      accel=+14.91pp yoy= +16.40% prior=  +1.49% m3ann=41.16 z=0.74 ACCELERATING_CONFIRMED
- `01:55:53`   PCU3211133211133       accel=+14.68pp yoy=  +6.99% prior=  -7.69% m3ann=24.64 z=0.03 ACCELERATING_CONFIRMED
- `01:55:53`   PCU32513251            accel=+13.48pp yoy= +15.01% prior=  +1.53% m3ann=48.4 z=0.59 ACCELERATING_CONFIRMED
- `01:55:53` PASS G3_data_truth — rows=117 math_mismatch=[] bad_confirmed=[] z_leak=[]
## G4 — sweep covers the discovered universe

- `01:55:53`   universe=198 swept=117 coverage=59.1%
- `01:55:53` FAIL G4_breadth — coverage 59.1% (117 of 198) — a sweep that collapses to a handful defeats the canary
## G5 — page served + field coverage + nav

- `01:55:53`   attempt 0: HTTP Error 404: Not Found
- `01:56:13`   attempt 1: HTTP Error 404: Not Found
- `01:56:33`   attempt 2: HTTP Error 404: Not Found
- `01:56:53`   attempt 3: HTTP Error 404: Not Found
- `01:57:13`   attempt 4: HTTP Error 404: Not Found
- `01:57:33`   attempt 5: HTTP Error 404: Not Found
- `01:57:53`   served page CURRENT len=10339 after 120s
- `01:57:53` PASS G5_page_live — len=10339
- `01:57:53` PASS G5_field_coverage — every published key has a render path
- `01:57:53` PASS G5_nav — listed under ('Macro & Liquidity', 'PPI Acceleration')
## VERDICT

- `01:57:53` ✗ gates failed: ['G4_breadth']
