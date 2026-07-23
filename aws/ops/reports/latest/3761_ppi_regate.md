# ops 3761 — canary #13: narrow-line PPI acceleration

**Status:** failure  
**Duration:** 151.9s  
**Finished:** 2026-07-23T02:03:42+00:00  

## Error

```
SystemExit: 1
```

## Data

| accelerating | decelerating | failed | lines | verdict |
|---|---|---|---|---|
| 41 | 3 | G4_breadth | 120 | FAIL |

## Log
## G0 — key contract (producer + page)

- `02:01:10` PASS G0_key_contract — producer_missing=[] page_missing=[]
## G1 — settle v1.1.0

- `02:01:11` PASS G1_settle — deployed
## G2 — async invoke + freshness

- `02:01:42` PASS G2_artifact — lines=120 accel=41 decel=3
## G3 — data truth (accel IS the 2nd derivative)

- `02:01:42`   PCU3313143313142       accel=+34.70pp yoy= +58.69% prior= +23.99% m3ann=103.72 z=1.5 ACCELERATING_CONFIRMED
- `02:01:42`   PCU3251993251991       accel=+32.79pp yoy= +59.53% prior= +26.74% m3ann=118.64 z=1.06 ACCELERATING_CONFIRMED
- `02:01:42`   PCU3251803251806       accel=+25.35pp yoy= +17.53% prior=  -7.82% m3ann=96.28 z=0.3 ACCELERATING_CONFIRMED
- `02:01:42`   PCU3252113252111       accel=+22.34pp yoy= +18.35% prior=  -3.99% m3ann=75.76 z=0.58 ACCELERATING_CONFIRMED
- `02:01:42`   PCU325211325211P       accel=+20.62pp yoy= +16.57% prior=  -4.05% m3ann=72.6 z=0.57 ACCELERATING_CONFIRMED
- `02:01:42`   PCU32551032551071      accel=+14.91pp yoy= +16.40% prior=  +1.49% m3ann=41.16 z=0.74 ACCELERATING_CONFIRMED
- `02:01:42`   PCU3211133211133       accel=+14.68pp yoy=  +6.99% prior=  -7.69% m3ann=24.64 z=0.03 ACCELERATING_CONFIRMED
- `02:01:42`   PCU32513251            accel=+13.48pp yoy= +15.01% prior=  +1.53% m3ann=48.4 z=0.59 ACCELERATING_CONFIRMED
- `02:01:42` PASS G3_data_truth — rows=120 math_mismatch=[] bad_confirmed=[] z_leak=[]
## G4 — sweep covers the discovered universe

- `02:01:42`   universe=198 swept=120 coverage=60.6%
- `02:01:42`   drop reasons: {'fetch_error': 78, 'short_history': 0, 'exception': 0}
- `02:01:42`   dropped ids (sample): ['PCU3315243315240', 'PCU3339123339121', 'PCU33333333', 'PCU333992333992A', 'PCU333318333318', 'PCU3339943339940', 'PCU3339933339931', 'PCU33411133411172']
- `02:01:42` FAIL G4_breadth — coverage 60.6% (120 of 198) · accounted=198/198 · reasons={'fetch_error': 78, 'short_history': 0, 'exception': 0}
## G5 — page served + field coverage + nav

- `02:03:42`   served page CURRENT len=10687 after 120s
- `02:03:42` PASS G5_page_live — len=10687
- `02:03:42` PASS G5_field_coverage — every published key has a render path
- `02:03:42` PASS G5_nav — listed under ('Macro & Liquidity', 'PPI Acceleration')
## VERDICT

- `02:03:42` ✗ gates failed: ['G4_breadth']
