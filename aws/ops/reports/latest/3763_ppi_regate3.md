# ops 3763 — canary #13: narrow-line PPI acceleration

**Status:** failure  
**Duration:** 47.7s  
**Finished:** 2026-07-23T02:16:11+00:00  

## Error

```
SystemExit: 1
```

## Data

| accelerating | decelerating | failed | lines | verdict |
|---|---|---|---|---|
| 50 | 7 | G4_breadth | 178 | FAIL |

## Log
## G0 — key contract (producer + page)

- `02:15:24` PASS G0_key_contract — producer_missing=[] page_missing=[]
## G1 — settle v1.2.0

- `02:15:25` PASS G1_settle — deployed
## G2 — async invoke + freshness

- `02:16:11` PASS G2_artifact — lines=178 accel=50 decel=7
## G3 — data truth (accel IS the 2nd derivative)

- `02:16:11`   PCU42993042993022      accel=+56.30pp yoy=+145.04% prior= +88.74% m3ann=246.64 z=None ACCELERATING_CONFIRMED
- `02:16:11`   PCU3313143313142       accel=+34.70pp yoy= +58.69% prior= +23.99% m3ann=103.72 z=1.5 ACCELERATING_CONFIRMED
- `02:16:11`   PCU3251993251991       accel=+32.79pp yoy= +59.53% prior= +26.74% m3ann=118.64 z=1.06 ACCELERATING_CONFIRMED
- `02:16:11`   PCU3251803251806       accel=+25.35pp yoy= +17.53% prior=  -7.82% m3ann=96.28 z=0.3 ACCELERATING_CONFIRMED
- `02:16:11`   PCU3252113252111       accel=+22.34pp yoy= +18.35% prior=  -3.99% m3ann=75.76 z=0.58 ACCELERATING_CONFIRMED
- `02:16:11`   PCU325211325211P       accel=+20.62pp yoy= +16.57% prior=  -4.05% m3ann=72.6 z=0.57 ACCELERATING_CONFIRMED
- `02:16:11`   PCU32551032551071      accel=+14.91pp yoy= +16.40% prior=  +1.49% m3ann=41.16 z=0.74 ACCELERATING_CONFIRMED
- `02:16:11`   PCU3211133211133       accel=+14.68pp yoy=  +6.99% prior=  -7.69% m3ann=24.64 z=0.03 ACCELERATING_CONFIRMED
- `02:16:11` PASS G3_data_truth — rows=178 math_mismatch=[] bad_confirmed=[] z_leak=[]
## G4 — sweep covers the discovered universe

- `02:16:11`   universe=198 swept=178 coverage=89.9%
- `02:16:11`   drop reasons: {'fetch_error': 20, 'short_history': 0, 'exception': 0}
- `02:16:11`   dropped ids (sample): ['PCU33333333', 'PCU3335133351', 'PCU333318333318', 'PCU3333133331', 'PCU3339123339121', 'PCU33353335', 'PCU333618333618F', 'PCU33361133361105']
- `02:16:11` FAIL G4_breadth — coverage 89.9% (178 of 198) · accounted=198/198 · reasons={'fetch_error': 20, 'short_history': 0, 'exception': 0}
## G5 — page served + field coverage + nav

- `02:16:11`   served page CURRENT len=10871 after 0s
- `02:16:11` PASS G5_page_live — len=10871
- `02:16:11` PASS G5_field_coverage — every published key has a render path
- `02:16:11` PASS G5_nav — listed under ('Macro & Liquidity', 'PPI Acceleration')
## VERDICT

- `02:16:11` ✗ gates failed: ['G4_breadth']
