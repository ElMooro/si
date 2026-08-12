# ops 4618 — copper k=1.2 + Destatis GENESIS

**Status:** success  
**Duration:** 57.6s  
**Finished:** 2026-08-12T00:37:40+00:00  

## Data

| composite | label | subs |
|---|---|---|
| 50.4 | NEUTRAL | {"energy": 57.9, "trade_transport": 37.6, "materials": 70.6, "labor": 53.0, "construction": 36.5} |

## Log
## deploy-settle

- `00:37:14` justhodl-real-economy-collector carries v1.2.1
- `00:37:15` justhodl-physical-econ carries v2.0.2
- `00:37:15` ✅   [deploy] collector v1.2.1 + signal v2.0.2
## invoke chain

- `00:37:34` destatis: DEGRADED via Destatis — truck-toll-mileage.html:no csv href | lkw-maut.html:no csv href
- `00:37:34` ⚠ destatis still degraded (guest quota or table shape) — honest, observed-only impact
- `00:37:40` ✅   [copper] copper 77.8 · +23.1% (3m vs prior 12m, IMF monthly, k=1.2) · $13552/tonne
- `00:37:40` ✅   [materials] materials 70.6
- `00:37:40` ✅   [coverage] 23 live legs
## edge

- `00:37:40` ✅   [edge] edge serves calibrated copper
## verdict

- `00:37:40` ✅ CALIBRATED — copper 77.8 (real +23%/3m move, sane scale), materials 70.6, composite 50.4 (NEUTRAL), 23 legs; destatis DEGRADED
