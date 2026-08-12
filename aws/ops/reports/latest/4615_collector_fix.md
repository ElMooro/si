# ops 4615 — collector v1.1.0 regate

**Status:** failure  
**Duration:** 195.6s  
**Finished:** 2026-08-12T00:04:52+00:00  

## Error

```
SystemExit: 1
```

## Data

| composite | counts | label | subs |
|---|---|---|---|
|  | {"OK": 14, "FAILED": 3, "DEGRADED": 3} |  |  |
| 50.4 |  | NEUTRAL | {"energy": 55.1, "trade_transport": 34.4, "materials": 81.7, "labor": 53.0, "construction": 36.5} |

## Log
## deploy-settle

- `00:01:37` v1.1.0 live (attempt 1)
- `00:01:37` ✅   [deploy] collector v1.1.0
## collector run + fixed-leg contracts

- `00:02:05` chokepoints      FAILED    no n_* transit fields; keys=['ISO3', 'LOCODE', 'ObjectId', 'continent', 'country', 'countrynoaccents', 'fullna
- `00:02:05` eia930_us48      FAILED    HTTP Error 400: Bad Request
- `00:02:05` eia930_ercot     FAILED    HTTP Error 400: Bad Request
- `00:02:05` copper           OK        stooq empty (parsed 0 rows) — IMF monthly copper via FRED
- `00:02:05` noaa_degree_days OK        weather context for the power-demand legs
- `00:02:05` ✗   [chokepoints] CONTRACT MISS — chokepoints OK
- `00:02:05` ✗   [eia930] CONTRACT MISS — US48 + ERCOT daily demand OK
- `00:02:05` ✗   [tier1] CONTRACT MISS — tier-1 OK: 10/13
## signal + canary + coverage

- `00:02:11` ✗   [chokepoint-canary] CONTRACT MISS — chokepoint_shock: {}
- `00:02:11` ✗   [coverage] CONTRACT MISS — 19 live legs
- `00:02:11` ✗   [energy-depth] CONTRACT MISS — energy sub-pillar 7/9 live
## edge

- `00:04:52` ✗   [edge] CONTRACT MISS — edge shows >=20 legs
## verdict

- `00:04:52` ✗ collector fix: 7 red
