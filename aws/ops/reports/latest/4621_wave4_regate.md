# ops 4621 — Wave-4 regate

**Status:** failure  
**Duration:** 40.0s  
**Finished:** 2026-08-12T01:22:43+00:00  

## Error

```
SystemExit: 1
```

## Data

| canaries | composite | label | subs |
|---|---|---|---|
| {"oil_backwardation": "AMBER", "chokepoint_shock": "CALM", "cushing_squeeze": "AMBER", "wei_divergence": "AMBER", "claims_spike": "CALM"} | 53.4 | NEUTRAL | {"energy": 56.9, "trade_transport": 33.0, "materials": 86.9, "labor": 56.3, "construction": 47.4} |

## Log
## deploy-settle

- `01:22:04` v1.3.1 live (attempt 1)
- `01:22:04` ✅   [deploy] collector v1.3.1
## collector + healed legs

- `01:22:37` noaa_degree_days OK        n=221  daily national CDD series (US line, header dates); HDD latest attached
- `01:22:37` ✅   [noaa] daily CDD series n=221
- `01:22:37` dts_withheld     OK        n=277  daily $M deposits — aggregate wages
- `01:22:37` ✅   [dts-depth] withheld series n=277 (YoY-able)
- `01:22:37` dts_customs      OK        n=277  daily $M — physical imports proxy
## signal + healed contracts

- `01:22:43` ✅   [ex-weather] ex-weather 32.6 · -2.18% (7d vs 21d residual) · beta 293.0 GWh/CDD over 59 aligned days
- `01:22:43` ✗   [withheld-leg] CONTRACT MISS — withheld leg None · transform: name 'timedelta' is not defined
- `01:22:43` ✗   [withheld-canary] CONTRACT MISS — withheld_stall: {}
- `01:22:43` ✗   [coverage] CONTRACT MISS — 31 live legs
## edge

- `01:22:43` ✅   [edge] edge serves the ex-weather leg
## verdict

- `01:22:43` ✗ regate: 3 red
