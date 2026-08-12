# ops 4621 — Wave-4 regate

**Status:** success  
**Duration:** 37.8s  
**Finished:** 2026-08-12T01:25:39+00:00  

## Data

| canaries | composite | label | subs |
|---|---|---|---|
| {"oil_backwardation": "AMBER", "chokepoint_shock": "CALM", "cushing_squeeze": "AMBER", "withheld_stall": "CALM", "wei_divergence": "AMBER", "claims_spike": "CALM"} | 53.1 | NEUTRAL | {"energy": 56.1, "trade_transport": 33.3, "materials": 86.9, "labor": 55.0, "construction": 47.4} |

## Log
## deploy-settle

- `01:25:01` signal v2.1.1 live (attempt 1)
- `01:25:01` ✅   [deploy] signal v2.1.1
## collector + healed legs

- `01:25:32` noaa_degree_days OK        n=221  daily national CDD series (US line, header dates); HDD latest attached
- `01:25:32` ✅   [noaa] daily CDD series n=221
- `01:25:32` dts_withheld     OK        n=277  daily $M deposits — aggregate wages
- `01:25:32` ✅   [dts-depth] withheld series n=277 (YoY-able)
- `01:25:32` dts_customs      OK        n=277  daily $M — physical imports proxy
## signal + healed contracts

- `01:25:38` ✅   [ex-weather] ex-weather 32.6 · -2.18% (7d vs 21d residual) · beta 293.0 GWh/CDD over 59 aligned days
- `01:25:38` ✅   [withheld-leg] withheld leg 50.1 · +4.0% YoY (20d sums) · latest 25355 $M/d
- `01:25:38` ✅   [withheld-canary] withheld_stall: {"state": "CALM", "yoy_pct": 4.0, "doctrine": "nominal withheld-tax growth under 2% = aggregate labor income stalling"}
- `01:25:38` ✅   [coverage] 34 live legs
## edge

- `01:25:39` ✅   [edge] edge serves the ex-weather leg
## verdict

- `01:25:39` ✅ WAVE 4 COMPLETE — 34 legs, composite 53.1 (NEUTRAL); ex-weather live, wage canary armed
