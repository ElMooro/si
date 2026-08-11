# ops 4614 — Real Economy institutional build

**Status:** failure  
**Duration:** 169.1s  
**Finished:** 2026-08-11T23:57:54+00:00  

## Error

```
SystemExit: 1
```

## Data

| collector | counts |
|---|---|
| {"ok": true, "counts": {"OK": 12, "FAILED": 3, "DEGRADED": 5}, "duration_s": 18.2} |  |
|  | {"OK": 12, "FAILED": 3, "DEGRADED": 5} |

## Log
## deploy-settle (both)

- `23:55:05` justhodl-real-economy-collector attempt 1: not created yet
- `23:55:36` justhodl-real-economy-collector carries justhodl-real-economy-collector v1.0.0
- `23:55:36` justhodl-physical-econ carries justhodl-physical-econ v2.0.0
- `23:55:36` ✅   [deploy-collector] collector v1.0.0
- `23:55:36` ✅   [deploy-signal] signal v2.0.0
## keys + config + schedule

- `23:55:36` ✅   [eia-secret] EIA_API_KEY in runner env (len=40)
- `23:55:43` ✅   [schedule] collector hourly schedule set
## collector run + per-leg truth table

- `23:56:02` aar_rail           tier3  DEGRADED  pattern not found
- `23:56:02` acc_cab            tier3  DEGRADED  HTTP Error 404: Not Found
- `23:56:02` aisi_steel         tier2  OK        measured weekly steel output
- `23:56:02` chokepoints        tier1  FAILED    arcgis Cannot perform query. Invalid query parameters.
- `23:56:02` copper             tier2  DEGRADED  parsed 0 rows
- `23:56:03` destatis_toll      tier3  DEGRADED  pattern not found
- `23:56:03` eia930_ercot       tier1  FAILED    0 daily rows
- `23:56:03` eia930_us48        tier1  FAILED    0 daily rows
- `23:56:03` eia_distillate     tier1  OK        distillate product supplied — diesel IS freight
- `23:56:03` eia_gasoline       tier1  OK        gasoline supplied — miles driven
- `23:56:03` eia_jet            tier1  OK        jet fuel supplied
- `23:56:03` eia_refinery       tier1  OK        refinery net crude inputs — industrial runs
- `23:56:03` fred_claims        tier1  OK        initial claims (inverted leg)
- `23:56:03` fred_hours         tier1  OK        aggregate weekly hours index — labor as quantity
- `23:56:03` fred_houst         tier1  OK        housing starts SAAR
- `23:56:04` fred_permit        tier1  OK        building permits SAAR
- `23:56:04` indeed_postings    tier1  OK        daily US job postings index, Feb-2020=100
- `23:56:04` noaa_degree_days   tier2  DEGRADED  {}
- `23:56:04` tsa                tier2  OK        daily travelers through US checkpoints
- `23:56:04` wti_term           tier1  OK        c1 minus c4, $/bbl; >0 = backwardation (the doctrine crisis precursor)
- `23:56:04` ✅   [tier1] tier-1 legs OK: 10/13 (hard SLO >=10)
## signal v2 + contracts

- `23:56:06` ✅   [signal-ok] signal ok:true ({"ok": true, "composite": 47.7, "label": "NEUTRAL", "n_live": 18, "confidence": "HIGH"})
- `23:56:06` ✅   [schema] schema 2.0
- `23:56:06` ✅   [sub-energy] energy scoring (55.1, 7/9 live)
- `23:56:06` ✅   [sub-trade_transport] trade_transport scoring (34.4, 4/5 live)
- `23:56:06` ✅   [sub-materials] materials scoring (63.4, 1/2 live)
- `23:56:06` ✅   [sub-labor] labor scoring (53.0, 3/3 live)
- `23:56:06` ✅   [sub-construction] construction scoring (36.5, 3/3 live)
- `23:56:06` ✅   [coverage] 18 live legs fleet-wide
- `23:56:06` ✅   [canary-oil_backwardation] oil_backwardation: {"state": "AMBER", "c1_minus_c4": 2.67, "doctrine": "extreme backwardation = physical scarcity, the crisis precursor"}
- `23:56:06` ✗   [canary-chokepoint_shock] CONTRACT MISS — chokepoint_shock: {}
- `23:56:06` ✅   [canary-claims_spike] claims_spike: {"state": "CALM", "wow_pct": 0.5, "latest": 199000}
## bug-check (institutional self-test)

- `23:56:06` ✅   [bug-weights] sub-pillar weights sum to 1.0
- `23:56:06` ✅   [bug-range] all scored legs in [0,100] (violations: none)
- `23:56:06` ✅   [bug-gate] no non-OK leg leaked into scoring (violations: none)
- `23:56:06` ✅   [bug-composite] composite 47.7 (NEUTRAL) confidence HIGH
## machine regress + purge + edge

- `23:56:12` ✅   [machine] machine P1 n=4 · composite 67.7
- `23:57:54` ✅   [edge-page] v2 page live
- `23:57:54` ✅   [edge-payload] v2 payload at the edge
## verdict

- `23:57:54` ✗ real economy build: 1 red (per-leg table above is the ground truth)
