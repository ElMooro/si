# ops 5223 -- Release B2 gate: risk-gate v2.5 pure replay / disclosed overlays / native-month indicators / page contract

**Status:** success  
**Duration:** 12.4s  
**Finished:** 2026-09-09T01:57:07+00:00  

## Data

| composite | elapsed_s | overlays | posture | replay | sahm | step | truck | version |
|---|---|---|---|---|---|---|---|---|
| -0.125 | 10.7 | 0.0 | NEUTRAL | NEUTRAL | -0.07 | risk-gate | -0.3 | 2.5 |

## Log
- `01:56:55` justhodl-risk-gate LastModified 2026-09-09T01:54:41.000+0000 (push 2026-09-09T01:53:38+00:00)
- `01:57:06` invoke -> {"ok": true, "posture": "NEUTRAL", "composite": -0.125, "sizing_multiplier": 0.75, "n_flips": 35}
- `01:57:06` ✅ invoke succeeded (FunctionError=None)
- `01:57:07` ✅ artifact version 2.5 (got 2.5)
- `01:57:07` ✅ replay_purity declared
- `01:57:07` ✅ 2 disclosed overlays with contribution/status/eligible: [('collateral', 'OK', 0.0), ('foreign_official', 'STALE', 0.0)]
- `01:57:07` ✅ composite -0.125 == weighted legs -0.125 + overlays 0.000
- `01:57:07` ✅ composite_identity.check_ok
- `01:57:07` ✅ every leg publishes engine_score/fleet_fused_score/state/drivers
- `01:57:07` ✅ fleet_context.inputs dict with 30 entries
- `01:57:07` ✅ sahm_rule is native-month (basis+observation_date) or honestly pending: {'value': -0.07, 'signal': 'CLEAR', 'basis': 'native monthly UNRATE; 3-mo avg minus min of prior 12 3-mo avgs (official formula)', 'observation_date': '2026-08-01', 'pending_source': None}
- `01:57:07` ✅ truck_transport is native-month (basis+observation_date) or honestly pending: {'value': -0.3, 'signal': 'SOFT', 'basis': 'native monthly TRUCKD11 vs the observation 12 calendar months earlier', 'observation_date': '2026-06-01', 'pending_source': None}
- `01:57:07` posture=NEUTRAL composite=-0.125 (replay FRED-only NEUTRAL/-0.1) sizing x0.75 | sahm -0.07 CLEAR | truck -0.3 SOFT | elapsed 10.7s
- `01:57:07` ✅ risk-gate.html carries the indicators/overlays panels at the edge
## verdict

- `01:57:07` ✅ GREEN -- risk-gate replay is FRED-pure, overlays disclosed with identity, monthly indicators native
