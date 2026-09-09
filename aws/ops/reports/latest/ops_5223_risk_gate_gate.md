# ops 5223 -- Release B2 gate: risk-gate v2.5 pure replay / disclosed overlays / native-month indicators / page contract

**Status:** failure  
**Duration:** 10.1s  
**Finished:** 2026-09-09T01:18:29+00:00  

## Error

```
SystemExit: 1
```

## Data

| composite | elapsed_s | overlays | posture | replay | sahm | step | truck | version |
|---|---|---|---|---|---|---|---|---|
| -0.125 | 87.6 | 0 | NEUTRAL | NEUTRAL | 0.0 | risk-gate | 0.0 | 1.0 |

## Log
- `01:18:20` justhodl-risk-gate LastModified 2026-09-09T00:26:45.000+0000 (push 2026-09-08T23:29:19+00:00)
- `01:18:29` invoke -> {"errorMessage": "name 'cur_v' is not defined", "errorType": "NameError", "requestId": "fcc0cb1a-b0f6-4507-a6f4-1ab3970124ff", "stackTrace": ["  File \"/var/task/lambda_function.py\", line 1088, in lambda_handler\n    \"indicators\": comput
- `01:18:29` ✗ invoke succeeded (FunctionError=Unhandled)
- `01:18:29` ✗ artifact version 2.5 (got 1.0)
- `01:18:29` ✗ replay_purity declared
- `01:18:29` ✗ 2 disclosed overlays with contribution/status/eligible: []
- `01:18:29` ✅ composite -0.125 == weighted legs -0.125 + overlays 0.000
- `01:18:29` ✗ composite_identity.check_ok
- `01:18:29` ✗ every leg publishes engine_score/fleet_fused_score/state/drivers
- `01:18:29` ✗ fleet_context.inputs dict with 0 entries
- `01:18:29` ✗ sahm_rule is native-month (basis+observation_date) or honestly pending: {'value': 0.0, 'signal': 'CLEAR', 'basis': None, 'observation_date': None, 'pending_source': None}
- `01:18:29` ✗ truck_transport is native-month (basis+observation_date) or honestly pending: {'value': 0.0, 'signal': 'EXPANDING', 'basis': None, 'observation_date': None, 'pending_source': None}
- `01:18:29` posture=NEUTRAL composite=-0.125 (replay FRED-only NEUTRAL/-0.1) sizing x0.75 | sahm 0.0 CLEAR | truck 0.0 EXPANDING | elapsed 87.6s
- `01:18:29` ✅ risk-gate.html carries the indicators/overlays panels at the edge
## verdict

- `01:18:29` ✗ invoke succeeded (FunctionError=Unhandled)
- `01:18:29` ✗ artifact version 2.5 (got 1.0)
- `01:18:29` ✗ replay_purity declared
- `01:18:29` ✗ 2 disclosed overlays with contribution/status/eligible: []
- `01:18:29` ✗ composite_identity.check_ok
- `01:18:29` ✗ every leg publishes engine_score/fleet_fused_score/state/drivers
- `01:18:29` ✗ fleet_context.inputs dict with 0 entries
- `01:18:29` ✗ sahm_rule is native-month (basis+observation_date) or honestly pending: {'value': 0.0, 'signal': 'CLEAR', 'basis': None, 'observation_date': None, 'pending_source': None}
- `01:18:29` ✗ truck_transport is native-month (basis+observation_date) or honestly pending: {'value': 0.0, 'signal': 'EXPANDING', 'basis': None, 'observation_date': None, 'pending_source': None}
