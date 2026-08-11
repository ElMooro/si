# ops 4610 — Physical Economy trade signal

**Status:** failure  
**Duration:** 163.7s  
**Finished:** 2026-08-11T22:18:10+00:00  

## Error

```
SystemExit: 1
```

## Data

| machine_composite | machine_verdict | physical_invoke |
|---|---|---|
|  |  | {"ok": true, "composite": 52.8, "label": "NEUTRAL", "n_components": 2, "signal": "NEUTRAL", "confidence": "LOW"} |
| 67.3 | profit expectations rising · rates squeezing valuations · money flowing IN · no one is forced to sell |  |

## Log
## shape-dump: the three legacy physical artifacts

- `22:15:26` data/port-cargo.json → {"engine": "'port-cargo'", "version": "'1.1.0'", "date_field_type": "NoneType", "engine_class": "'physical_trade_fast_layer", "evidence_tier": "'tier_1_measured_physical'", "lag_months": "int", "generated_at": "'2026-08-11T12:40:38.16005", "duration_s": "float", "fetch_status": "'FAILED'", "latest_data_date": "NoneType", "data_age_days": "NoneType", "expected_lag_days": "int", "stale": "bool", "method": "'IMF PortWat
- `22:15:26` data/grid-queue.json → {"version": "'2.1.0'", "generated_at": "'2026-08-11T12:50:16.81702", "national": {"primary_metric": "str", "mw_with_executed_ia": "float", "ia_measured_isos": "list", "headline_queue_mw": "float", "headline_risk_adjusted_mw": "float", "isos_live": "list", "isos_missing": "list", "n_isos_live": "int", "assumption": "str", "blind_spot": "str"}, "large_load_queue": {"status": "str"}, "queue_velocity": {"status": "str", 
- `22:15:26` data/freight-pulse.json → {"ok": "bool", "version": "'2.0.0'", "generated_at": "'2026-08-11T11:50:17.83271", "engine_class": "'physical_trade_slow_confi", "composite_role": "'slow_confirmation_leg'", "lag_months": "int", "role_note": "'ops-4559 BUG-13: six US m", "series": {"tsi_freight": "dict", "cass_shipments": "dict", "cass_expend": "dict", "truck_tonnage": "dict", "rail_carloads": "dict", "rail_intermodal": "dict"}, "errors": ["list[0]",
## deploy-settle (both functions)

- `22:15:26` justhodl-physical-econ attempt 1: not created yet
- `22:15:57` justhodl-physical-econ carries justhodl-physical-econ v1.0.0 (attempt 2)
- `22:15:57` justhodl-market-machine carries v1.2.0 (attempt 1)
- `22:15:57` ✅   [deploy-physical] physical-econ live v1.0.0
- `22:15:57` ✅   [deploy-machine] market-machine live v1.2.0
## physical-econ config + schedule

- `22:15:57` ✅   [schedule] hourly schedule set for physical-econ
## invoke chain: physical -> machine

- `22:15:58` ✅   [invoke-physical] physical-econ ok:true
- `22:15:58` ✗   [components] CONTRACT MISS — 2 of 5 components found: ["PJM demand momentum (8d)", "Freight pulse"]
- `22:15:58` ✗   [pjm-legs] CONTRACT MISS — both PJM legs joined (momentum + LMP shock canary), found 1
- `22:15:58` ✅   [signal] trade signal NEUTRAL (LOW confidence) · composite 52.8
- `22:16:04` ✅   [invoke-machine] market-machine ok:true
- `22:16:04` ✅   [machine-p1] profits pillar carries the physical pulse (n=4, score=70.2)
## purge + edge (both payloads)

- `22:16:04` edge 1: HTTP Error 404: Not Found
- `22:16:29` edge 2: HTTP Error 404: Not Found
- `22:16:54` edge 3: HTTP Error 404: Not Found
- `22:17:19` edge 4: HTTP Error 404: Not Found
- `22:17:44` edge 5: HTTP Error 404: Not Found
- `22:18:10` ✅   [edge-page] physical-economy.html live
- `22:18:10` ✅   [edge-payload] physical-economy.json serving 1.0
## verdict

- `22:18:10` ✗ physical wiring: 2 red
