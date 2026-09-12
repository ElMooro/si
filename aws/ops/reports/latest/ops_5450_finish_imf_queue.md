- `15:30:05` before={"observed_at": "2026-09-12T15:30:05.882947+00:00", "last_modified": "2026-09-12T15:04:36+00:00", "state_age_h": 0.4249674852777778, "phase": "DRAIN", "as_of": "2026-09-02T02:43:15+00:00", "banked": 218, "queued": 2, "failure_count": 2, "lease_until": 1789226315.1350305, "lease_active_with_margin": false, "ids": [{"id": "PIP", "status": "THREE_ATTEMPT_QUARANTINE", "queued_attempts": null, "prior_5449_attempts": 3, "failure": null, "banked": null, "quarantine_basis": "existing importer skips queue attempts >=3; no source refusal inferred"}, {"id": "IMTS", "status": "PENDING_OR_IN_FLIGHT", "queued_attempts": 1, "prior_5449_attempts": 0, "failure": null, "banked": null, "quarantine_basis": null}, {"id": "IMTS_2026_MAY_VINTAGE", "status": "UNTRIED", "queued_attempts": 0, "prior_5449_attempts": 0, "failure": null, "banked": null, "quarantine_basis": null}]}
- `15:30:06` ✅ one Event invocation accepted; watching existing IMF state
**Status:** failure  
**Duration:** 2108.7s  
**Finished:** 2026-09-12T16:05:14+00:00  

## Error

```
SystemExit: 1
```

## Log
- `15:30:31` state write banked=218 queued=2 ids=[('PIP', 'PENDING_OR_IN_FLIGHT', None), ('IMTS', 'PENDING_OR_IN_FLIGHT', 2), ('IMTS_2026_MAY_VINTAGE', 'UNTRIED', 0)]
- `15:45:34` state write banked=218 queued=2 ids=[('PIP', 'PENDING_OR_IN_FLIGHT', None), ('IMTS', 'PENDING_OR_IN_FLIGHT', 3), ('IMTS_2026_MAY_VINTAGE', 'UNTRIED', 0)]
- `16:01:28` state write banked=218 queued=1 ids=[('PIP', 'PENDING_OR_IN_FLIGHT', None), ('IMTS', 'PENDING_OR_IN_FLIGHT', None), ('IMTS_2026_MAY_VINTAGE', 'PENDING_OR_IN_FLIGHT', 1)]
- `16:05:14` ✗ {"type": "RuntimeError", "message": "IMF IDs still unaccounted after bounded observation; no second kick"}
