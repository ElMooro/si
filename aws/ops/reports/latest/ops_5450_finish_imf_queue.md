- `16:10:33` before={"observed_at": "2026-09-12T16:10:33.568374+00:00", "last_modified": "2026-09-12T16:01:22+00:00", "state_age_h": 0.15321343722222222, "phase": "DRAIN", "as_of": "2026-09-02T02:43:15+00:00", "banked": 218, "queued": 1, "failure_count": 2, "lease_until": 1789229721.5406823, "lease_active_with_margin": true, "ids": [{"id": "PIP", "status": "THREE_ATTEMPT_QUARANTINE", "queued_attempts": null, "max_observed_attempts": 3, "failure": null, "execution_failure": null, "banked": null, "quarantine_basis": "existing importer skips queue attempts >=3; no source refusal inferred"}, {"id": "IMTS", "status": "THREE_ATTEMPT_QUARANTINE", "queued_attempts": null, "max_observed_attempts": 3, "failure": null, "execution_failure": null, "banked": null, "quarantine_basis": "existing importer skips queue attempts >=3; no source refusal inferred"}, {"id": "IMTS_2026_MAY_VINTAGE", "status": "PENDING_OR_IN_FLIGHT", "queued_attempts": 1, "max_observed_attempts": 1, "failure": null, "execution_failure": null, "banked": null, "quarantine_basis": null}]}
- `16:16:30` ✅ IMF accounted: banked=218 queued=1 lm=2026-09-12T16:01:22+00:00
**Status:** success  
**Duration:** 357.5s  
**Finished:** 2026-09-12T16:16:30+00:00  

## Log
- `16:16:30` PIP: THREE_ATTEMPT_QUARANTINE failure=None
- `16:16:30` IMTS: THREE_ATTEMPT_QUARANTINE failure=None
- `16:16:30` IMTS_2026_MAY_VINTAGE: NAMED_EXECUTION_TIMEOUT failure=None
